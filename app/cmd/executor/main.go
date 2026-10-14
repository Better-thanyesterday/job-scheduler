package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/executor/executor"
	"job-scheduler/app/internal/executor/handler"
	"job-scheduler/app/internal/executor/resultstore"
	"job-scheduler/app/internal/executor/rpc"
	infraetcd "job-scheduler/app/internal/infra/etcd"
	"job-scheduler/app/internal/infra/mq"
	"job-scheduler/app/internal/infra/mysql"
	repo "job-scheduler/app/internal/infra/mysql/sqlx/repo"
	"job-scheduler/app/pkg/logger"
	"job-scheduler/app/pkg/metrics"
	"job-scheduler/app/settings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// rawPublisher 把 mq.Producer 适配成 executor.MessagePublisher：
// 消息任务的 topic 即 routing key，走 direct 交换机。
// ★ 注意分工：业务载荷（消息任务的 payload）仍走 MQ；执行消息本身已改走 gRPC 推送
type rawPublisher struct {
	producer *mq.Producer
}

func (p *rawPublisher) Publish(ctx context.Context, topic string, body []byte) error {
	return p.producer.PublishRaw(ctx, topic, body)
}

// resultNotifier 把 mq.Producer 适配成 handler.ResultNotifier：
// 执行结果回执按"来源(dag/cron) × 成败"选结果队列投递。
// 独立短超时 + 吞掉错误：结果通知是 best-effort，MQ 卡死不能拖住执行 goroutine
type resultNotifier struct {
	producer *mq.Producer
	timeout  time.Duration
}

func (n *resultNotifier) NotifyResult(ctx context.Context, r *coordination.ExecutionResult) {
	body, err := r.Body()
	if err != nil {
		zap.L().Error("序列化执行结果失败", zap.Int64("execution_id", r.ExecID), zap.Error(err))
		return
	}
	nctx, cancel := context.WithTimeout(context.Background(), n.timeout)
	defer cancel()
	key := mq.RoutingKeyForResult(r.FromDag(), r.Success, r.FailureClass)
	if key == "" {
		// cron 来源无 MQ 回执：结果已落库，planner/reaper 轮询感知
		return
	}
	if err := n.producer.PublishRaw(nctx, key, body); err != nil {
		zap.L().Error("推送执行结果到 MQ 失败（不影响执行本身）",
			zap.Int64("execution_id", r.ExecID),
			zap.String("routing_key", key), zap.Error(err))
	}
}

// referencedResult GC 的引用检查：executions.result 列原文包含对象 key 即视为在用
func referencedResult(executionDB mysql.ExecutionDB) resultstore.ReferencedFunc {
	return func(ctx context.Context, execID int64, key string) (bool, error) {
		var result sql.NullString
		err := executionDB.QueryRowContext(ctx,
			`SELECT result FROM executions WHERE id = ?`, execID).Scan(&result)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// 行已物理删除 = 引用必然不存在：GC 的兜底目标场景，
			// 当查询失败跳过会让孤儿对象永远回收不掉
			return false, nil
		case err != nil:
			return false, err
		}
		return resultstore.ResultReferences(result.String, key), nil
	}
}

func main() {
	// 1. 配置 + 日志
	if err := settings.Init(); err != nil {
		fmt.Printf("init settings failed: %v\n", err)
		return
	}
	if err := logger.Init(settings.Conf.Load().LogConfig, settings.Conf.Load().Mode, "executor"); err != nil {
		fmt.Printf("init logger failed: %v\n", err)
		return
	}
	defer zap.L().Sync()

	exeConf := settings.Conf.Load().ExecutorConfig
	if exeConf == nil || exeConf.Listen == "" {
		fmt.Println("executor config missing (executor.listen)")
		return
	}

	// 2. MySQL（执行域）
	_, _, _, executionDB, _ := mysql.InitAll(settings.Conf.Load().MySqls)
	defer mysql.CloseAll()

	// 3. RabbitMQ：消息任务的业务载荷（MessageExecutor）+ best-effort 结果回执。
	// 执行链路（gRPC 推送）不依赖 MQ：未配置/连不上只降级这两项能力，服务照常起——
	// 回执丢了不影响正确性（DB 终态是事实源），消息任务不可用有 metrics 可见
	var messagePublisher executor.MessagePublisher
	var resultNotif handler.ResultNotifier
	if mqCfg := settings.Conf.Load().MQConfig; mqCfg != nil {
		client, err := mq.NewClient(&mq.Config{
			Host: mqCfg.Host, Port: mqCfg.Port,
			User: mqCfg.User, Password: mqCfg.Password, VHost: mqCfg.VHost,
			ConfirmMode: mqCfg.ConfirmMode, PrefetchCount: mqCfg.PrefetchCount,
		})
		if err != nil {
			zap.L().Warn("MQ 初始化失败：消息任务与结果回执降级为不可用（执行不受影响）", zap.Error(err))
		} else {
			defer client.Close()
			if err := client.DeclareAll(); err != nil {
				zap.L().Warn("MQ 拓扑声明失败：消息任务与结果回执降级为不可用", zap.Error(err))
			} else {
				messagePublisher = &rawPublisher{producer: client.NewProducer()}
				resultNotif = &resultNotifier{producer: client.NewProducer(), timeout: 5 * time.Second}
			}
		}
	}

	// 4. etcd：执行器注册上报（崩溃自动退表，优雅退出主动注销）
	if settings.Conf.Load().EtcdConfig == nil || len(settings.Conf.Load().EtcdConfig.Endpoints) == 0 {
		fmt.Println("etcd config missing (etcd.endpoints)")
		return
	}
	etcdCli, err := infraetcd.NewEtcdClient(settings.Conf.Load().EtcdConfig.Endpoints)
	if err != nil {
		fmt.Printf("init etcd failed: %v\n", err)
		return
	}
	defer etcdCli.Close()

	// 5. 执行器注册表 + 推送处理器（ClaimPending 幂等语义与 MQ 时期一致）
	registry := executor.NewRegistry(messagePublisher)
	workerID := settings.Conf.Load().WorkerID
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", os.Getpid())
	}
	h := handler.NewHandler(repo.NewExecutionRepo(executionDB), registry, workerID, resultNotif)

	// 服务生命周期 ctx：信号触发 cancel，溢出存储的 GC 等后台 goroutine 共用
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 5.1 结果溢出存储（可选）：配置了 executor.minio 才启用三段式溢出。
	// 启动失败不阻断服务——降级为内联截断，只打 warn
	if exeConf.Minio != nil {
		store, err := resultstore.New(resultstore.Config{
			Endpoint:  exeConf.Minio.Endpoint,
			AccessKey: exeConf.Minio.AccessKey,
			SecretKey: exeConf.Minio.SecretKey,
			Bucket:    exeConf.Minio.Bucket,
			UseSSL:    exeConf.Minio.UseSSL,
		})
		if err != nil {
			zap.L().Warn("结果溢出存储初始化失败，保持内联截断", zap.Error(err))
		} else {
			if err := store.EnsureSetup(ctx); err != nil {
				zap.L().Warn("结果溢出存储就绪检查失败（建桶/生命周期），保持内联截断", zap.Error(err))
			} else {
				// 放开脚本输出上限：全量交给 handler 溢出决策（不放开则执行器内
				// 截成 4KB，溢出永远不触发）
				registry = registry.WithScriptOutputCap(0)
				h.EnableSpill(store, exeConf.ResultInlineMaxBytes)
				zap.L().Info("结果溢出存储已启用",
					zap.String("bucket", store.BucketName()),
					zap.Int("inline_max_bytes", exeConf.ResultInlineMaxBytes))
				// 孤儿对象回收：宽限 24h（覆盖"先上传后写库"窗口），每 6h 扫一轮
				go store.RunGC(ctx, referencedResult(executionDB), 6*time.Hour, 24*time.Hour)
			}
		}
	}

	// 6. gRPC server：ExecutorService + 健康检查
	lis, err := net.Listen("tcp", exeConf.Listen)
	if err != nil {
		fmt.Printf("listen %s failed: %v\n", exeConf.Listen, err)
		return
	}
	grpcSrv := grpc.NewServer()
	rpc.RegisterExecutorServiceServer(grpcSrv, rpc.NewServer(h))
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcSrv, hs)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			zap.L().Error("executor grpc server failed", zap.Error(err))
		}
	}()

	// 7. etcd 状态上报：Running 等动态值从 handler 实时取
	maxConcurre := exeConf.MaxConcurre
	if maxConcurre == 0 {
		maxConcurre = 10
	}
	startedAt := time.Now()
	baseInfo := func() infraetcd.ExecutorInfo {
		return infraetcd.ExecutorInfo{
			WorkerID:   workerID,
			Addr:       exeConf.AdvertiseAddr(),
			MaxConcurr: maxConcurre,
			Running:    h.Running(),
			Version:    exeConf.Version,
			StartedAt:  startedAt,
		}
	}
	reporter := infraetcd.NewReporter(etcdCli, baseInfo(), exeConf.LeaseTTL, 0, baseInfo)
	go reporter.Run(ctx)

	// 8. /metrics（executor 专属端口，与 scheduler 的 9100 区分）
	metricsPort := exeConf.MetricsPort
	if metricsPort == 0 {
		metricsPort = 9902
	}
	metricsSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", metricsPort),
		Handler: promhttp.Handler(),
	}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			zap.L().Error("executor metrics server failed", zap.Error(err))
		}
	}()
	metrics.SetExecutorUp(true)
	zap.L().Info("executor started",
		zap.String("worker_id", workerID),
		zap.String("grpc_listen", exeConf.Listen),
		zap.String("advertise", exeConf.AdvertiseAddr()),
		zap.Int("metrics_port", metricsPort))

	// 9. 等待退出信号：先从 etcd 注销（scheduler 即刻停推），再排空在途执行
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	metrics.SetExecutorUp(false)
	cancel()
	_ = reporter.Close(context.Background())

	stopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop() // 等在途 RPC 返回（执行中任务跑完写终态）
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		zap.L().Warn("executor: 优雅停止超时，强制关闭（在途任务交由 reaper 收割）")
		grpcSrv.Stop()
	}

	mctx, mcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer mcancel()
	_ = metricsSrv.Shutdown(mctx)
	zap.L().Info("executor exiting")
}
