package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"encoding/json"

	infraetcd "job-scheduler/app/internal/infra/etcd"
	"job-scheduler/app/internal/infra/mysql"
	repo "job-scheduler/app/internal/infra/mysql/sqlx/repo"
	"job-scheduler/app/internal/scheduler"
	"job-scheduler/app/internal/scheduler/membership"
	"job-scheduler/app/pkg/logger"
	"job-scheduler/app/pkg/metrics"
	"job-scheduler/app/pkg/snowflake"
	"job-scheduler/app/settings"

	"go.uber.org/zap"
)

func main() {
	// 1. 配置 + 日志
	if err := settings.Init(); err != nil {
		fmt.Printf("init settings failed: %v\n", err)
		return
	}
	if err := logger.Init(settings.Conf.Load().LogConfig, settings.Conf.Load().Mode, "scheduler"); err != nil {
		fmt.Printf("init logger failed: %v\n", err)
		return
	}
	defer zap.L().Sync()

	// 2. MySQL（分域连接池：system/task/schedule/execution/dag；锁走 schedule 库连接）
	_, taskDB, scheduleDB, executionDB, _ := mysql.InitAll(settings.Conf.Load().MySqls)
	defer mysql.CloseAll()

	// 3. 雪花算法（execution.New 内部用它分配执行 ID）
	if err := snowflake.Init(settings.Conf.Load().StartTime, settings.Conf.Load().MachineID); err != nil {
		fmt.Printf("init snowflake failed: %v\n", err)
		return
	}

	// 4. etcd：选主 + 执行消息推送器（gRPC 推送，按 etcd 成员表发现 executor）
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
	pusher := scheduler.NewPusher(etcdCli)

	// 5. 锁管理器：etcd 实现（租约 + 事务 CAS）。
	//    trigger 对 ErrLockUnavailable fail-open，nextRun 推进锚点兜底正确性
	locks := infraetcd.NewLockManager(etcdCli, 30)

	// 6. 本实例标识（etcd 选主 value / 日志对账用）
	nodeID := settings.Conf.Load().WorkerID
	if nodeID == "" {
		nodeID, _ = os.Hostname()
		nodeID = fmt.Sprintf("%s-%d", nodeID, os.Getpid())
	}

	// 7. 组件（只构造，不启动——等当选）。
	//    repusher（投递补偿器）独立于分片：它自带幂等与阈值，
	//    多实例并行只会有重复推送被抢票丢弃，无须按成员协调
	engine := scheduler.NewEngine(
		repo.NewScheduleRepo(scheduleDB),
		repo.NewExecutionRegistryRepo(executionDB),
		repo.NewTaskSourceRepo(taskDB), pusher, locks, 0)
	planner := scheduler.NewPlanner(repo.NewRetryPlannerRepo(executionDB),
		repo.NewTaskSourceRepo(taskDB), pusher)
	reaper := scheduler.NewReaper(repo.NewReaperRepo(executionDB))
	repusher := scheduler.NewRepusher(repo.NewExecutionRegistryRepo(executionDB),
		repo.NewTaskSourceRepo(taskDB), pusher)

	// 8. /metrics：Prometheus 抓取端口（独立 mux 不走业务中间件，默认 9903，
	//    与 executor 的 9902 区分）。engine/planner/reaper 的埋点都从这暴露
	metricsPort := settings.Conf.Load().MetricsPort
	if metricsPort == 0 {
		metricsPort = 9903
	}
	metricsSrv := &http.Server{Addr: fmt.Sprintf(":%d", metricsPort), Handler: metrics.Handler()}
	go func() {
		zap.L().Info("scheduler metrics listening", zap.Int("port", metricsPort))
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			zap.L().Error("metrics server failed", zap.Error(err))
		}
	}()

	// 9. 多活跃分片模型：所有活跃节点都跑全量调度循环，
	//    触发权按 HRW 分片归属（成员表变动自动再均衡）。
	//    正确性不依赖分片：GET_LOCK + nextRun CAS + 抢票幂等兜底重叠窗口。
	//    9.1 成员注册（租约 15s：崩溃自动退表）+ 负载上报
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	initLoad := membership.NodeLoad{NodeID: nodeID}
	reg, err := membership.Register(ctx, etcdCli,
		membership.NodesPrefix+nodeID, mustNodeLoadJSON(initLoad), 15)
	if err != nil {
		fmt.Printf("register member failed: %v\n", err)
		return
	}
	defer reg.Close(ctx)
	loader := membership.NewLoadReporter(reg, 10*time.Second, func() *membership.NodeLoad {
		initLoad.PendingScheduleCount = engine.PendingSchedules()
		initLoad.UpdateAt = time.Now()
		return &initLoad
	})
	go loader.Run(ctx)

	// 9.2 成员表 watch → HRW 分片过滤器注入 engine；
	//     成员变动（扩缩容/崩溃退表）下一个触发点自动按新视图归属
	members, err := membership.WatchNodeInfo(ctx, etcdCli, nil)
	if err != nil {
		fmt.Printf("watch node info failed: %v\n", err)
		return
	}
	engine.SetShardFilter(func(scheduleID int64) bool {
		owner, err := members.Assign(scheduleID)
		if err != nil {
			// 成员表空（启动窗口/全体掉线）：fail-open 全量触发，
			// GET_LOCK + nextRun CAS 兜底正确性
			return true
		}
		return owner == nodeID
	})

	// 9.3 调度循环全量启动（多活跃）；投递补偿器独立常驻
	go engine.Run(ctx)
	go engine.RunCompensator(ctx)
	go planner.RunPlanner(ctx)
	go reaper.RunReaper(ctx)
	go repusher.RunRepusher(ctx)

	zap.L().Info("scheduler started (multi-active shard)", zap.String("node_id", nodeID))

	// 10. 等待退出信号：注销成员（其余实例立即感知），再收剩余 goroutine
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()
	_ = metricsSrv.Shutdown(context.Background())
	zap.L().Info("scheduler exiting")
}

// mustNodeLoadJSON 成员表注册值的序列化
func mustNodeLoadJSON(load membership.NodeLoad) string {
	b, _ := json.Marshal(load)
	return string(b)
}
