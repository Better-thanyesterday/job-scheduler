package main

import (
	"context"
	"encoding/json"
	"fmt"
	"job-scheduler/app/internal/dag"
	"job-scheduler/app/internal/domain/coordination"
	infraetcd "job-scheduler/app/internal/infra/etcd"
	"job-scheduler/app/internal/infra/mq"
	"job-scheduler/app/internal/infra/mysql"
	repo "job-scheduler/app/internal/infra/mysql/sqlx/repo"
	"job-scheduler/app/internal/scheduler"
	"job-scheduler/app/pkg/logger"
	"job-scheduler/app/pkg/snowflake"
	"job-scheduler/app/settings"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

func main() {
	// 1. 配置 + 日志
	if err := settings.Init(); err != nil {
		fmt.Printf("init settings failed: %v\n", err)
		return
	}
	if err := logger.Init(settings.Conf.Load().LogConfig, settings.Conf.Load().Mode, "dag"); err != nil {
		fmt.Printf("init logger failed: %v\n", err)
		return
	}
	defer zap.L().Sync()

	// 2. MySQL（分域连接池；锁已改走 etcd，schedule 池按需保留给其他组件）
	_, taskDB, _, executionDB, dagDB := mysql.InitAll(settings.Conf.Load().MySqls)
	defer mysql.CloseAll()

	// 3. 雪花算法（execution.New 内部用它分配执行 ID）
	if err := snowflake.Init(settings.Conf.Load().StartTime, settings.Conf.Load().MachineID); err != nil {
		fmt.Printf("init snowflake failed: %v\n", err)
		return
	}

	// 4. etcd + 执行消息推送器（gRPC 推送，按 etcd 成员表发现 executor）
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
	//    引擎侧对 ErrLockUnavailable fail-open，锁只是效率优化不是正确性依赖
	locks := infraetcd.NewLockManager(etcdCli, 30)

	// 6. RabbitMQ：结果队列消费（唤醒加速）。消费失败不致命——tick 轮询兜底
	var engine *dag.Engine
	wakeHandler := func(ctx context.Context, body []byte) error {
		var r coordination.ExecutionResult
		if err := json.Unmarshal(body, &r); err != nil {
			return nil // 脏消息：ack 丢弃（重投也还是脏的）
		}
		if r.DagExecID > 0 {
			engine.Wake(r.DagExecID)
		}
		return nil
	}

	// 7. 组装组件
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine = dag.NewEngine(repo.NewDagExecutionRepo(dagDB),
		repo.NewExecutionRegistryRepo(executionDB),
		repo.NewTaskSourceRepo(taskDB),
		pusher, locks, 4*time.Second)
	go engine.Run(ctx)
	go engine.RunReaper(ctx)

	if mqCfg := settings.Conf.Load().MQConfig; mqCfg != nil {
		mqClient, err := mq.NewClient(&mq.Config{
			Host: mqCfg.Host, Port: mqCfg.Port,
			User: mqCfg.User, Password: mqCfg.Password, VHost: mqCfg.VHost,
			ConfirmMode: mqCfg.ConfirmMode, PrefetchCount: mqCfg.PrefetchCount,
		})
		// 结果队列只做"节点完成 → Wake"的加速：失败不退出，tick 轮询兜底正确性
		if err != nil {
			zap.L().Warn("MQ 初始化失败：结果唤醒消费降级为 tick 轮询兜底", zap.Error(err))
		} else {
			defer mqClient.Close()
			if err := mqClient.DeclareAll(); err != nil {
				zap.L().Warn("MQ 拓扑声明失败：结果唤醒消费降级为 tick 轮询兜底", zap.Error(err))
			} else {
				// dag 族三条结果队列各起一个消费 goroutine，处理只是Wake，无状态
				for _, q := range []string{mq.QueueDagSuccess, mq.QueueDagFailed, mq.QueueDagTimeout} {
					go mqClient.NewConsumer(q, 100).Run(ctx, wakeHandler)
				}
			}
		}
	} else {
		zap.L().Info("mq config missing: 结果唤醒消费未启用（tick 轮询兜底）")
	}
	zap.L().Info("dag started")

	// 7. 等待退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()
	zap.L().Info("dag exiting")
}
