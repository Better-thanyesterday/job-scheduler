package scheduler

import (
	"context"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/domain/schedule"

	"go.uber.org/zap"
)

// 投递补偿器（repusher）：
//
//	gRPC 推送失败的执行永远停在 Pending——登记与投递分离只做了分离，
//	没有补偿它们就只有人工重投一条路。对 DAG 伤害最大：节点 Running +
//	execution Pending → dag reaper 因有在飞节点拒绝收割 → 运行永久卡死。
//
//	每 60s 扫一次"Pending 且登记超过 2 分钟"的执行（cron 与 DAG 共用一张表，
//	一个循环全兜），按行内 task_id 重取任务快照、重建消息、重新 Push。
//	幂等由消费端 ClaimPending 抢票保证；重复推送最坏是被幂等丢弃。
//
//	阈值 2 分钟的依据：推送在途时执行尚未被认领（Claim 是 handler 第一步），
//	正常认领在秒级；超过 2 分钟仍 Pending 基本可断定推送已失败。
const (
	// repushInterval 补偿扫描周期
	repushInterval = time.Minute
	// pendingGrace 判定"投递已失败"的 Pending 停留阈值
	pendingGrace = 2 * time.Minute
	// repushBatch 每轮最多补偿多少条
	repushBatch = 200
)

type Repusher struct {
	scan   execution.PendingRepushRepo
	tasks  schedule.TaskSource
	pusher coordination.TaskPusher
}

func NewRepusher(scan execution.PendingRepushRepo, tasks schedule.TaskSource,
	pusher coordination.TaskPusher) *Repusher {
	return &Repusher{scan: scan, tasks: tasks, pusher: pusher}
}

// RunRepusher 阻塞循环（调用方放独立 goroutine）
func (rp *Repusher) RunRepusher(ctx context.Context) {
	ticker := time.NewTicker(repushInterval)
	defer ticker.Stop()
	zap.L().Info("execution repusher started",
		zap.Duration("interval", repushInterval), zap.Duration("pending_grace", pendingGrace))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rp.repushOnce(ctx)
		}
	}
}

func (rp *Repusher) repushOnce(ctx context.Context) {
	execs, err := rp.scan.ScanStalePending(ctx, time.Now().Add(-pendingGrace), repushBatch)
	if err != nil {
		zap.L().Error("扫描未投递执行失败", zap.Error(err))
		return
	}
	if len(execs) == 0 {
		return
	}
	zap.L().Warn("发现投递失败的执行，开始补偿重投", zap.Int("count", len(execs)))
	for _, e := range execs {
		if ctx.Err() != nil {
			return
		}
		t, err := rp.tasks.Get(ctx, e.TaskID())
		if err != nil {
			// 任务没了：这条执行永远无法重建消息，只能人工处理。
			// 不能标终态——投递补偿器没有执行结果，标 Failed 是伪造事实
			zap.L().Error("补偿重投取不到任务快照，执行保持 Pending（需人工处理）",
				zap.Int64("execution_id", e.ID()), zap.Int64("task_id", e.TaskID()), zap.Error(err))
			continue
		}
		if err := rp.pusher.Push(ctx, BuildExecutionMessage(e, t)); err != nil {
			// 本轮推不出去：下一轮扫描还会捞到它（阈值已过，持续重试）
			zap.L().Error("补偿重投失败",
				zap.Int64("execution_id", e.ID()), zap.Error(err))
			continue
		}
		zap.L().Info("补偿重投完成", zap.Int64("execution_id", e.ID()))
	}
}
