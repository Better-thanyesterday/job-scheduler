package scheduler

import (
	"context"
	"time"

	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/pkg/metrics"

	"go.uber.org/zap"
)

// reaperInterval 超时扫描周期
const reaperInterval = 15 * time.Second

// reapTimeout MVP 的固定超时阈值：Running 超过该时长未写终态即收割。
// 精确判定 start_time + task.timeout_sec 需要按任务取超时配置（P2 再做）
const reapTimeout = 10 * time.Minute

type Reaper struct {
	reaperRepo execution.ReaperRepo
}

func NewReaper(reaprepo execution.ReaperRepo) *Reaper {
	return &Reaper{
		reaperRepo: reaprepo,
	}
}

// RunReaper 每 15s 一次：
// 1. ScanTimeout：Running 且 start_time 早于阈值
// 2. e.Reap(now) → UpdateTerminal：置 Failed（后续重试由 planner 接管）
func (rp *Reaper) RunReaper(ctx context.Context) {
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rp.reapOnce(ctx)
		}
	}
}

func (rp *Reaper) reapOnce(ctx context.Context) {
	now := time.Now()
	execs, err := rp.reaperRepo.ScanTimeout(ctx, now.Add(-reapTimeout), 200)
	if err != nil {
		zap.L().Error("扫描超时执行失败", zap.Error(err))
		return
	}
	for _, e := range execs {
		if err := e.Reap(now); err != nil {
			zap.L().Error("收割状态流转失败", zap.Int64("execution_id", e.ID()), zap.Error(err))
			continue
		}
		if err := rp.reaperRepo.UpdateTerminal(ctx, e); err != nil {
			zap.L().Error("收割写终态失败", zap.Int64("execution_id", e.ID()), zap.Error(err))
			continue
		}
		metrics.IncTaskExecuted("timeout")
		zap.L().Warn("执行超时已收割", zap.Int64("execution_id", e.ID()))
	}
}
