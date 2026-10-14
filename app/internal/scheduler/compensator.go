package scheduler

import (
	"context"
	"errors"
	"time"

	"job-scheduler/app/internal/domain/schedule"

	"go.uber.org/zap"
)

const (
	compensatorInterval = 30 * time.Second
	// misfireGrace nextRun 过期超过该时长才按错过处理，
	// 避免与 engine 正常触发竞争（engine 1s 扫描 + 5s 重建，30s 绰绰有余）
	misfireGrace = 60 * time.Second
)

// RunCompensator 每 30s 补发一轮：筛出 Running 且 nextRun 过期超阈值的调度，
// 按 misfire 策略逐点走 trigger。与 engine 天然去重：都走 GET_LOCK/CAS
func (e *Engine) RunCompensator(ctx context.Context) {
	ticker := time.NewTicker(compensatorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.compensateOnce(ctx)
		}
	}
}

func (e *Engine) compensateOnce(ctx context.Context) {
	now := time.Now()
	schedules, err := e.scheduleRepo.ListActive(ctx)
	if err != nil {
		zap.L().Error("补发扫描失败", zap.Error(err))
		return
	}
	for _, s := range schedules {
		if !s.Status().Active() {
			continue
		}
		if s.NextRun().IsZero() || !s.NextRun().Add(misfireGrace).Before(now) {
			continue
		}
		// 触发前按 DB 复核状态：手里是装载时快照，装载之后 api 可能已 Pause。
		// 不复核：多触发一次，且快照写回会把暂停静默打回运行
		fresh, err := e.scheduleRepo.Get(ctx, s.ID())
		if err != nil {
			zap.L().Error("补发前复核调度状态失败",
				zap.Int64("schedule_id", s.ID()), zap.Error(err))
			continue
		}
		if !fresh.Status().Active() {
			continue
		}
		// lastRun 零值（从未跑过）没有"错过的点"：MisfirePlan 会从公元 1 年
		// 开始枚举出远古 planned。只补触发一次 nextRun 本身（首个合法点）
		if s.LastRun().IsZero() {
			if err := e.trigger(ctx, s, s.NextRun()); err != nil &&
				!errors.Is(err, schedule.ErrConflict) && !errors.Is(err, errSkipTrigger) {
				zap.L().Error("补触发从未运行的调度失败",
					zap.Int64("schedule_id", s.ID()), zap.Error(err))
			}
			continue
		}
		for _, planned := range s.MisfirePlan(s.LastRun(), now, 30) {
			if err := e.trigger(ctx, s, planned); err != nil {
				if errors.Is(err, schedule.ErrConflict) || errors.Is(err, errSkipTrigger) {
					break // 时间点已被认领/分片属其他节点：正常分支
				}
				zap.L().Error("补发触发失败",
					zap.Int64("schedule_id", s.ID()), zap.Error(err))
				break // 该调度本轮失败就停，下轮再试
			}
		}
	}
}
