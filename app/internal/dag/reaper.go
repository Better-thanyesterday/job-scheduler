package dag

import (
	"context"
	"errors"
	"fmt"
	"time"

	"job-scheduler/app/internal/domain/coordination"

	"go.uber.org/zap"
)

// RunReaper 死运行收割循环：独立于推进循环（频率低得多），
// 把长时间无推进的运行强制收场，接线方 go engine.RunReaper(ctx)
func (e *Engine) RunReaper(ctx context.Context) {
	ticker := time.NewTicker(staleAfter / 4)
	defer ticker.Stop()
	zap.L().Info("dag reaper started", zap.Duration("stale_after", staleAfter))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.reapOnce(ctx)
		}
	}
}

// reapOnce 收割一轮。Repo 的扫描已限定"无 Running 节点"，
// 这里再在实体上确认一次（扫描与收割之间可能有新派发）
func (e *Engine) reapOnce(ctx context.Context) {
	runs, broken, err := e.execRepo.ListStaleRunning(ctx, time.Now().Add(-staleAfter), e.batchSize)
	if err != nil {
		zap.L().Error("扫描卡死的 DAG 运行失败", zap.Error(err))
		return
	}
	e.quarantineBroken(ctx, broken)
	for _, de := range runs {
		lock, err := e.locks.Acquire(ctx, fmt.Sprintf("dagexec:%d", de.ID()), lockTTL)
		if err != nil {
			if errors.Is(err, coordination.ErrLockNotAcquired) {
				continue // 推进循环正拿着这把锁干活，说明没死透
			}
			if errors.Is(err, coordination.ErrLockUnavailable) {
				// 收割选择 fail-closed：这轮跳过（收割 15 分钟一轮，不急），
				// 无锁强收与无锁推进叠加时的竞争窗口不值得冒
				zap.L().Warn("锁服务不可用，本轮跳过收割", zap.Error(err))
				return
			}
			zap.L().Error("收割获取运行锁失败",
				zap.Int64("dag_execution_id", de.ID()), zap.Error(err))
			continue
		}
		now := time.Now()
		if de.RunningCount() > 0 {
			_ = e.locks.Release(ctx, lock)
			continue // 有在飞执行：不收割
		}
		changed, err := de.Reap(now)
		if err != nil {
			zap.L().Error("收割状态流转失败",
				zap.Int64("dag_execution_id", de.ID()), zap.Error(err))
			_ = e.locks.Release(ctx, lock)
			continue
		}
		if changed {
			if err := e.execRepo.Update(ctx, de); err != nil {
				zap.L().Error("收割落库失败",
					zap.Int64("dag_execution_id", de.ID()), zap.Error(err))
				_ = e.locks.Release(ctx, lock)
				continue
			}
			zap.L().Warn("DAG 运行超时未推进，已强制收场为 Failed",
				zap.Int64("dag_execution_id", de.ID()),
				zap.Int64("dag_id", de.DagID()),
				zap.Duration("stale_after", staleAfter))
		}
		_ = e.locks.Release(ctx, lock)
	}
}
