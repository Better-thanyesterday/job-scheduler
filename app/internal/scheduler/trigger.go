package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/domain/schedule"
	"job-scheduler/app/internal/domain/task"
	"job-scheduler/app/pkg/metrics"

	"go.uber.org/zap"
)

const (
	lockTTL = 30 * time.Second
	// fireAllRuntimeCap 运行期滞后时 FireAll 单次最多逐点补发的错过点数，
	// 超过即归位 now（防高频表达式 × 长积压的无界连环触发）
	fireAllRuntimeCap = 30
)

// trigger 执行一次触发，planned 是理论触发点（堆里的 nextRun），
// 与实际执行时刻的差值即调度偏差（lag_ms 落 history）
func (e *Engine) trigger(ctx context.Context, s *schedule.Schedule, planned time.Time) error {
	// 分片过滤：非属主跳过（compensator 路径也经此统一收口）
	if e.owns != nil && !e.owns(s.ID()) {
		return errSkipTrigger
	}
	// 抢锁（非阻塞）。锁服务不可用时 fail-open 无锁继续：
	// nextRun CAS 推进才是互斥的事实源，锁只是减少无效竞争的优化
	var lock *coordination.LockHandle
	err := func() error {
		l, err := e.locks.Acquire(ctx, fmt.Sprintf("schedule:%d", s.ID()), lockTTL)
		switch {
		case err == nil:
			lock = l
			return nil
		case errors.Is(err, coordination.ErrLockNotAcquired):
			zap.L().Debug("调度锁已被其他实例持有，跳过",
				zap.Int64("schedule_id", s.ID()))
			return errSkipTrigger
		case errors.Is(err, coordination.ErrLockUnavailable):
			zap.L().Warn("锁服务不可用，fail-open 无锁触发（nextRun CAS 兜底）",
				zap.Int64("schedule_id", s.ID()), zap.Error(err))
			return nil
		default:
			return fmt.Errorf("acquire lock: %w", err)
		}
	}()
	if err != nil {
		return err
	}
	if lock != nil {
		defer func() {
			_ = e.locks.Release(ctx, lock)
		}()
	}

	// 推进锚点落库（CAS：0 行 = 别的实例/api 已认领该时间点 → 放弃）。
	// 积压导致 nextRun 仍在过去时按 misfire 策略收口，否则堆会立刻再次
	// 弹出、无策略地连环补触发；FireAll 限 fireAllRuntimeCap 点防失控
	if err := s.Advance(planned); err != nil {
		return err
	}
	if s.NextRun().Before(time.Now()) {
		switch s.Misfire() {
		case schedule.MisfireSkip, schedule.MisfireMerge:
			_ = s.Advance(time.Now())
		default:
			jumped := 0
			for s.NextRun().Before(time.Now()) && jumped < fireAllRuntimeCap {
				_ = s.Advance(s.NextRun())
				jumped++
			}
			if s.NextRun().Before(time.Now()) {
				_ = s.Advance(time.Now())
			}
		}
		zap.L().Warn("调度触发滞后超阈值，已按 misfire 策略收口 next_run",
			zap.Int64("schedule_id", s.ID()),
			zap.Time("planned", planned),
			zap.Time("next_run", s.NextRun()))
	}
	if err := e.scheduleRepo.UpdateNextRun(ctx, s); err != nil {
		if errors.Is(err, schedule.ErrConflict) {
			zap.L().Debug("调度推进冲突，另一写方已认领该时间点",
				zap.Int64("schedule_id", s.ID()))
			return schedule.ErrConflict
		}
		return err
	}

	// 任务配置快照。任务被并发删除时 nextRun 已推进，不会重复触发
	t, err := e.tasks.Get(ctx, s.TaskID())
	if err != nil {
		return fmt.Errorf("%w: task=%d", ErrNoTaskSnapshot, s.TaskID())
	}

	exec, err := execution.New(s.TaskID(), s.ID(), t.RetryPolicy().MaxRetry(),
		strconv.FormatInt(s.ID(), 10), time.Now())
	if err != nil {
		return err
	}
	if err := e.execReg.Create(ctx, exec); err != nil {
		return err
	}

	// 投递失败不回滚登记：exec 已落库，由 Repusher/人工重投
	if err := e.pusher.Push(ctx, BuildExecutionMessage(exec, t)); err != nil {
		zap.L().Error("投递执行消息失败",
			zap.Int64("execution_id", exec.ID()), zap.Error(err))
		return fmt.Errorf("%w: %v", ErrTriggerFailed, err)
	}

	now := time.Now()
	if err := e.scheduleRepo.RecordHistory(ctx, &schedule.RunHistory{
		ScheduleID:  s.ID(),
		ExecutionID: exec.ID(),
		TriggerTime: now,
		PlannedTime: planned,
		Status:      1,
		LagMS:       now.Sub(planned).Milliseconds(),
		TriggeredBy: schedule.Cron,
	}); err != nil {
		zap.L().Error("记录调度历史失败", zap.Int64("schedule_id", s.ID()), zap.Error(err))
	}

	metrics.IncTaskTriggered(s.ID())
	metrics.ObserveScheduleLag(now.Sub(planned).Seconds())
	return nil
}

func BuildExecutionMessage(exec *execution.Execution, t *task.Task) *coordination.ExecutionMessage {
	cfg, _ := task.MarshalConfig(t.Config())
	return &coordination.ExecutionMessage{
		EnqueuedAt: time.Now(), // 队列滞留观测：executor 消费时对 now-EnqueuedAt 打直方图
		ExecID:     exec.ID(),
		TaskID:     exec.TaskID(),
		ScheduleID: exec.ScheduleID(),
		DagExecID:  exec.DagExecID(),
		DagNodeID:  exec.DagNodeID(),
		TaskType:   int32(t.Type()),
		MaxRetry:   int32(t.RetryPolicy().MaxRetry()),
		Config:     cfg,
		Priority:   t.Priority(),
		TimeoutSec: int32(t.Timeout().Seconds()),
		TraceID:    exec.TraceID(),
	}
}
