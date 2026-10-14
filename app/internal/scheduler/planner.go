package scheduler

import (
	"context"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/domain/schedule"
	"job-scheduler/app/internal/domain/task"
	"job-scheduler/app/pkg/metrics"

	"go.uber.org/zap"
)

// plannerInterval 重试扫描周期
const plannerInterval = 10 * time.Second

type Planner struct {
	repo   execution.RetryPlannerRepo
	tasks  schedule.TaskSource // 读 retry_policy 算退避
	pusher coordination.TaskPusher
}

func NewPlanner(repo execution.RetryPlannerRepo, tasks schedule.TaskSource,
	pusher coordination.TaskPusher) *Planner {
	return &Planner{repo: repo, tasks: tasks, pusher: pusher}
}

// RunPlanner 每 10s 一轮：扫描可重试执行 → 逐个规划重试或判终局 → 重投
func (p *Planner) RunPlanner(ctx context.Context) {
	ticker := time.NewTicker(plannerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.planOnce(ctx)
		}
	}
}

func (p *Planner) planOnce(ctx context.Context) {
	now := time.Now()
	execs, err := p.repo.ScanRetryable(ctx, now, 500)
	if err != nil {
		zap.L().Error("扫描可重试执行失败", zap.Error(err))
		return
	}
	metrics.SetRetryPending(float64(len(execs)))
	if len(execs) == 0 {
		return
	}

	// 批量取任务配置（缺任务 = 已被删，按终局处理）
	ids := make([]int64, 0, len(execs))
	for _, e := range execs {
		ids = append(ids, e.TaskID())
	}
	tasks, err := p.tasks.ListByIDs(ctx, ids)
	if err != nil {
		zap.L().Error("批量取任务失败", zap.Error(err))
		return
	}
	taskByID := make(map[int64]*task.Task, len(tasks))
	for _, t := range tasks {
		taskByID[t.ID()] = t
	}

	for _, e := range execs {
		// Retrying 僵尸：上次已 CAS 进 Retrying 但投递失败/进程崩溃，
		// 行到 next_retry_at 到期仍无人投递。直接重投同一 execution_id——
		// 不再走 PlanRetry（重试次数已 +1 过，不能重复计）；
		// 消费端 ClaimPending 接受 Retrying，投递幂等
		if e.Status() == execution.StatusRetrying {
			if t, exists := taskByID[e.TaskID()]; exists {
				if err := p.pusher.Push(ctx, BuildExecutionMessage(e, t)); err != nil {
					zap.L().Error("僵尸重试投递失败",
						zap.Int64("execution_id", e.ID()), zap.Error(err))
				}
			}
			continue
		}
		if e.RetryCount() >= e.MaxRetry() {
			if err := e.MarkFailedFinal("retry exhausted", now); err != nil {
				zap.L().Error("标记重试终局失败", zap.Int64("execution_id", e.ID()), zap.Error(err))
				continue
			}
			if err := p.repo.MarkFailedFinal(ctx, e); err != nil {
				zap.L().Error("持久化重试终局失败", zap.Int64("execution_id", e.ID()), zap.Error(err))
			} else {
				// 终局打点：increase(scheduler_task_final_failed_total[10m]) > 0
				// 即单任务失败告警的数据源（原 alert_events 轮询的替代）
				metrics.IncTaskExecuted("final_failed")
				metrics.IncTaskFinalFailed(e.TaskID())
			}
			continue
		}
		// 退避：直接用领域值对象的 NextDelay（固定间隔/指数退避+封顶）
		var backoff = time.Minute
		if t, ok := taskByID[e.TaskID()]; ok {
			backoff = t.RetryPolicy().NextDelay(e.RetryCount())
		}
		if _, err := e.PlanRetry(backoff, now); err != nil {
			zap.L().Error("规划重试失败", zap.Int64("execution_id", e.ID()), zap.Error(err))
			continue
		}
		ok, err := p.repo.PlanRetry(ctx, e)
		if err != nil {
			zap.L().Error("持久化重试规划失败", zap.Int64("execution_id", e.ID()), zap.Error(err))
			continue
		}
		if !ok {
			continue // 被其他 planner 实例抢先
		}
		// 重投同一 execution_id（消费端 ClaimPending 幂等）
		if t, exists := taskByID[e.TaskID()]; exists {
			if err := p.pusher.Push(ctx, BuildExecutionMessage(e, t)); err != nil {
				zap.L().Error("重试投递失败", zap.Int64("execution_id", e.ID()), zap.Error(err))
			}
		}
	}
}
