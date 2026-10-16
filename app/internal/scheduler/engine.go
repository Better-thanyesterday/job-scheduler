package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	dagdomain "job-scheduler/app/internal/domain/dag"
	"go.uber.org/zap"
	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/domain/schedule"
	"job-scheduler/app/pkg/metrics"
)

// refreshInterval 触发点全量校准周期：wheel 不支持取消调度的
// 创建/启停/删除/推进全靠本周期对账（正常推进的下一跳由 trigger 成功后立即下发，不等校准）
const refreshInterval = 5 * time.Second

type Engine struct {
	wheel        *MultiTimeWheel
	scheduleRepo schedule.Repository // ListActive / UpdateNextRun / RecordHistory
	execReg      execution.Registry  // 登记执行
	dagRepo      dagdomain.DagRepo          // DAG 目标调度：加载定义
	dagExecRepo  dagdomain.DagExecutionRepo // DAG 目标调度：落运行记录
	tasks        schedule.TaskSource // 按 task_id 批量取任务
	pusher       coordination.TaskPusher
	locks        coordination.LockManager
	consumerN    int // 触发回调的并发上限

	// owns 分片过滤器：非 nil 时仅属主节点触发该调度。
	// 正确性不依赖它——nextRun 相等校验 + nextRun CAS + 抢票幂等兜底重叠窗口；
	// 分片只是消除多实例间的无效竞争与重复装载
	owns func(scheduleID int64) bool

	mu      sync.Mutex
	planned map[int64]time.Time // scheduleID → 已投进轮子的触发点（校准去重）
}

func NewEngine(scheduleRepo schedule.Repository, execReg execution.Registry,
	dagRepo dagdomain.DagRepo, dagExecRepo dagdomain.DagExecutionRepo,
	tasks schedule.TaskSource, pusher coordination.TaskPusher,
	locks coordination.LockManager, consumerN int) *Engine {
	if consumerN <= 0 {
		consumerN = 8
	}
	return &Engine{
		wheel:        NewMultiTimeWheel(),
		scheduleRepo: scheduleRepo,
		execReg:      execReg,
		dagRepo:      dagRepo,
		dagExecRepo:  dagExecRepo,
		tasks:        tasks,
		pusher:       pusher,
		locks:        locks,
		consumerN:    consumerN,
		planned:      make(map[int64]time.Time),
	}
}

// Run 阻塞运行：轮子推进 + 触发消费 + 周期校准。调用方放独立 goroutine。
// engine_up / engine_heartbeat 是 调度停摆告警的数据源：
// 校准成功才刷心跳——循环活着但 DB 持续失败时心跳冻结，告警如期触发
// SetShardFilter 注入分片过滤器（多活跃部署时由 cmd 层用成员表 HRW 构造）。
// nil = 不分片（单实例模式，全量触发）
func (e *Engine) SetShardFilter(fn func(scheduleID int64) bool) {
	e.owns = fn
}

// PendingSchedules 当前已投进轮子的触发点数（负载上报数据源）
func (e *Engine) PendingSchedules() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return int64(len(e.planned))
}

func (e *Engine) Run(ctx context.Context) {
	metrics.SetEngineUp(true)
	defer metrics.SetEngineUp(false)
	if err := e.refresh(ctx); err != nil {
		zap.L().Error("engine 启动装载失败", zap.Error(err))
	}
	wait := e.wheel.StartConsumer(ctx, e.consumerN)
	go e.wheel.Advance(ctx)
	refresh := time.NewTicker(refreshInterval)
	defer refresh.Stop()
	zap.L().Info("scheduler engine started (timewheel)")
	for {
		select {
		case <-ctx.Done():
			wait() // 等 consumer goroutine 收尾
			zap.L().Info("scheduler engine stopped")
			return
		case <-refresh.C:
			if err := e.refresh(ctx); err != nil {
				zap.L().Error("engine 校准失败", zap.Error(err))
				continue
			}
			metrics.SetEngineHeartbeat(time.Now())
		}
	}
}

// refresh 全量校准：只装 Running 调度；nextRun 为零的（新建还没算初始点）
// 用 cron 补算并回写；nextRun 与已投进轮子的不一致（被推进/重算/另一写方
// 改动）才重新下发，一致的跳过
func (e *Engine) refresh(ctx context.Context) error {
	list, err := e.scheduleRepo.ListActive(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, s := range list {
		if !s.Status().Active() { // 暂停/删除不装
			continue
		}
		if s.NextRun().IsZero() {
			if err := s.InitNextRun(now); err != nil {
				// 手动/API 触发类型没有 cron：不参与定时触发
				continue
			}
			if err := e.scheduleRepo.UpdateNextRun(ctx, s); err != nil {
				if errors.Is(err, schedule.ErrConflict) {
					// 另一实例已初始化该调度的触发点：正常分支，按库里的最新值装载
					fresh, getErr := e.scheduleRepo.Get(ctx, s.ID())
					if getErr != nil {
						zap.L().Error("回读调度失败", zap.Int64("schedule_id", s.ID()), zap.Error(getErr))
						continue
					}
					s = fresh
				} else {
					zap.L().Error("回写初始 nextRun 失败", zap.Int64("schedule_id", s.ID()), zap.Error(err))
					continue
				}
			}
		}
		e.planItem(s.ID(), s.TaskID(), s.NextRun())
	}
	return nil
}

// planItem 把触发点投进轮子（已投且未变的跳过）。
// planned[sID] 在触发时删除——失败/冲突/暂停的场景由下一轮校准自愈
func (e *Engine) planItem(scheduleID, taskID int64, at time.Time) {
	if at.IsZero() {
		return
	}
	e.mu.Lock()
	if known, ok := e.planned[scheduleID]; ok && known.Equal(at) {
		e.mu.Unlock()
		return
	}
	e.planned[scheduleID] = at
	e.mu.Unlock()

	if err := e.wheel.Schedule(scheduleID, taskID, at, func() { e.onFire(scheduleID, at) }); err != nil {
		e.mu.Lock()
		delete(e.planned, scheduleID)
		e.mu.Unlock()
		zap.L().Error("触发点投进时间轮失败",
			zap.Int64("schedule_id", scheduleID), zap.Time("next_run", at), zap.Error(err))
	}
}

// onFire 轮子到期回调：先做新鲜度校验再触发——wheel 无法取消，
// 暂停/删除/被另一写方推进的过期条目在这里被丢弃
func (e *Engine) onFire(scheduleID int64, planned time.Time) {
	e.mu.Lock()
	delete(e.planned, scheduleID)
	e.mu.Unlock()

	// 分片过滤：该调度的属主是其他活跃节点 → 本实例跳过（省一次 DB 读）
	if e.owns != nil && !e.owns(scheduleID) {
		return
	}

	s, err := e.scheduleRepo.Get(context.Background(), scheduleID)
	if err != nil {
		zap.L().Warn("触发点到期但调度不可得，丢弃", zap.Int64("schedule_id", scheduleID), zap.Error(err))
		return
	}
	if !s.Status().Active() {
		zap.L().Info("调度已暂停/删除，过期条目丢弃",
			zap.Int64("schedule_id", scheduleID), zap.String("status", s.Status().String()))
		return
	}
	if !s.NextRun().Equal(planned) {
		// 触发点已被推进（另一实例触发/补偿/手动重算）：本条目作废
		zap.L().Debug("触发点已过期作废",
			zap.Int64("schedule_id", scheduleID),
			zap.Time("planned", planned), zap.Time("db_next_run", s.NextRun()))
		return
	}
	if err := e.trigger(context.Background(), s, planned); err != nil {
		if errors.Is(err, schedule.ErrConflict) {
			return // 另一写方已认领：正常分支，等校准取最新
		}
		zap.L().Error("触发调度失败", zap.Int64("schedule_id", scheduleID), zap.Error(err))
		return
	}
	// 推进成功：下一跳立即入轮，不等校准周期
	e.planItem(s.ID(), s.TaskID(), s.NextRun())
}
