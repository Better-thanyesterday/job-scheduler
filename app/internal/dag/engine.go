// Package dag DAG 编排引擎：驱动一次运行内部的节点调度。
//
// 与 scheduler.Engine（时间驱动，cron 到点触发）不同，这里是状态驱动：
// 节点到达终态 → 下游依赖判定 → 派发就绪节点 → 尝试收尾。
// 节点执行完全复用现有链路：execution 登记 → MQ 投递 → worker 认领，
// 重试由 execution 的 planner 托管、超时由 reaper 托管，本引擎只看终态。
package dag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	dagdomain "job-scheduler/app/internal/domain/dag"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/domain/schedule"
	"job-scheduler/app/internal/domain/task"
	"job-scheduler/app/pkg/metrics"

	"go.uber.org/zap"
)

const (
	// defaultInterval 推进扫描周期：节点粒度的调度延迟上界。
	// MQ 唤醒消费（见 Wake）把它压到毫秒级，本周期退化为兜底轮询
	defaultInterval = 5 * time.Second
	// defaultBatch 每轮最多推进多少次运行（防止单轮拖太久）
	defaultBatch = 100
	// lockTTL 单次推进动作的锁持有上限（与 scheduler engine 的 trigger 锁一致）
	lockTTL = 30 * time.Second
	// staleAfter 判定运行"卡死"的静默阈值：无在飞执行、又这么久没有任何
	// 状态推进（updated_at 不再变化），基本只有任务快照永久取不到一种成因。
	// 要远大于正常节点的执行时长上限（那些有 execution 的 planner/reaper 兜底）
	staleAfter = time.Hour
	// wakeQueueLen 唤醒队列容量。满了就丢——唤醒只是加速信号，
	// 丢了由下一轮 tick 兜底（DB 终态才是事实源）
	wakeQueueLen = 256
)

type Engine struct {
	execRepo  dagdomain.DagExecutionRepo // ListRunning / Update / Get
	execReg   execution.Registry         // 节点 execution 的登记与终态查询
	tasks     schedule.TaskSource        // 按 task_id 取任务快照（构建消息）
	pusher    coordination.TaskPusher
	locks     coordination.LockManager
	interval  time.Duration
	batchSize int
	wakeCh    chan int64 // 结果消息带来的"待推进运行"唤醒信号
}

func NewEngine(execRepo dagdomain.DagExecutionRepo, execReg execution.Registry,
	tasks schedule.TaskSource, pusher coordination.TaskPusher,
	locks coordination.LockManager, interval time.Duration) *Engine {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Engine{
		execRepo:  execRepo,
		execReg:   execReg,
		tasks:     tasks,
		pusher:    pusher,
		locks:     locks,
		interval:  interval,
		batchSize: defaultBatch,
		wakeCh:    make(chan int64, wakeQueueLen),
	}
}

// Wake 结果消费方（cmd 层的 MQ consumer）收到 dag.* 结果消息后调用：
// 标记该运行待推进。★ 语义是"加速信号"而非"状态驱动"——消息可能重复、
// 乱序、丢失，推进时终态判定仍以 DB 为准（见 advanceLocked 的注释）。
// 非阻塞：队列满即丢弃
func (e *Engine) Wake(dagExecID int64) {
	select {
	case e.wakeCh <- dagExecID:
	default:
		zap.L().Debug("唤醒队列已满，丢弃唤醒信号（tick 兜底）",
			zap.Int64("dag_execution_id", dagExecID))
	}
}

// Run 周期推进所有运行中的 DAG，直到 ctx 取消。
// 与 planner/reaper 同款循环；接线方在 cmd 里 go engine.Run(ctx) 即可。
// tick 与 MQ 唤醒共用同一推进路径，互不抢锁（per-run 锁串行化）
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	zap.L().Info("dag engine started", zap.Duration("interval", e.interval))
	for {
		select {
		case <-ctx.Done():
			zap.L().Info("dag engine stopped")
			return
		case <-ticker.C:
			e.advanceOnce(ctx)
		case id := <-e.wakeCh:
			if err := e.advanceByID(ctx, id); err != nil {
				zap.L().Error("唤醒推进 DAG 运行失败",
					zap.Int64("dag_execution_id", id), zap.Error(err))
			}
		}
	}
}

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

// advanceOnce 一轮：扫描全部运行中的 DAG，逐个推进。
// 单运行失败只记日志，不影响其他运行
func (e *Engine) advanceOnce(ctx context.Context) {
	runs, broken, err := e.execRepo.ListRunning(ctx, e.batchSize)
	if err != nil {
		zap.L().Error("扫描运行中的 DAG 失败", zap.Error(err))
		return
	}
	e.quarantineBroken(ctx, broken)
	for _, de := range runs {
		if err := e.advanceRun(ctx, de); err != nil && !errors.Is(err, errSkipRun) {
			zap.L().Error("推进 DAG 运行失败",
				zap.Int64("dag_execution_id", de.ID()), zap.Error(err))
		}
	}
}

// quarantineBroken 定义被运行期间改坏、无法装配的运行收场为 Failed 终态。
// 不隔离则永远卡 Running（装配不出实体，推进和收割都碰不到它）
func (e *Engine) quarantineBroken(ctx context.Context, broken []dagdomain.BrokenRun) {
	for _, b := range broken {
		if err := e.execRepo.MarkRunDead(ctx, b.RunID); err != nil {
			zap.L().Error("隔离无法装配的 DAG 运行失败，下轮重试",
				zap.Int64("dag_execution_id", b.RunID),
				zap.Int64("dag_id", b.DagID), zap.Error(err))
			continue
		}
		zap.L().Warn("DAG 定义在运行期间被修改，运行无法装配，已强制收场为 Failed",
			zap.Int64("dag_execution_id", b.RunID),
			zap.Int64("dag_id", b.DagID),
			zap.Error(b.Err))
		metrics.IncDagBrokenIsolated()
	}
}

// advanceRun 推进一次运行（tick 路径）：抢锁 → advanceLocked
func (e *Engine) advanceRun(ctx context.Context, de *dagdomain.DagExecution) error {
	_, release, err := e.acquireRunLock(ctx, de.ID())
	if err != nil {
		return err
	}
	defer release()
	return e.advanceLocked(ctx, de)
}

// advanceByID 按运行 ID 推进（MQ 唤醒路径）：抢锁 → 取最新实体 → advanceLocked。
// 锁内重取而不是用唤醒时的快照：唤醒消息可能陈旧，实体必须是当下的
func (e *Engine) advanceByID(ctx context.Context, dagExecID int64) error {
	_, release, err := e.acquireRunLock(ctx, dagExecID)
	if err != nil {
		return err
	}
	defer release()

	de, err := e.execRepo.Get(ctx, dagExecID)
	if err != nil {
		if errors.Is(err, dagdomain.ErrNotFound) {
			return nil // 运行已不存在（被清理）：唤醒信号过期，静默丢弃
		}
		return fmt.Errorf("load dag execution %d: %w", dagExecID, err)
	}
	return e.advanceLocked(ctx, de)
}

// acquireRunLock 统一的 per-run 锁获取。
// 返回的 release 恒非 nil（未持锁时是 no-op），调用方无脑 defer 即可。
// 锁策略：
//   - 被其他实例持有（ErrLockNotAcquired）→ 正常跳过本轮，返回哨兵 errSkipRun
//   - 锁服务不可用（ErrLockUnavailable）→ fail-open：无锁继续推进。
//     正确性由 DB 侧守卫兜底（主行状态 CAS、节点前驱 CAS、抢票幂等），
//     锁在这里只是减少无效竞争的效率优化，不是正确性依赖
var errSkipRun = errors.New("dag: skipped (lock held by another instance)")

func (e *Engine) acquireRunLock(ctx context.Context, dagExecID int64) (
	*coordination.LockHandle, func(), error) {
	lock, err := e.locks.Acquire(ctx, fmt.Sprintf("dagexec:%d", dagExecID), lockTTL)
	switch {
	case err == nil:
		return lock, func() { _ = e.locks.Release(ctx, lock) }, nil
	case errors.Is(err, coordination.ErrLockNotAcquired):
		zap.L().Debug("DAG 运行锁已被其他实例持有，跳过",
			zap.Int64("dag_execution_id", dagExecID))
		return nil, func() {}, errSkipRun
	case errors.Is(err, coordination.ErrLockUnavailable):
		zap.L().Warn("锁服务不可用，fail-open 无锁推进（DB 守卫兜底）",
			zap.Int64("dag_execution_id", dagExecID), zap.Error(err))
		return nil, func() {}, nil
	default:
		return nil, func() {}, fmt.Errorf("acquire lock: %w", err)
	}
}

// advanceLocked 推进一次运行（调用方必须已持 per-run 锁）：
//
//	同步节点终态 → fail_fast/级联跳过 → 派发就绪节点 → 落库 → 投递 → 收尾
//
// 落库前的所有变更只发生在内存，最后由 execRepo.Update 一次事务写入；
// 投递在落库之后（登记与投递分离，与 cron engine 的 trigger 同一纪律）。
//
// ★ 终态判定只认 DB：唤醒消息/内存实体的成败都不是事实源。failed 结果
// 消息是"单次失败"（重试归 planner），绝不能直接驱动节点终态——这里
// 只按 exec 行的真实状态同步，重试中的执行在节点视角仍是 Running
func (e *Engine) advanceLocked(ctx context.Context, de *dagdomain.DagExecution) error {
	// 已收尾的运行被陈旧唤醒信号重复触发：直接短路，不做任何读写
	if de.Status().IsTerminal() {
		return nil
	}

	now := time.Now()

	// 1. 终态同步：Running 节点绑定的 execution 已到终态的，落成节点终态。
	//    execution 只到 Failed（等待 planner 重试）时节点保持 Running
	changed, err := e.syncFinishedNodes(ctx, de, now)
	if err != nil {
		return err
	}

	// 2. fail_fast：已开开关且有节点失败 → 跳过所有还没跑的节点。
	//    正在 Running 的节点无法跳过（迁移表不允许），等它自然终结后再收尾
	if de.ShouldAbort() {
		for _, ne := range de.NodeExecutions() {
			if ne.Status() == dagdomain.DagNodeStatusWaiting {
				if err := ne.Skip(now); err != nil {
					zap.L().Warn("fail_fast 跳过节点失败",
						zap.Int64("dag_execution_id", de.ID()),
						zap.String("node_id", ne.NodeID()), zap.Error(err))
					continue
				}
				changed = true
			}
		}
	}

	// 3. 级联跳过：上游已终态但边永远无法满足的等待节点
	for _, n := range de.StalledNodes() {
		if err := de.Node(n.NodeID()).Skip(now); err != nil {
			zap.L().Warn("级联跳过节点失败",
				zap.Int64("dag_execution_id", de.ID()),
				zap.String("node_id", n.NodeID()), zap.Error(err))
			continue
		}
		changed = true
	}

	// 4. 派发就绪节点（登记 execution + 节点 Bind）
	msgs, dispatched, err := e.dispatchReady(ctx, de, now)
	if err != nil {
		return err
	}
	if dispatched {
		changed = true
	}

	// 5. 收尾：全部节点终态时归档整体状态（含 end_time/duration_ms）
	finalized, err := de.Finalize(now)
	if err != nil {
		return err
	}
	if finalized {
		changed = true
	}

	// 6. 落库：节点状态 + 主行一次事务写入；本轮无任何变更则跳过
	if changed {
		if err := e.execRepo.Update(ctx, de); err != nil {
			return fmt.Errorf("persist dag execution %d: %w", de.ID(), err)
		}
	}

	// 7. 投递：落库之后进行。投递失败只记日志不回滚——execution 已登记、
	//    节点已 Running，与 cron trigger 的"登记与投递分离"同一兜底逻辑
	for _, msg := range msgs {
		if err := e.pusher.Push(ctx, msg); err != nil {
			zap.L().Error("投递 DAG 节点消息失败（execution 已登记，可人工重投）",
				zap.Int64("dag_execution_id", de.ID()),
				zap.Int64("execution_id", msg.ExecID),
				zap.String("node_id", msg.DagNodeID), zap.Error(err))
		}
	}

	if finalized {
		zap.L().Info("DAG 运行收尾",
			zap.Int64("dag_execution_id", de.ID()),
			zap.Int64("dag_id", de.DagID()),
			zap.String("status", de.Status().String()),
			zap.Int64("duration_ms", de.DurationMS()))
	}
	return nil
}

// syncFinishedNodes 把 Running 节点绑定的 execution 终态同步到节点上。
// 一次 IN 查询批量取回本轮全部相关 execution（替代逐节点 Get 的 N+1）。
// 只认终态：Success → 节点成功；FailedFinal（重试耗尽）→ 节点失败。
// Pending/Running/Failed/Retrying 都还是"在路上"，节点保持 Running
func (e *Engine) syncFinishedNodes(ctx context.Context, de *dagdomain.DagExecution,
	now time.Time) (bool, error) {
	running := make([]*dagdomain.NodeExecution, 0)
	execIDs := make([]int64, 0)
	for _, ne := range de.NodeExecutions() {
		if ne.Status() == dagdomain.DagNodeStatusRunning && ne.ExecID() > 0 {
			running = append(running, ne)
			execIDs = append(execIDs, ne.ExecID())
		}
	}
	if len(running) == 0 {
		return false, nil
	}
	execs, err := e.execReg.ListByIDs(ctx, execIDs)
	if err != nil {
		return false, fmt.Errorf("load executions for dag %d: %w", de.ID(), err)
	}
	byID := make(map[int64]*execution.Execution, len(execs))
	for _, exec := range execs {
		byID[exec.ID()] = exec
	}

	changed := false
	for _, ne := range running {
		exec, ok := byID[ne.ExecID()]
		if !ok {
			// 登记过的 execution 不该查不到：真发生时保留现场交人工，
			// 不静默跳过（否则节点会永远 Running）
			return changed, fmt.Errorf("execution %d for node %q not found", ne.ExecID(), ne.NodeID())
		}
		switch exec.Status() {
		case execution.StatusSuccess:
			if err := ne.FinishSuccess(exec.Result(), now); err != nil {
				return changed, fmt.Errorf("finish node %q: %w", ne.NodeID(), err)
			}
			changed = true
		case execution.StatusFailedFinal:
			if err := ne.FinishFailed(now); err != nil {
				return changed, fmt.Errorf("fail node %q: %w", ne.NodeID(), err)
			}
			zap.L().Warn("DAG 节点重试耗尽失败",
				zap.Int64("dag_execution_id", de.ID()),
				zap.Int64("execution_id", ne.ExecID()),
				zap.String("node_id", ne.NodeID()))
			changed = true
		}
	}
	return changed, nil
}

// dispatchReady 派发所有就绪节点：登记 execution → 节点 Bind → 构建消息。
// 返回待投递消息与是否有派发发生。
//
// 顺序说明：先登记后落库，两步之间存在极小的崩溃窗口（execution 已建、
// 节点仍 Waiting）——下轮会对同一节点再派发一次，产生重复 execution；
// 反过来（先落库后登记）崩溃会留下 Running 无 execution 的死节点，
// 只能人工介入。两害取轻：重复可被人工清理消化，死节点不能
func (e *Engine) dispatchReady(ctx context.Context, de *dagdomain.DagExecution,
	now time.Time) ([]*coordination.ExecutionMessage, bool, error) {
	ready := de.ReadyNodes()
	if len(ready) == 0 {
		return nil, false, nil
	}

	// 跨库不能 join，按 taskID 批量取快照（job_task 库）
	taskIDs := make([]int64, 0, len(ready))
	for _, n := range ready {
		taskIDs = append(taskIDs, n.TaskID())
	}
	tasks, err := e.tasks.ListByIDs(ctx, taskIDs)
	if err != nil {
		return nil, false, fmt.Errorf("load task snapshots for dag %d: %w", de.ID(), err)
	}
	byID := make(map[int64]*task.Task, len(tasks))
	for _, t := range tasks {
		byID[t.ID()] = t
	}

	msgs := make([]*coordination.ExecutionMessage, 0, len(ready))
	dispatched := false
	for _, n := range ready {
		t, ok := byID[n.TaskID()]
		if !ok {
			// 任务被删/暂不可见：节点保持 Waiting，下轮重试（任务恢复后继续跑）
			zap.L().Warn("取不到任务快照，DAG 节点保持等待",
				zap.Int64("dag_execution_id", de.ID()),
				zap.String("node_id", n.NodeID()),
				zap.Int64("task_id", n.TaskID()))
			continue
		}
		// 数据流装配：所有上游节点的输出按 node_id 收进本节点的 input，
		// 落库 input_context 并随消息透传给 executor。
		// 上游没有产出（output 为空）就不进集合，消费端拿到的 key 是实打实跑过的
		inputs := make(map[string]json.RawMessage)
		for _, e := range de.Def().Incoming(n.NodeID()) {
			if up := de.Node(e.FromNodeID()); up != nil && len(up.Output()) > 0 {
				inputs[e.FromNodeID()] = up.Output()
			}
		}
		exec, err := execution.New(n.TaskID(), 0, t.RetryPolicy().MaxRetry(),
			fmt.Sprintf("dag-%d-%s", de.ID(), n.NodeID()), now)
		if err != nil {
			return nil, dispatched, fmt.Errorf("new execution for node %q: %w", n.NodeID(), err)
		}
		if err := e.execReg.Create(ctx, exec); err != nil {
			return nil, dispatched, fmt.Errorf("register execution for node %q: %w", n.NodeID(), err)
		}
		ne := de.Node(n.NodeID())
		if len(inputs) > 0 {
			if data, err := json.Marshal(inputs); err == nil {
				if err := ne.SetInput(data); err != nil {
					return nil, dispatched, fmt.Errorf("set input for node %q: %w", n.NodeID(), err)
				}
			} else {
				return nil, dispatched, fmt.Errorf("marshal inputs for node %q: %w", n.NodeID(), err)
			}
		}
		if err := ne.Bind(exec, now); err != nil {
			return nil, dispatched, fmt.Errorf("bind node %q: %w", n.NodeID(), err)
		}
		msgs = append(msgs, buildNodeMessage(de, n.NodeID(), exec, t, inputs))
		dispatched = true
	}
	return msgs, dispatched, nil
}

// buildNodeMessage 节点消息：与 scheduler.BuildExecutionMessage 同构，
// 差异仅在于带 DagExecID/DagNodeID/上游输出、不带 ScheduleID。
// 单独实现而不是 import scheduler 包：引擎与引擎之间不做纵向依赖
func buildNodeMessage(de *dagdomain.DagExecution, nodeID string,
	exec *execution.Execution, t *task.Task,
	inputs map[string]json.RawMessage) *coordination.ExecutionMessage {
	cfg, _ := task.MarshalConfig(t.Config())
	return &coordination.ExecutionMessage{
		ExecID:     exec.ID(),
		TaskID:     exec.TaskID(),
		DagExecID:  de.ID(),
		DagNodeID:  nodeID,
		TaskType:   int32(t.Type()),
		MaxRetry:   int32(t.RetryPolicy().MaxRetry()),
		Config:     cfg,
		Priority:   t.Priority(),
		TimeoutSec: int32(t.Timeout().Seconds()),
		EnqueuedAt: time.Now(),
		TraceID:    exec.TraceID(),
		NodeInputs: inputs,
	}
}
