package dag

import (
	"context"
	"errors"
	"fmt"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	dagdomain "job-scheduler/app/internal/domain/dag"
	"job-scheduler/app/pkg/metrics"

	"go.uber.org/zap"
)

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
// 终态判定只认 DB：唤醒消息/内存实体的成败都不是事实源。failed 结果
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
