package dag

import (
	"context"
	"fmt"
	"time"

	dagdomain "job-scheduler/app/internal/domain/dag"
	"job-scheduler/app/internal/domain/execution"

	"go.uber.org/zap"
)

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
