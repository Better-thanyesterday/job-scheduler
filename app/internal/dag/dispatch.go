package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	dagdomain "job-scheduler/app/internal/domain/dag"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/domain/task"

	"go.uber.org/zap"
)

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
		// 归属落库：executions.dag_execution_id/dag_node_id 是执行域反查 DAG 的关联键
		if err := exec.AttachDAG(de.ID(), n.NodeID()); err != nil {
			return nil, dispatched, fmt.Errorf("attach dag to execution for node %q: %w", n.NodeID(), err)
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
