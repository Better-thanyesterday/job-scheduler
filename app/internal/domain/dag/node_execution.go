package dag

import (
	"encoding/json"
	"fmt"
	"job-scheduler/app/internal/domain/execution"
	"time"
)

type NodeExecution struct {
	nodeID    string
	status    DagNodeStatus        // Waiting / Running / Success / Failed / Skipped
	execID    int64                // 关联的 executions.id;Waiting/Skipped 为 0,落库为 NULL
	exec      *execution.Execution // 跳过的节点为 nil
	input     json.RawMessage      // 上游节点的输出集合(引擎派发前装配),落库 input_context
	output    json.RawMessage      // 结果快照(与 exec.Result 同源),落库 output_context
	startTime time.Time
	endTime   time.Time
}

// ---------- NodeExecution 侧行为 ----------

// NewNodeExec 组装节点运行记录(infra 从持久化重建的唯一入口)。
// input 为重建入口参数:引擎派发时经 SetInput 写入,与 output 一样可持久往返
func NewNodeExec(nodeID string, status DagNodeStatus, execID int64,
	input, output json.RawMessage, start, end time.Time) (*NodeExecution, error) {
	if !status.Valid() {
		return nil, fmt.Errorf("dag: invalid node status %d", status)
	}
	if execID < 0 {
		return nil, fmt.Errorf("dag: node %q invalid execution_id %d", nodeID, execID)
	}
	if status == DagNodeStatusSkipped && execID != 0 {
		return nil, fmt.Errorf("dag: node %q skipped but carries execution_id %d", nodeID, execID)
	}
	return &NodeExecution{
		nodeID: nodeID, status: status, execID: execID,
		input: input, output: output,
		startTime: start, endTime: end,
	}, nil
}

func (ne *NodeExecution) NodeID() string          { return ne.nodeID }
func (ne *NodeExecution) Status() DagNodeStatus   { return ne.status }
func (ne *NodeExecution) ExecID() int64           { return ne.execID }
func (ne *NodeExecution) Input() json.RawMessage  { return ne.input }
func (ne *NodeExecution) Output() json.RawMessage { return ne.output }
func (ne *NodeExecution) StartTime() time.Time    { return ne.startTime }
func (ne *NodeExecution) EndTime() time.Time      { return ne.endTime }

// SetInput 派发前装配上游输出(引擎专属动作,只允许 Waiting 阶段:
// 一旦 Running,消费方已经拿到快照,再改就前后不一致了)
func (ne *NodeExecution) SetInput(data json.RawMessage) error {
	if ne.status != DagNodeStatusWaiting {
		return fmt.Errorf("dag: node %q can only take input while Waiting, status %d",
			ne.nodeID, ne.status)
	}
	ne.input = data
	return nil
}

// Bind 登记 execution:节点从 Waiting 进入 Running。
// 引擎在 execution.New + execReg.Create 之后调用
func (ne *NodeExecution) Bind(e *execution.Execution, now time.Time) error {
	if e == nil {
		return fmt.Errorf("dag: bind nil execution to node %q", ne.nodeID)
	}
	if err := ne.status.Transition(DagNodeStatusRunning); err != nil {
		return fmt.Errorf("dag: bind node %q: %w", ne.nodeID, err)
	}
	ne.exec = e
	ne.execID = e.ID()
	ne.startTime = now
	return nil
}

// Skip 跳过:fail_fast 短路 / 上游失败级联跳过。
// 没有 execution 记录,exec_id 保持 0(表 dag_node_executions.execution_id 可空)
func (ne *NodeExecution) Skip(now time.Time) error {
	if err := ne.status.Transition(DagNodeStatusSkipped); err != nil {
		return fmt.Errorf("dag: skip node %q: %w", ne.nodeID, err)
	}
	ne.endTime = now
	return nil
}

// FinishSuccess 节点成功;output 是结果快照(与 exec.Result 同源)
func (ne *NodeExecution) FinishSuccess(output json.RawMessage, now time.Time) error {
	if err := ne.status.Transition(DagNodeStatusSuccess); err != nil {
		return fmt.Errorf("dag: finish node %q: %w", ne.nodeID, err)
	}
	ne.output = output
	ne.endTime = now
	return nil
}

// FinishFailed 节点失败。★节点级终态:引擎只在 execution 重试耗尽后才调它,
// 中途失败由 execution 的 planner 自己兜
func (ne *NodeExecution) FinishFailed(now time.Time) error {
	if err := ne.status.Transition(DagNodeStatusFailed); err != nil {
		return fmt.Errorf("dag: fail node %q: %w", ne.nodeID, err)
	}
	ne.endTime = now
	return nil
}
