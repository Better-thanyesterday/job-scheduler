package dag

import (
	"encoding/json"
	"fmt"
	"job-scheduler/app/pkg/snowflake"
	"time"
)

// ---------- 运行态(对应 dag_executions + dag_node_executions)----------

type DagExecution struct {
	id          int64
	dagID       int64
	status      DagExecStatus // Running / Success / Failed / Partial
	triggeredBy TriggerType   // dag_executions.triggered_by:1-Cron 2-手动 3-子工作流
	ctx         json.RawMessage
	def         *Dag                      // 依赖判定 / 并行上限都查定义
	nodes       map[string]*NodeExecution // key = nodeID
	startTime   time.Time
	endTime     time.Time
	durationMS  int64
}

// NewExecution 开始一次 DAG 运行:所有节点出生即 Waiting
func NewExecution(def *Dag, ctx json.RawMessage, triggeredBy TriggerType,
	now time.Time) (*DagExecution, error) {
	if def == nil {
		return nil, fmt.Errorf("dag: nil definition")
	}
	if !triggeredBy.Valid() {
		return nil, fmt.Errorf("dag: invalid triggered_by %d", triggeredBy)
	}
	nodes := make(map[string]*NodeExecution, len(def.nodes))
	for _, n := range def.nodes {
		nodes[n.nodeID] = &NodeExecution{nodeID: n.nodeID, status: DagNodeStatusWaiting}
	}
	return &DagExecution{
		id:          snowflake.GenID(),
		dagID:       def.id,
		status:      DagExecStatusRunning,
		triggeredBy: triggeredBy,
		ctx:         ctx,
		def:         def,
		nodes:       nodes,
		startTime:   now,
	}, nil
}

// RebuildExecution 从持久化重建运行态。节点运行记录必须与定义一一对应:
// 多出的或缺失的都是脏数据,直接拒绝
func RebuildExecution(def *Dag, id int64, status DagExecStatus, triggeredBy TriggerType,
	ctx json.RawMessage, start, end time.Time, durationMS int64,
	nodeExecs []*NodeExecution) (*DagExecution, error) {
	if def == nil {
		return nil, fmt.Errorf("dag: nil definition")
	}
	if !status.Valid() {
		return nil, fmt.Errorf("dag: rebuild id=%d invalid status %d", id, status)
	}
	if !triggeredBy.Valid() {
		return nil, fmt.Errorf("dag: rebuild id=%d invalid triggered_by %d", id, triggeredBy)
	}
	nodes := make(map[string]*NodeExecution, len(nodeExecs))
	for _, ne := range nodeExecs {
		if _, dup := nodes[ne.nodeID]; dup {
			return nil, fmt.Errorf("dag: duplicate node execution %q", ne.nodeID)
		}
		if def.Node(ne.nodeID) == nil {
			return nil, fmt.Errorf("dag: node execution %q not in definition", ne.nodeID)
		}
		nodes[ne.nodeID] = ne
	}
	for _, n := range def.nodes {
		if _, ok := nodes[n.nodeID]; !ok {
			return nil, fmt.Errorf("dag: missing node execution %q", n.nodeID)
		}
	}
	return &DagExecution{
		id:          id,
		dagID:       def.id,
		status:      status,
		triggeredBy: triggeredBy,
		ctx:         ctx,
		def:         def,
		nodes:       nodes,
		startTime:   start,
		endTime:     end,
		durationMS:  durationMS,
	}, nil
}

// ---------- DagExecution 侧行为 ----------

// ReadyNodes 依赖已满足、可以派发的等待节点(按定义顺序)。
// 边语义:Success=上游成功 / Failure=上游失败 / Always=上游终态。
// 并行上限扣除已在运行的节点:limit<=0 视为不限
func (de *DagExecution) ReadyNodes() []*DagNode {
	ready := make([]*DagNode, 0)
	for _, n := range de.def.nodes {
		ne, ok := de.nodes[n.nodeID]
		if !ok || ne.status != DagNodeStatusWaiting {
			continue
		}
		if de.depsSatisfied(n.nodeID) {
			ready = append(ready, n)
		}
	}
	limit := int(de.def.parallelLimit)
	if limit <= 0 {
		return ready
	}
	available := limit - de.RunningCount()
	if available <= 0 {
		return nil
	}
	if available >= len(ready) {
		return ready
	}
	return ready[:available]
}

func (de *DagExecution) depsSatisfied(nodeID string) bool {
	for _, e := range de.def.Incoming(nodeID) {
		if !e.satisfied(de.nodes[e.fromNodeID].status) {
			return false
		}
	}
	return true
}

// RunningCount 运行中节点数(并行上限的占用量)
func (de *DagExecution) RunningCount() int {
	count := 0
	for _, ne := range de.nodes {
		if ne.status == DagNodeStatusRunning {
			count++
		}
	}
	return count
}

// StalledNodes 已经永远等不到上游的等待节点,引擎要把它们 Skip 掉(级联跳过)。
// Success 边撞上 Skipped/Failed 上游、Failure 边撞上 Success/Skipped 上游 → 卡死;
// Always 边的上游必然到达终态,不会卡死
func (de *DagExecution) StalledNodes() []*DagNode {
	stalled := make([]*DagNode, 0)
	for _, n := range de.def.nodes {
		ne, ok := de.nodes[n.nodeID]
		if !ok || ne.status != DagNodeStatusWaiting {
			continue
		}
		for _, e := range de.def.Incoming(n.nodeID) {
			if e.unsatisfiable(de.nodes[e.fromNodeID].status) {
				stalled = append(stalled, n)
				break
			}
		}
	}
	return stalled
}

// ShouldAbort fail_fast 判定:有节点失败且开了 fail_fast,
// 引擎应把所有未跑节点 Skip 掉、尽快收尾
func (de *DagExecution) ShouldAbort() bool {
	if !de.def.failFast {
		return false
	}
	for _, ne := range de.nodes {
		if ne.status == DagNodeStatusFailed {
			return true
		}
	}
	return false
}

// Finalize 收尾:所有节点到达终态后归档整体状态。
//
//	全部成功 → Success;存在失败 → Failed;无失败但有跳过 → Partial
//
// 返回 false 表示还有节点没跑完,本次不收尾
func (de *DagExecution) Finalize(now time.Time) (bool, error) {
	if de.status.IsTerminal() {
		return true, nil
	}
	for _, ne := range de.nodes {
		if !ne.status.IsTerminal() {
			return false, nil
		}
	}
	allSuccess, anyFailed := true, false
	for _, ne := range de.nodes {
		switch ne.status {
		case DagNodeStatusFailed:
			anyFailed = true
			allSuccess = false
		case DagNodeStatusSuccess:
		default: // Skipped
			allSuccess = false
		}
	}
	var to DagExecStatus
	switch {
	case allSuccess:
		to = DagExecStatusSuccess
	case anyFailed:
		to = DagExecStatusFailed
	default:
		to = DagExecStatusPartial
	}
	if err := de.status.Transition(to); err != nil {
		return false, err
	}
	de.endTime = now
	de.durationMS = now.Sub(de.startTime).Milliseconds()
	return true, nil
}

// Reap 死运行收割：长时间无任何推进的运行强制收场。
// 前置条件由引擎把关——只对"无 Running 节点且整体仍 Running"的运行调用
// （还有在飞执行的运行交给 execution 的 planner/reaper，轮不到这里）。
// Waiting 节点 → Skipped，Running 节点 → Failed；整体状态固定为 Failed
// （被收割本身就是失败，不走 Finalize 的 Partial 语义）。
// 返回是否有变更
func (de *DagExecution) Reap(now time.Time) (bool, error) {
	if de.status.IsTerminal() {
		return false, nil
	}
	changed := false
	for _, ne := range de.nodes {
		switch ne.status {
		case DagNodeStatusWaiting:
			if err := ne.Skip(now); err != nil {
				return changed, fmt.Errorf("dag: reap skip node %q: %w", ne.nodeID, err)
			}
			changed = true
		case DagNodeStatusRunning:
			if err := ne.FinishFailed(now); err != nil {
				return changed, fmt.Errorf("dag: reap fail node %q: %w", ne.nodeID, err)
			}
			changed = true
		}
	}
	if err := de.status.Transition(DagExecStatusFailed); err != nil {
		return changed, fmt.Errorf("dag: reap run %d: %w", de.id, err)
	}
	de.endTime = now
	de.durationMS = now.Sub(de.startTime).Milliseconds()
	return true, nil
}

func (de *DagExecution) ID() int64                { return de.id }
func (de *DagExecution) DagID() int64             { return de.dagID }
func (de *DagExecution) Status() DagExecStatus    { return de.status }
func (de *DagExecution) TriggeredBy() TriggerType { return de.triggeredBy }
func (de *DagExecution) Ctx() json.RawMessage     { return de.ctx }
func (de *DagExecution) Def() *Dag                { return de.def }
func (de *DagExecution) StartTime() time.Time     { return de.startTime }
func (de *DagExecution) EndTime() time.Time       { return de.endTime }
func (de *DagExecution) DurationMS() int64        { return de.durationMS }

// Running 是否仍在推进(引擎扫描条件)
func (de *DagExecution) Running() bool { return de.status == DagExecStatusRunning }

// Node 取节点运行记录;不存在返回 nil
func (de *DagExecution) Node(nodeID string) *NodeExecution { return de.nodes[nodeID] }

// NodeExecutions 全部节点运行记录(按定义顺序)
func (de *DagExecution) NodeExecutions() []*NodeExecution {
	list := make([]*NodeExecution, 0, len(de.nodes))
	for _, n := range de.def.nodes {
		list = append(list, de.nodes[n.nodeID])
	}
	return list
}
