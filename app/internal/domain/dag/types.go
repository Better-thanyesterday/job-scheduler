package dag

import "fmt"

// EdgeType 边类型（对应 dag_edges.edge_type，编号与 DB 保持一致）
type EdgeStatus int32

const (
	EdgeStatusSuccess EdgeStatus = 1 + iota // 上游成功才跑
	EdgeStatusFailure                       // 上游失败才跑（补偿/告警分支）
	EdgeStatusAlways                        // 上游到达终态就跑
)

func (e EdgeStatus) Valid() bool { return e >= EdgeStatusSuccess && e <= EdgeStatusAlways }

func (e EdgeStatus) String() string {
	switch e {
	case EdgeStatusSuccess:
		return "Success"
	case EdgeStatusFailure:
		return "Failure"
	case EdgeStatusAlways:
		return "Always"
	default:
		return "unknown"
	}
}

// TriggerType 触发来源（对应 dag_executions.triggered_by：1-Cron 2-手动 3-子工作流）
type TriggerType int32

const (
	TriggerCron TriggerType = 1 + iota
	TriggerManual
	TriggerSubWorkflow // 由 TaskTypeSubDAG 任务发起
)

func (t TriggerType) Valid() bool { return t >= TriggerCron && t <= TriggerSubWorkflow }

func (t TriggerType) String() string {
	switch t {
	case TriggerCron:
		return "Cron"
	case TriggerManual:
		return "Manual"
	case TriggerSubWorkflow:
		return "SubWorkflow"
	default:
		return "unknown"
	}
}

// DefStatus DAG 定义状态（对应 dags.status：1-启用 2-禁用 3-删除）
type DefStatus int32

const (
	DefEnabled DefStatus = 1 + iota
	DefDisabled
	DefDeleted
)

func (s DefStatus) Valid() bool { return s >= DefEnabled && s <= DefDeleted }

func (s DefStatus) String() string {
	switch s {
	case DefEnabled:
		return "Enabled"
	case DefDisabled:
		return "Disabled"
	case DefDeleted:
		return "Deleted"
	default:
		return "unknown"
	}
}

// DagExecStatus 一次 DAG 运行的整体状态（对应 dag_executions.status：1-运行中 2-成功 3-失败 4-部分成功）
type DagExecStatus int32

const (
	DagExecStatusRunning DagExecStatus = 1 + iota
	DagExecStatusSuccess
	DagExecStatusFailed
	DagExecStatusPartial
)

func (s DagExecStatus) Valid() bool { return s >= DagExecStatusRunning && s <= DagExecStatusPartial }

func (s DagExecStatus) String() string {
	switch s {
	case DagExecStatusRunning:
		return "Running"
	case DagExecStatusSuccess:
		return "Success"
	case DagExecStatusFailed:
		return "Failed"
	case DagExecStatusPartial:
		return "Partial"
	default:
		return "unknown"
	}
}

// IsTerminal 终态判定：引擎靠它判断整次运行是否收尾
func (s DagExecStatus) IsTerminal() bool { return s != DagExecStatusRunning }

var canDagExecTransition = map[DagExecStatus]map[DagExecStatus]bool{
	DagExecStatusRunning: {DagExecStatusSuccess: true, DagExecStatusFailed: true, DagExecStatusPartial: true},
	// 终态是黑洞：不可迁出
}

func (s *DagExecStatus) Transition(to DagExecStatus) error {
	if !canDagExecTransition[*s][to] {
		return fmt.Errorf("dag: illegal dag status transition %d -> %d", *s, to)
	}
	*s = to
	return nil
}

// DagNodeStatus 节点运行状态（对应 dag_node_executions.status：1-等待中 2-运行中 3-成功 4-失败 5-跳过）
type DagNodeStatus int32

const (
	DagNodeStatusWaiting DagNodeStatus = 1 + iota
	DagNodeStatusRunning
	DagNodeStatusSuccess
	DagNodeStatusFailed
	DagNodeStatusSkipped
)

func (s DagNodeStatus) Valid() bool { return s >= DagNodeStatusWaiting && s <= DagNodeStatusSkipped }

func (s DagNodeStatus) String() string {
	switch s {
	case DagNodeStatusWaiting:
		return "Waiting"
	case DagNodeStatusRunning:
		return "Running"
	case DagNodeStatusSuccess:
		return "Success"
	case DagNodeStatusFailed:
		return "Failed"
	case DagNodeStatusSkipped:
		return "Skipped"
	default:
		return "unknown"
	}
}

// IsTerminal 终态判定：Skipped 也是终态（被跳过的节点永远不会被重跑）
func (s DagNodeStatus) IsTerminal() bool {
	return s == DagNodeStatusSuccess || s == DagNodeStatusFailed || s == DagNodeStatusSkipped
}

var canNodeTransition = map[DagNodeStatus]map[DagNodeStatus]bool{
	DagNodeStatusWaiting: {DagNodeStatusRunning: true, DagNodeStatusSkipped: true},
	DagNodeStatusRunning: {DagNodeStatusSuccess: true, DagNodeStatusFailed: true},
}

func (s *DagNodeStatus) Transition(to DagNodeStatus) error {
	if !canNodeTransition[*s][to] {
		return fmt.Errorf("dag: illegal node status transition %d -> %d", *s, to)
	}
	*s = to
	return nil
}
