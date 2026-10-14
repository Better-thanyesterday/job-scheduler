package dag

import "fmt"

type Edge struct {
	fromNodeID string
	toNodeID   string
	edgeType   EdgeStatus // Success / Failure / Always
}

func NewEdge(fromNodeID, toNodeID string, edgeStatus EdgeStatus) (*Edge, error) {
	if fromNodeID == "" || toNodeID == "" {
		return nil, fmt.Errorf("dag: edge endpoint is empty")
	}
	if fromNodeID == toNodeID {
		return nil, fmt.Errorf("dag: self loop on node %q", fromNodeID)
	}
	if !edgeStatus.Valid() {
		return nil, fmt.Errorf("dag: invalid edge type %d", edgeStatus)
	}
	return &Edge{fromNodeID: fromNodeID, toNodeID: toNodeID, edgeType: edgeStatus}, nil
}

func (e *Edge) FromNodeID() string   { return e.fromNodeID }
func (e *Edge) ToNodeID() string     { return e.toNodeID }
func (e *Edge) EdgeType() EdgeStatus { return e.edgeType }

// satisfied 上游处于 up 状态时,这条边是否已满足
func (e *Edge) satisfied(up DagNodeStatus) bool {
	switch e.edgeType {
	case EdgeStatusSuccess:
		return up == DagNodeStatusSuccess
	case EdgeStatusFailure:
		return up == DagNodeStatusFailed
	case EdgeStatusAlways:
		return up.IsTerminal()
	default:
		return false
	}
}

// unsatisfiable 上游处于 up 状态时,这条边是否永远无法满足(节点级终态才算:
// Waiting/Running 的上游还可能翻盘)
func (e *Edge) unsatisfiable(up DagNodeStatus) bool {
	switch e.edgeType {
	case EdgeStatusSuccess:
		return up == DagNodeStatusSkipped || up == DagNodeStatusFailed
	case EdgeStatusFailure:
		return up == DagNodeStatusSuccess || up == DagNodeStatusSkipped
	default: // Always:上游必然到达终态,不会卡死
		return false
	}
}
