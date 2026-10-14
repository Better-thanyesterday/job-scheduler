package dag

import (
	"fmt"
)

type DagNode struct {
	nodeID string // DAG 内唯一,edges 连线靠它
	taskID int64  // 指向普通任务
	name   string
}

func NewNode(nodeID string, taskID int64, name string) (*DagNode, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("dag: node_id is empty")
	}
	if taskID <= 0 {
		return nil, fmt.Errorf("dag: node %q invalid task_id %d", nodeID, taskID)
	}
	return &DagNode{nodeID: nodeID, taskID: taskID, name: name}, nil
}

func (n *DagNode) NodeID() string { return n.nodeID }
func (n *DagNode) TaskID() int64  { return n.taskID }
func (n *DagNode) Name() string   { return n.name }
