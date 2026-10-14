package model

import (
	"database/sql"
	"time"
)

// Dag 对应 job_dag.dags。description 可空
type Dag struct {
	ID            int64          `db:"id" json:"id"`
	Name          string         `db:"name" json:"name"`
	Description   sql.NullString `db:"description" json:"description"`
	ParallelLimit int32          `db:"parallel_limit" json:"parallel_limit"`
	FailFast      bool           `db:"fail_fast" json:"fail_fast"`
	Status        int32          `db:"status" json:"status"`
	CreatedBy     int64          `db:"created_by" json:"created_by"`
	CreatedAt     time.Time      `db:"created_at" json:"created_at"`
	UpdatedAt     time.Time      `db:"updated_at" json:"updated_at"`
	DeletedAt     sql.NullTime   `db:"deleted_at" json:"deleted_at"`
}

// DagNode 对应 job_dag.dag_nodes。task_id/config 可空（条件节点等未来类型可以没有任务）
type DagNode struct {
	ID        int64          `db:"id" json:"id"`
	DagID     int64          `db:"dag_id" json:"dag_id"`
	TaskID    sql.NullInt64  `db:"task_id" json:"task_id"`
	NodeID    string         `db:"node_id" json:"node_id"`
	NodeName  string         `db:"node_name" json:"node_name"`
	NodeType  int32          `db:"node_type" json:"node_type"`
	Config    sql.NullString `db:"config" json:"config"`
	SortOrder int32          `db:"sort_order" json:"sort_order"`
	CreatedAt time.Time      `db:"created_at" json:"created_at"`
}

// DagEdge 对应 job_dag.dag_edges
type DagEdge struct {
	ID         int64     `db:"id" json:"id"`
	DagID      int64     `db:"dag_id" json:"dag_id"`
	FromNodeID string    `db:"from_node_id" json:"from_node_id"`
	ToNodeID   string    `db:"to_node_id" json:"to_node_id"`
	EdgeType   int32     `db:"edge_type" json:"edge_type"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
}
