package model

import (
	"database/sql"
	"time"
)

// DagExecution 对应 job_dag.dag_executions。
// start/end/duration 未终结时为 NULL，context 是 JSON 列空值写 NULL
type DagExecution struct {
	ID          int64          `db:"id" json:"id"`
	DagID       int64          `db:"dag_id" json:"dag_id"`
	Status      int32          `db:"status" json:"status"`
	StartTime   sql.NullTime   `db:"start_time" json:"start_time"`
	EndTime     sql.NullTime   `db:"end_time" json:"end_time"`
	DurationMs  sql.NullInt32  `db:"duration_ms" json:"duration_ms"`
	Context     sql.NullString `db:"context" json:"context"`
	TriggeredBy int32          `db:"triggered_by" json:"triggered_by"`
	CreatedAt   time.Time      `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time      `db:"updated_at" json:"updated_at"`
}

// DagNodeExecution 对应 job_dag.dag_node_executions。
// execution_id 未派发时为 NULL，input/output_context 是 JSON 列
type DagNodeExecution struct {
	ID             int64          `db:"id" json:"id"`
	DagExecutionID int64          `db:"dag_execution_id" json:"dag_execution_id"`
	NodeID         string         `db:"node_id" json:"node_id"`
	ExecutionID    sql.NullInt64  `db:"execution_id" json:"execution_id"`
	Status         int32          `db:"status" json:"status"`
	StartTime      sql.NullTime   `db:"start_time" json:"start_time"`
	EndTime        sql.NullTime   `db:"end_time" json:"end_time"`
	InputContext   sql.NullString `db:"input_context" json:"input_context"`
	OutputContext  sql.NullString `db:"output_context" json:"output_context"`
	CreatedAt      time.Time      `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time      `db:"updated_at" json:"updated_at"`
}
