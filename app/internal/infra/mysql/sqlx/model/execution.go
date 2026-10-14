package model

import (
	"database/sql"
	"time"
)

// Execution 对应 job_execution.executions。
// 可空列与 DDL 一一对应：schedule_id/dag_execution_id/dag_node_id 未关联时为 NULL，
// start/end/duration 终态前为 NULL，result 是 JSON 列空值写 NULL，version 可空
type Execution struct {
	ID             int64          `db:"id" json:"id"`
	TaskID         int64          `db:"task_id" json:"task_id"`
	ScheduleID     sql.NullInt64  `db:"schedule_id" json:"schedule_id"`
	DagExecutionID sql.NullInt64  `db:"dag_execution_id" json:"dag_execution_id"`
	DagNodeID      sql.NullString `db:"dag_node_id" json:"dag_node_id"`
	Status         int32          `db:"status" json:"status"`
	StartTime      sql.NullTime   `db:"start_time" json:"start_time"`
	EndTime        sql.NullTime   `db:"end_time" json:"end_time"`
	DurationMs     sql.NullInt32  `db:"duration_ms" json:"duration_ms"`
	Result         sql.NullString `db:"result" json:"result"`
	ErrorMsg       sql.NullString `db:"error_msg" json:"error_msg"`
	RetryCount     int32          `db:"retry_count" json:"retry_count"`
	MaxRetry       int32          `db:"max_retry" json:"max_retry"`
	WorkerID       sql.NullString `db:"worker_id" json:"worker_id"`
	TraceID        string         `db:"trace_id" json:"trace_id"`
	CreatedAt      time.Time      `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time      `db:"updated_at" json:"updated_at"`
	Version        sql.NullInt32  `db:"version" json:"version"`
	NextRetryAt    sql.NullTime   `db:"next_retry_at" json:"next_retry_at"`
}
