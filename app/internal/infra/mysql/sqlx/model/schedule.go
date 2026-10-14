package model

import (
	"database/sql"
	"time"
)

// Schedule 对应 job_schedule.schedules。next_run_time/last_run_time 可空
// （手动/API 触发的计划没有运行时间），零值语义靠 Valid 区分
type Schedule struct {
	ID            int64        `db:"id" json:"id"`
	TaskID        int64        `db:"task_id" json:"task_id"`
	Name          string       `db:"name" json:"name"`
	CronExpr      string       `db:"cron_expr" json:"cron_expr"`
	ScheduleType  int32        `db:"schedule_type" json:"schedule_type"`
	NextRunTime   sql.NullTime `db:"next_run_time" json:"next_run_time"`
	LastRunTime   sql.NullTime `db:"last_run_time" json:"last_run_time"`
	Status        int32        `db:"status" json:"status"`
	MisfirePolicy int32        `db:"misfire_policy" json:"misfire_policy"`
	Timezone      string       `db:"timezone" json:"timezone"`
	CreatedBy     int64        `db:"created_by" json:"created_by"`
	CreatedAt     time.Time    `db:"created_at" json:"created_at"`
	UpdatedAt     time.Time    `db:"updated_at" json:"updated_at"`
	DeletedAt     sql.NullTime `db:"deleted_at" json:"deleted_at"`
}

// ScheduleHistory 对应 job_schedule.schedule_histories。execution_id 可空（尚未登记执行）
type ScheduleHistory struct {
	ID          int64     `db:"id" json:"id"`
	ScheduleID  int64     `db:"schedule_id" json:"schedule_id"`
	ExecutionID int64     `db:"execution_id" json:"execution_id"`
	TriggerTime time.Time `db:"trigger_time" json:"trigger_time"`
	PlannedTime time.Time `db:"planned_time" json:"planned_time"`
	LagMs       int32     `db:"lag_ms" json:"lag_ms"`
	Status      int32     `db:"status" json:"status"`
	TriggeredBy int32     `db:"triggered_by" json:"triggered_by"`
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
}
