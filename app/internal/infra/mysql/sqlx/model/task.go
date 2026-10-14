package model

import (
	"database/sql"
	"time"
)

// Task 对应数据库表 tasks
// 注意：表名、主键、默认值、索引等由 DDL 定义，sqlx 不参与建模
type Task struct {
	ID          int64        `db:"id" json:"id"`                     // 任务ID（雪花算法）
	Name        string       `db:"name" json:"name"`                 // 任务名称
	TaskType    int32        `db:"task_type" json:"task_type"`       // 任务类型：1-HTTP 2-脚本 3-消息 4-SQL 5-子工作流
	Config      string       `db:"config" json:"config"`             // 任务配置体
	Priority    int32        `db:"priority" json:"priority"`         // 优先级 1-10
	Timeout     int32        `db:"timeout" json:"timeout"`           // 超时时间（秒）
	RetryPolicy string       `db:"retry_policy" json:"retry_policy"` // 重试策略（可空）
	Status      int32        `db:"status" json:"status"`             // 状态：1-启用 2-禁用 3-删除
	Description string       `db:"description" json:"description"`   // 任务描述（可空）
	CreatedBy   int64        `db:"created_by" json:"created_by"`     // 创建者用户ID
	CreatedAt   time.Time    `db:"created_at" json:"created_at"`     // 创建时间
	UpdatedAt   time.Time    `db:"updated_at" json:"updated_at"`     // 更新时间
	DeletedAt   sql.NullTime `db:"deleted_at" json:"deleted_at"`     // 软删除时间（NULL 表示未删除）
}
