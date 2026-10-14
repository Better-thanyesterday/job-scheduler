package types

import (
	"encoding/json"
	"time"
)

// 任务域：创建 / 更新 / 列表的请求与响应。
// 校验规则全部写在 binding tag 上——Gin 的 validator 只认 binding，
// 写在 validate tag 上的规则不会生效

// RetryPolicy 重试策略（嵌套体：非 nil 时 validator 自动递归校验字段）。
// interval 是 Go time.Duration（纳秒），min=1 只挡 0/负值；
// 秒→纳秒的换算由前端/调用方完成
type RetryPolicy struct {
	MaxRetry int           `json:"max_retry" binding:"omitempty,min=0,max=10"`
	Strategy int           `json:"strategy" binding:"omitempty,oneof=1 2"`
	Interval time.Duration `json:"interval" binding:"omitempty,min=1"`
}

type CreateTaskReq struct {
	Name        string          `json:"name" binding:"required,max=128"`
	TaskType    int32           `json:"task_type" binding:"required,oneof=1 2 3 4 5"`
	Config      json.RawMessage `json:"config" binding:"required"`
	Priority    int32           `json:"priority" binding:"omitempty,min=1,max=10"`  // 不传用领域默认值 5
	Timeout     int32           `json:"timeout" binding:"omitempty,min=1,max=3600"` // 不传用领域默认值 300
	RetryPolicy *RetryPolicy    `json:"retry_policy"`
	Description string          `json:"description" binding:"max=512"`
}

// UpdateTaskReq 按传入字段更新（只更新请求里给了值的字段）
type UpdateTaskReq struct {
	Name        string          `json:"name" binding:"max=128"`
	Config      json.RawMessage `json:"config"`
	Priority    int             `json:"priority" binding:"omitempty,min=1,max=10"`
	Timeout     int             `json:"timeout" binding:"omitempty,min=1,max=3600"`
	RetryPolicy *RetryPolicy    `json:"retry_policy"`
	Description string          `json:"description" binding:"max=512"`
	Status      *int            `json:"status" binding:"omitempty,oneof=1 2"`
}

// ListTasksReq 列表查询（GET，form 绑定）
type ListTasksReq struct {
	Page     int    `form:"page,default=1" binding:"min=1"`
	PageSize int    `form:"page_size,default=20" binding:"min=1,max=100"`
	TaskType *int   `form:"task_type" binding:"omitempty,oneof=1 2 3 4 5"`
	Status   *int   `form:"status" binding:"omitempty,oneof=1 2"`
	Keyword  string `form:"keyword" binding:"max=128"`
}

type TaskResp struct {
	ID          int64           `json:"id"`
	Name        string          `json:"name"`
	TaskType    int             `json:"task_type"`
	Config      json.RawMessage `json:"config"`
	Priority    int             `json:"priority"`
	Timeout     int             `json:"timeout"`
	RetryPolicy *RetryPolicy    `json:"retry_policy,omitempty"`
	Status      int             `json:"status"`
	Description string          `json:"description,omitempty"`
	CreatedAt   string          `json:"created_at"`
	UpdatedAt   string          `json:"updated_at"`
}

type ListTasksResp struct {
	// total 仅第一页返回精确值：COUNT 成本随创建者名下行数线性增长，后续页不再计算，
	// 前端用 has_more 翻页即可
	Total   int64       `json:"total,omitempty"`
	HasMore bool        `json:"has_more"`
	Items   []*TaskResp `json:"items"`
}
