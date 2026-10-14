package types

import "encoding/json"

// ListExecutionsQueryReq 执行历史分页查询：按任务维度翻页（执行域只读）
type ListExecutionsReq struct {
	TaskID int64 `form:"task_id" binding:"required"`
	Page   int   `form:"page,default=1" binding:"min=1"`
}

// ExecutionResponse 执行记录
type ExecutionResp struct {
	ID          int64           `json:"id"`
	TaskID      int64           `json:"task_id"`
	ScheduleID  int64           `json:"schedule_id,omitempty"`      // Cron 触发才有
	DagExecID   int64           `json:"dag_execution_id,omitempty"` // DAG 子任务才有
	DagNodeID   string          `json:"dag_node_id,omitempty"`
	Status      int             `json:"status"` // 1-Pending 2-Scheduled 3-Running 4-Success 5-Failed 6-Retrying 7-FailedFinal
	RetryCount  int             `json:"retry_count"`
	MaxRetry    int             `json:"max_retry"`
	NextRetryAt string          `json:"next_retry_at,omitempty"`
	Version     int64           `json:"version"` // 乐观锁版本，排障用
	DurationMS  int64           `json:"duration_ms,omitempty"`
	TraceID     string          `json:"trace_id,omitempty"`
	WorkerID    string          `json:"worker_id,omitempty"`
	StartTime   string          `json:"start_time,omitempty"`
	EndTime     string          `json:"end_time,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"` // worker 上报的结果快照
	ErrorMsg    string          `json:"error_msg,omitempty"`
	CreatedAt   string          `json:"created_at"`
}

type ListExecutionsResp struct {
	Total int              `json:"total"`
	Page  int              `json:"page"`
	Items []*ExecutionResp `json:"items"`
}

// ExecutionResultDownload 执行结果的下载形态：
// Redirect 非空 = 302 目标（15 分钟预签名 URL，直连对象存储）；
// Inline 非空 = 内联原文（直接作为响应体）
type ExecutionResultDownload struct {
	Redirect    string `json:"redirect,omitempty"`
	Inline      []byte `json:"inline,omitempty"`
	ContentType string `json:"-"`
}
