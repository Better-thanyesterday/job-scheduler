package types

import (
	"fmt"
	"strconv"
	"strings"
)

// FlexInt64 兼容 JSON 里字符串形式的 int64。前端为防 JS 精度丢失
// （int64 超出 MAX_SAFE_INTEGER），会把 16 位以上的整数 ID 编码成字符串传回
type FlexInt64 int64

func (n *FlexInt64) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		*n = 0
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid int64 %q: %w", s, err)
	}
	*n = FlexInt64(v)
	return nil
}

// CreateScheduleReq 创建调度：把触发规则挂到任务或 DAG
type CreateScheduleReq struct {
	// 触发目标二选一：target_type=1（默认）配 task_id；=2 配 dag_id
	TargetType    int       `json:"target_type" binding:"omitempty,oneof=1 2"` // 1-任务 2-DAG，缺省 1
	TaskID        FlexInt64 `json:"task_id"`                                   // 关联任务ID（target_type=1 时必填）
	DagID         FlexInt64 `json:"dag_id"`                                    // 关联DAG ID（target_type=2 时必填）
	Name          string    `json:"name" binding:"required,max=128"`           // 调度名
	CronExpr      string    `json:"cron_expr" binding:"required,max=64"`       // 5 或 6 字段 Cron 表达式
	ScheduleType  int       `json:"schedule_type" binding:"omitempty,oneof=1 2 3"`  // 1-Cron 2-手动 3-API触发
	MisfirePolicy int       `json:"misfire_policy" binding:"omitempty,oneof=1 2 3"` // 1-立即补发 2-跳过 3-合并
	Timezone      string    `json:"timezone" binding:"omitempty,max=64"`            // 缺省 Asia/Shanghai
}

// ScheduleResponse 调度详情
type ScheduleResp struct {
	ID            int64  `json:"id"`
	TaskID        int64  `json:"task_id"`
	DagID         int64  `json:"dag_id,omitempty"`
	TargetType    int    `json:"target_type"` // 1-任务 2-DAG
	Name          string `json:"name"`
	CronExpr      string `json:"cron_expr"`
	ScheduleType  int    `json:"schedule_type"`
	NextRunTime   string `json:"next_run_time,omitempty"`
	LastRunTime   string `json:"last_run_time,omitempty"`
	Status        int    `json:"status"`         // 1-运行中 2-暂停 3-过期 4-删除
	MisfirePolicy int    `json:"misfire_policy"` // 1-立即补发 2-跳过 3-合并
	Timezone      string `json:"timezone"`
	CreatedBy     int64  `json:"created_by"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

type ListSchedulesResp struct {
	Total int64           `json:"total"`
	Items []*ScheduleResp `json:"items"`
}
