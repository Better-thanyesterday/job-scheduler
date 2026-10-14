package schedule

import (
	"context"
	"job-scheduler/app/internal/domain/task"
	"time"
)

// TaskSource 补充：engine 需要按 task_id 批量取任务配置构建 ExecutionMessage。
// job_schedule 与 job_task 分库，不能 SQL join，跨聚合配对在 engine 应用层完成
// （这也是为什么 Schedule 实体只存 TaskID，不嵌整个 Task）
type TaskSource interface {
	ListByIDs(ctx context.Context, ids []int64) ([]*task.Task, error)
	Get(ctx context.Context, id int64) (*task.Task, error)
}

type Repository interface {
	Save(ctx context.Context, s *Schedule) error // 新建（含 SetStatus 初值）
	Get(ctx context.Context, id int64) (*Schedule, error)
	ListActive(ctx context.Context) ([]*Schedule, error)                     // engine 启动构建时间轮
	UpdateNextRun(ctx context.Context, s *Schedule) error                    // Advance/Resume 后持久化；CAS：WHERE next_run=实体装载值（NextRunAnchor），被并发写方抢先返回 ErrConflict
	SetStatus(ctx context.Context, id int64, from []Status, to Status) error // api 启停/删除的轻量路径；CAS：仅列出的 from 状态可迁移，否则 ErrConflict（实体校验仍由 Pause/Resume 把关）
	RecordHistory(ctx context.Context, h *RunHistory) error                  // schedule_histories 写入
}

// RunHistory 调度历史（写入方仅 engine）
type RunHistory struct {
	ScheduleID  int64
	ExecutionID int64
	TriggerTime time.Time
	PlannedTime time.Time
	Status      int32
	LagMS       int64
	TriggeredBy TriggerSource // 1-Cron 2-手动 3-补偿
}
