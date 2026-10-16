package logic

import (
	"context"
	"errors"
	"fmt"
	"time"

	"job-scheduler/app/internal/api/types"
	"job-scheduler/app/internal/domain/schedule"
	"job-scheduler/app/internal/domain/task"
	repo "job-scheduler/app/internal/infra/mysql/sqlx/repo"
	"job-scheduler/app/pkg/snowflake"
)

// ScheduleLogic 调度域的应用服务。
type ScheduleLogic struct {
	scheduleRepo   *repo.ScheduleRepo
	taskSourceRepo *repo.TaskSourceRepo
	dagRepo        *repo.DagRepo // target=DAG 的调度：校验 DAG 存在且启用
}

func NewScheduleLogic(scheduleRepo *repo.ScheduleRepo, taskSourceRepo *repo.TaskSourceRepo,
	dagRepo *repo.DagRepo) *ScheduleLogic {
	return &ScheduleLogic{
		scheduleRepo:   scheduleRepo,
		taskSourceRepo: taskSourceRepo,
		dagRepo:        dagRepo,
	}
}

// Create 创建调度：校验触发目标存在 → 构造实体 → Cron 类型算初始 nextRun → 落库
func (l *ScheduleLogic) Create(ctx context.Context, in *types.CreateScheduleReq, createdBy int64) (*types.ScheduleResp, error) {
	targetType := in.TargetType
	if targetType == 0 {
		targetType = 1 // 缺省：定时任务
	}
	switch targetType {
	case 1:
		// 任务目标：跨库校验任务存在（软删的任务 Get 查不到）。
		// 翻译成调度域自己的 ErrTaskNotFound，controller 才能映射到参数错误而不是服务繁忙
		if _, err := l.taskSourceRepo.Get(ctx, int64(in.TaskID)); err != nil {
			if errors.Is(err, task.ErrNotFound) {
				return nil, schedule.ErrTaskNotFound
			}
			return nil, err
		}
	case 2:
		// DAG 目标：校验 DAG 存在且启用
		d, err := l.dagRepo.Get(ctx, int64(in.DagID))
		if err != nil {
			return nil, fmt.Errorf("%w: dag_id=%d", schedule.ErrNotFound, in.DagID)
		}
		if !d.Active() {
			return nil, fmt.Errorf("%w: dag_id=%d 未启用", schedule.ErrNotFound, in.DagID)
		}
	default:
		return nil, fmt.Errorf("target_type 取值 1（任务）或 2（DAG）")
	}
	// 2. 默认值收口
	typ := schedule.ScheduleType(in.ScheduleType)
	if in.ScheduleType == 0 {
		typ = schedule.TypeCron
	}
	misfire := schedule.MisfirePolicy(in.MisfirePolicy)
	if in.MisfirePolicy == 0 {
		misfire = schedule.MisfireFireAll
	}
	timezone := in.Timezone
	if timezone == "" {
		timezone = "Asia/Shanghai"
	}
	// 3. cron 解析（构造即校验）；手动/API 触发没有 cron 表达式
	var cron task.Cron
	if typ == schedule.TypeCron {
		c, err := task.Parse(in.CronExpr)
		if err != nil {
			return nil, err
		}
		cron = c
	}
	// 4. 实体（按触发目标二选一；schedule.New 现在带 id 入参，雪花分配）
	var taskID, dagID int64
	if targetType == 1 {
		taskID = int64(in.TaskID)
	} else {
		dagID = int64(in.DagID)
	}
	s, err := schedule.New(snowflake.GenID(), taskID, dagID, in.Name, cron, typ, misfire, timezone, createdBy)
	if err != nil {
		return nil, err
	}
	// 5. Cron 类型算初始触发点
	if typ == schedule.TypeCron {
		if err := s.InitNextRun(time.Now()); err != nil {
			return nil, err
		}
	}
	// 6. 落库
	if err := l.scheduleRepo.Save(ctx, s); err != nil {
		return nil, err
	}
	return toScheduleResponse(s), nil
}

func (l *ScheduleLogic) Get(ctx context.Context, id int64) (*types.ScheduleResp, error) {
	s, err := l.scheduleRepo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return toScheduleResponse(s), nil
}

// List 返回运行中与暂停的调度（domain 只提供 ListActive 这一种列表语义）
func (l *ScheduleLogic) List(ctx context.Context) (*types.ListSchedulesResp, error) {
	list, err := l.scheduleRepo.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]*types.ScheduleResp, 0, len(list))
	for _, s := range list {
		items = append(items, toScheduleResponse(s))
	}
	return &types.ListSchedulesResp{
		Total: int64(len(items)),
		Items: items,
	}, nil
}

// Pause 暂停：先由实体把关（只有运行中可暂停），再走轻量状态迁移。
// SetStatus 的 CAS 只认 Running → Paused：engine 推进/另一实例并发启停抢先后返回冲突
func (l *ScheduleLogic) Pause(ctx context.Context, id int64) error {
	s, err := l.scheduleRepo.Get(ctx, id)
	if err != nil {
		return err
	}
	if err := s.Pause(); err != nil {
		return err
	}
	return l.scheduleRepo.SetStatus(ctx, id, []schedule.Status{schedule.StatusRunning}, schedule.StatusPaused)
}

// Resume 恢复：实体按 cron 重算 next_run（避免恢复即补发历史时刻）。
// 落库分两步：UpdateNextRun 只写时间列（status 归属 SetStatus 专管，
// 混写会用陈旧内存状态覆盖 DB）；再 SetStatus(Paused→Running) 提交状态迁移
func (l *ScheduleLogic) Resume(ctx context.Context, id int64) error {
	s, err := l.scheduleRepo.Get(ctx, id)
	if err != nil {
		return err
	}
	if err := s.Resume(time.Now()); err != nil {
		return err
	}
	if err := l.scheduleRepo.UpdateNextRun(ctx, s); err != nil {
		return err
	}
	return l.scheduleRepo.SetStatus(ctx, id, []schedule.Status{schedule.StatusPaused}, schedule.StatusRunning)
}

// Delete 删除：置为已删除状态，engine 的 ListActive 不再加载它。
// CAS 认 Running/Paused → Deleted（暂停后删除是合法操作流）：
// 行已是 Deleted 或被并发迁移时返回冲突。
// 不走实体的 s.Delete()——domain 里那个方法当前只是把状态置成 Paused（未完成的桩）
func (l *ScheduleLogic) Delete(ctx context.Context, id int64) error {
	if _, err := l.scheduleRepo.Get(ctx, id); err != nil {
		return err
	}
	return l.scheduleRepo.SetStatus(ctx, id,
		[]schedule.Status{schedule.StatusRunning, schedule.StatusPaused},
		schedule.StatusDeleted)
}

func toScheduleResponse(s *schedule.Schedule) *types.ScheduleResp {
	return &types.ScheduleResp{
		ID:            s.ID(),
		TaskID:        s.TaskID(),
		DagID:         s.DagID(),
		TargetType:    int(s.Target()),
		Name:          s.Name(),
		CronExpr:      s.Cron().String(),
		ScheduleType:  int(s.Type()),
		NextRunTime:   formatTime(s.NextRun()),
		LastRunTime:   formatTime(s.LastRun()),
		Status:        int(s.Status()),
		MisfirePolicy: int(s.Misfire()),
		Timezone:      s.TimeOne(),
		CreatedBy:     s.CreatedBy(),
		CreatedAt:     formatTime(s.CreatedAt()),
		UpdatedAt:     formatTime(s.UpdatedAt()),
	}
}

// formatTime 零值时间输出空串，交给 omitempty 处理
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// formatTimePtr 可空时间（如 next_retry_at 未安排重试时为 nil）
func formatTimePtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return formatTime(*t)
}
