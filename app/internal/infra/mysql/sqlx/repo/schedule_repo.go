package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"job-scheduler/app/internal/domain/schedule"
	"job-scheduler/app/internal/domain/task"
	"job-scheduler/app/internal/infra/mysql"
	"job-scheduler/app/internal/infra/mysql/sqlx/model"
	"job-scheduler/app/pkg/snowflake"

	"github.com/jmoiron/sqlx"
)

var _ schedule.Repository = (*ScheduleRepo)(nil)

type ScheduleRepo struct {
	db *sqlx.DB
}

func NewScheduleRepo(scheduleDB mysql.ScheduleDB) *ScheduleRepo {
	return &ScheduleRepo{db: scheduleDB.DB}
}

// Save 新建计划。created_at/updated_at 交给 DB 默认值；next_run_time/last_run_time
// 只在非零时写入（手动/API 触发的计划没有运行时间，零值时间会被严格模式拒绝）
func (s *ScheduleRepo) Save(ctx context.Context, sched *schedule.Schedule) error {
	po, err := scheduleToPO(sched)
	if err != nil {
		return fmt.Errorf("计划转PO失败：%w", err)
	}
	cols := []string{"id", "task_id", "name", "cron_expr", "schedule_type",
		"status", "misfire_policy", "timezone", "created_by"}
	args := []interface{}{po.ID, po.TaskID, po.Name, po.CronExpr, po.ScheduleType,
		po.Status, po.MisfirePolicy, po.Timezone, po.CreatedBy}
	if po.NextRunTime.Valid {
		cols = append(cols, "next_run_time")
		args = append(args, po.NextRunTime.Time)
	}
	if po.LastRunTime.Valid {
		cols = append(cols, "last_run_time")
		args = append(args, po.LastRunTime.Time)
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",")
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO schedules (`+strings.Join(cols, ",")+`) VALUES (`+placeholders+`)`,
		args...); err != nil {
		return fmt.Errorf("创建计划失败：%w", err)
	}
	return nil
}

func (s *ScheduleRepo) Get(ctx context.Context, id int64) (*schedule.Schedule, error) {
	var po model.Schedule
	err := s.db.GetContext(ctx, &po,
		`SELECT * FROM schedules WHERE id = ? AND deleted_at IS NULL`, id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("%w: id=%d", schedule.ErrNotFound, id)
	case err != nil:
		return nil, fmt.Errorf("查询计划失败：%w", err)
	}
	entity, err := scheduleToEntity(&po)
	if err != nil {
		return nil, fmt.Errorf("重建计划实体失败：%w", err)
	}
	return entity, nil
}

func (s *ScheduleRepo) ListActive(ctx context.Context) ([]*schedule.Schedule, error) {
	var pos []*model.Schedule
	err := s.db.SelectContext(ctx, &pos,
		`SELECT * FROM schedules WHERE status IN (?, ?) AND deleted_at IS NULL`,
		1, 2)
	if err != nil {
		return nil, fmt.Errorf("获取已激活计划失败：%w", err)
	}
	list, err := schedulesToEntities(pos)
	if err != nil {
		return nil, fmt.Errorf("重建计划实体失败：%w", err)
	}
	return list, nil
}

// UpdateNextRun 乐观锁推进：WHERE next_run_time = 实体装载值（NextRunAnchor）。
// 语义是"认领当前这个触发点"——engine 触发（Advance）与 api 恢复（Resume）共用同一写入口，
// 任何一方抢先推进后，另一方拿旧锚点 CAS 必然 0 行 → ErrConflict，调用方放弃本次、下轮重读。
// 装载时 next_run 为空（从未初始化触发点）的行锚 IS NULL，同样可 CAS。
// 只更新实体管辖的列，不整行覆盖，避免把堆里陈旧的 name/cron 写回去
func (s *ScheduleRepo) UpdateNextRun(ctx context.Context, sched *schedule.Schedule) error {
	anchor := sched.NextRunAnchor()
	anchorCond := "next_run_time IS NULL"
	// ★ 只写时间列，不写 status：状态迁移归 SetStatus 专管（带 from→to CAS）。
	//   混写会用调用方的陈旧内存状态覆盖 DB——compensator 持有的是装载时快照，
	//   触发间隙里 api 的 Pause 会被静默改回 Running（真实事故路径）。
	// ★ args 顺序必须与占位符出现顺序一致：SET(updated_at[, next][, last])
	//   → WHERE(id[, anchor])
	args := []interface{}{time.Now()}
	sets := "updated_at = ?"
	// 手动/API 触发类型没有 next_run/last_run，零值时间会被严格模式拒绝，跳过
	if !sched.NextRun().IsZero() {
		sets += ", next_run_time = ?"
		args = append(args, sched.NextRun())
	}
	if !sched.LastRun().IsZero() {
		sets += ", last_run_time = ?"
		args = append(args, sched.LastRun())
	}
	args = append(args, sched.ID()) // WHERE id = ?
	if !anchor.IsZero() {
		anchorCond = "next_run_time = ?"
		args = append(args, anchor) // WHERE next_run_time = ?
	}

	info, err := s.db.ExecContext(ctx,
		`UPDATE schedules SET `+sets+
			` WHERE id = ? AND deleted_at IS NULL AND `+anchorCond,
		args...)
	if err != nil {
		return fmt.Errorf("更新计划失败：%w", err)
	}
	if n, err := info.RowsAffected(); err != nil {
		return fmt.Errorf("更新计划失败：%w", err)
	} else if n == 0 {
		return fmt.Errorf("%w: id=%d", schedule.ErrConflict, sched.ID())
	}
	sched.SyncAnchor()
	return nil
}

// SetStatus 状态轻量迁移（api Pause/Delete 用）：CAS 只认列出的 from 状态。
// RowsAffected=0 = 行已不在 from 状态（被并发启停/推进/删除）或已不存在 → ErrConflict；
// "不存在"由调用方先行的 Get 拦截，这里统一按冲突处理
func (s *ScheduleRepo) SetStatus(ctx context.Context, id int64, from []schedule.Status, to schedule.Status) error {
	fromInt := make([]int, 0, len(from))
	for _, st := range from {
		fromInt = append(fromInt, int(st))
	}
	inPlaceholders := strings.TrimSuffix(strings.Repeat("?,", len(fromInt)), ",")
	// ★ args 顺序必须与占位符出现顺序一致：SET(status, updated_at) → WHERE(id, status IN...)
	args := []interface{}{int32(to), time.Now(), id}
	for _, st := range fromInt {
		args = append(args, st)
	}

	info, err := s.db.ExecContext(ctx,
		`UPDATE schedules SET status = ?, updated_at = ?
		 WHERE id = ? AND deleted_at IS NULL AND status IN (`+inPlaceholders+`)`,
		args...)
	if err != nil {
		return fmt.Errorf("update schedule status: %w", err)
	}
	if n, err := info.RowsAffected(); err != nil {
		return fmt.Errorf("update schedule status: %w", err)
	} else if n == 0 {
		return fmt.Errorf("%w: id=%d", schedule.ErrConflict, id)
	}
	return nil
}

// RecordHistory 写调度历史。schedule_histories.id 无自增，写入方自分配
func (s *ScheduleRepo) RecordHistory(ctx context.Context, h *schedule.RunHistory) error {
	// schedule_histories.id 无自增，写入方自分配
	id := snowflake.GenID()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO schedule_histories
		 (id, schedule_id, execution_id, trigger_time, planned_time, lag_ms, status, triggered_by)
		 VALUES (?,?,?,?,?,?,?,?)`,
		id, h.ScheduleID, h.ExecutionID, h.TriggerTime, h.PlannedTime,
		int32(h.LagMS), h.Status, int32(h.TriggeredBy))
	if err != nil {
		return fmt.Errorf("记录调度历史失败:%w", err)
	}
	return nil
}

// ==================== 实体 ↔ PO ====================

func scheduleToPO(s *schedule.Schedule) (*model.Schedule, error) {
	return &model.Schedule{
		ID:            s.ID(),
		TaskID:        s.TaskID(),
		Name:          s.Name(),
		CronExpr:      s.Cron().String(),
		ScheduleType:  int32(s.Type()),
		NextRunTime:   sql.NullTime{Time: s.NextRun(), Valid: !s.NextRun().IsZero()},
		LastRunTime:   sql.NullTime{Time: s.LastRun(), Valid: !s.LastRun().IsZero()},
		Status:        int32(s.Status()),
		MisfirePolicy: int32(s.Misfire()),
		Timezone:      s.TimeOne(),
		CreatedBy:     s.CreatedBy(),
		CreatedAt:     s.CreatedAt(),
		UpdatedAt:     s.UpdatedAt(),
	}, nil
}

func scheduleToEntity(po *model.Schedule) (*schedule.Schedule, error) {
	// Rebuild：信任已持久化数据，不做业务校验
	cron, err := task.Parse(po.CronExpr)
	if err != nil {
		return nil, err
	}
	return schedule.Rebuild(
		po.ID,
		po.TaskID,
		po.Name,
		cron,
		schedule.ScheduleType(po.ScheduleType),
		po.NextRunTime.Time,
		po.LastRunTime.Time,
		schedule.MisfirePolicy(po.MisfirePolicy),
		po.Timezone,
		schedule.Status(po.Status),
		po.CreatedBy,
		po.CreatedAt,
		po.UpdatedAt,
	)
}

func schedulesToEntities(pos []*model.Schedule) ([]*schedule.Schedule, error) {
	schedules := make([]*schedule.Schedule, 0, len(pos))
	for _, po := range pos {
		s, err := scheduleToEntity(po)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, s)
	}
	return schedules, nil
}
