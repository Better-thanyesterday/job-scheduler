package schedule

import (
	"fmt"
	"job-scheduler/app/internal/domain/task"
	"time"
)

type Schedule struct {
	id            int64
	taskID        int64
	targetType    TargetType // 触发目标：1-任务 2-DAG
	dagID         int64      // target=DAG 时有效；taskID 与 dagID 互斥
	name          string
	cron          task.Cron
	typ           ScheduleType
	nextRun       time.Time
	nextRunAnchor time.Time // 装载时的 next_run：repo CAS 锚点，随 SyncAnchor 推进
	lastRun       time.Time
	misfire       MisfirePolicy
	timezone      string
	status        Status
	createdBy     int64
	createdAt     time.Time
	updatedAt     time.Time
}

// 初始 nextRun 不在这里算：创建后由调用方（api）调 InitNextRun 落库，
// engine 装载时也会对 nextRun 为零的 Running 调度兜底计算。
// 触发目标二选一：任务（taskID>0）或 DAG（dagID>0），都填或都不填均拒绝
func New(id, taskID, dagID int64, name string, cron task.Cron, typ ScheduleType,
	misfire MisfirePolicy, timezone string, createdBy int64) (*Schedule, error) {

	if id <= 0 {
		return nil, fmt.Errorf("schedule: invalid id %d", id)
	}
	var target TargetType
	switch {
	case taskID > 0 && dagID == 0:
		target = TargetTask
	case dagID > 0 && taskID == 0:
		target = TargetDAG
	default:
		return nil, fmt.Errorf("schedule: task_id 与 dag_id 必须二选一（task=%d dag=%d）", taskID, dagID)
	}
	if name == "" {
		return nil, fmt.Errorf("schedule: name is empty")
	}
	if !typ.Valid() {
		return nil, fmt.Errorf("schedule: invalid schedule_type %d", typ)
	}
	if !misfire.Valid() {
		return nil, fmt.Errorf("schedule: invalid misfire_policy %d", misfire)
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return nil, fmt.Errorf("schedule: invalid timezone %q: %w", timezone, err)
	}
	return &Schedule{
		id: id, taskID: taskID, targetType: target, dagID: dagID, name: name, cron: cron, typ: typ,
		misfire: misfire, timezone: timezone,
		status: StatusRunning, createdBy: createdBy,
	}, nil
}

// InitNextRun 计算初始触发点（首次创建/装载时用）。
// 手动/API 触发类型没有 cron，返回错误，调用方按无定期触发处理
func (s *Schedule) InitNextRun(now time.Time) error {
	next, err := s.cron.Next(now)
	if err != nil {
		return fmt.Errorf("schedule: init next run id=%d: %w", s.id, err)
	}
	s.nextRun = next
	return nil
}

// Rebuild 从持久化数据重建（infra convert 唯一入口，与 New 的区别同 task.Rebuild）
func Rebuild(id, taskID int64, name string, cron task.Cron, typ ScheduleType,
	nextRun, lastRun time.Time, misfire MisfirePolicy,
	timezone string, status Status, createdBy int64,
	createdAt, updatedAt time.Time) (*Schedule, error) {

	if !status.Valid() {
		return nil, fmt.Errorf("schedule: rebuild id=%d invalid status %d", id, status)
	}
	if !misfire.Valid() {
		return nil, fmt.Errorf("schedule: rebuild id=%d invalid misfire_policy %d", id, misfire)
	}
	// 不在这里把"nextRun 已过期"改写为 Expired：持久化状态必须原样回填，
	//   过期与否是调度侧的动态判定（compensator 按 nextRun 补发），
	//   读库时改状态会让 api 展示与 engine 装载都失真
	return &Schedule{id: id, taskID: taskID, name: name, cron: cron, typ: typ,
		misfire: misfire, timezone: timezone, nextRun: nextRun, nextRunAnchor: nextRun, lastRun: lastRun,
		status: status, createdBy: createdBy, createdAt: createdAt, updatedAt: updatedAt}, nil
}

func (s *Schedule) ID() int64              { return s.id }
func (s *Schedule) TaskID() int64          { return s.taskID }
func (s *Schedule) Name() string           { return s.name }
func (s *Schedule) Cron() task.Cron        { return s.cron } // 值对象，天然只读
func (s *Schedule) Misfire() MisfirePolicy { return s.misfire }
func (s *Schedule) Type() int32            { return int32(s.typ) }
func (s *Schedule) Status() Status         { return s.status }
func (s *Schedule) NextRun() time.Time     { return s.nextRun }
func (s *Schedule) LastRun() time.Time     { return s.lastRun }
func (s *Schedule) TimeOne() string        { return s.timezone }
func (s *Schedule) CreatedBy() int64       { return s.createdBy }
func (s *Schedule) CreatedAt() time.Time   { return s.createdAt }
func (s *Schedule) UpdatedAt() time.Time   { return s.updatedAt }

func (s *Schedule) Pause() error {
	if s.status != StatusRunning {
		return fmt.Errorf("%w: pause id=%d status=%d", ErrWrongStatus, s.id, s.status)
	}
	s.status = StatusPaused
	return nil
}

func (s *Schedule) Delete() error {
	s.status = StatusPaused
	return nil
}

// Resume 恢复：重算 next_run 为未来最近的触发点，避免恢复即触发历史时刻
func (s *Schedule) Resume(now time.Time) error {
	if s.status != StatusPaused {
		return fmt.Errorf("%w: resume id=%d status=%d", ErrWrongStatus, s.id, s.status)
	}
	next, err := s.cron.Next(now)
	if err != nil {
		return fmt.Errorf("schedule: resume id=%d: %w", s.id, err)
	}
	s.status = StatusRunning
	s.nextRun = next
	return nil
}

// Advance 触发完成后推进：更新 last_run 并计算下一次（engine 主循环的唯一推进入口）
func (s *Schedule) Advance(now time.Time) error {
	next, err := s.cron.Next(now)
	if err != nil {
		return fmt.Errorf("schedule: advance id=%d: %w", s.id, err)
	}
	s.lastRun = now
	s.nextRun = next
	return nil
}

// NextRunAnchor 实体装载时的 next_run——UpdateNextRun 乐观锁的 CAS 锚点（repo 专用）。
// Advance/Resume 会改写 nextRun，但锚点保持装载值不变，
// repo 才能用它对上"我读到的那一行"（updated_at 是 DATETIME(3) 毫秒截断，
// 纳秒值落库后读不回来，不能当锚点；next_run 是 cron 整秒值，可无损往返）
func (s *Schedule) NextRunAnchor() time.Time { return s.nextRunAnchor }

// SyncAnchor CAS 写成功后由 repo 调用：锚点推进到当前 next_run。
// engine 堆里复用的实体由此持有"最新一版"，下一轮触发才能继续 CAS 命中
func (s *Schedule) SyncAnchor() { s.nextRunAnchor = s.nextRun }

// MisfirePlan 错过补偿决策：
// 输入上次运行时间与当前时间，按策略返回应补发的时间点集合。
// skip → 空 slice；merge → 只含一个合并点；fire_all → 全部错过点（截断上限防雪崩）
// 调用方（compensator）只对"nextRun 已过期"的 Running 调度调用本方法，
// 状态判断留在调度侧，这里只按时间窗口做纯计算。
// lastRun 为零值（从未跑过，DB 里是 NULL）→ 返回空：没有历史就没有"错过的点"，
// 从公元 1 年开始枚举会产出远古 planned → next_run 越界写库报错 / 连环补触发。
// 从未跑过的调度由调用方补触发一次 nextRun 本身（首个合法点），不走本方法
func (s *Schedule) MisfirePlan(lastRun, now time.Time, maxPoints int) []time.Time {
	if lastRun.IsZero() || !now.After(lastRun) {
		return nil
	}
	var missed []time.Time
	for t, _ := s.cron.Next(lastRun); t.Before(now); t, _ = s.cron.Next(t) {
		missed = append(missed, t)
		if len(missed) >= maxPoints { // 防止误配 "*/1秒" 表达式导致补发风暴
			break
		}
	}
	switch s.misfire {
	case MisfireSkip:
		return nil
	case MisfireMerge:
		if len(missed) == 0 {
			return nil
		}
		return []time.Time{now} // 合并为一次，立即执行
	default: // MisfireFireAll
		return missed
	}
}
