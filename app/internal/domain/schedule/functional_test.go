package schedule_test

import (
	"testing"
	"time"

	"job-scheduler/app/internal/domain/schedule"
	"job-scheduler/app/internal/domain/task"
)

// 功能验证：调度状态机（暂停/恢复/推进）与 next_run 语义
func TestScheduleStateMachine(t *testing.T) {
	cron, err := task.Parse("0 30 9 * * *") // 每天 09:30:00
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.Local) // 周日 10:00

	s, err := schedule.New(1, 100, "daily-930", cron,
		schedule.TypeCron, schedule.MisfireSkip, "Asia/Shanghai", 42)
	if err != nil {
		t.Fatalf("创建调度失败: %v", err)
	}
	if s.Status() != schedule.StatusRunning {
		t.Fatalf("出生状态 = %v, 期望 Running", s.Status())
	}

	// 初始触发点：必须是未来最近的一个 09:30（明天）
	if err := s.InitNextRun(now); err != nil {
		t.Fatalf("InitNextRun: %v", err)
	}
	nr := s.NextRun()
	want := time.Date(2026, 9, 28, 9, 30, 0, 0, time.Local)
	if !nr.Equal(want) {
		t.Fatalf("初始 next_run = %v, 期望 %v", nr, want)
	}

	// 暂停 → 重复暂停被拒 → 恢复（恢复必须把 next_run 重算到未来，避免恢复即补触发历史时刻）
	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if s.Status() != schedule.StatusPaused {
		t.Fatalf("暂停后状态 = %v, 期望 Paused", s.Status())
	}
	if err := s.Pause(); err == nil {
		t.Fatal("Paused 状态重复 Pause 应当被拒绝")
	}
	if err := s.Resume(now); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if s.Status() != schedule.StatusRunning {
		t.Fatalf("恢复后状态 = %v, 期望 Running", s.Status())
	}
	if err := s.Resume(now); err == nil {
		t.Fatal("Running 状态重复 Resume 应当被拒绝")
	}
	if !s.NextRun().After(now) {
		t.Fatalf("Resume 后 next_run = %v, 必须在 now 之后", s.NextRun())
	}

	// Advance：模拟"该触发了"——以触发时刻推进，last_run 落账、next_run 严格前进
	fireAt := s.NextRun()
	if err := s.Advance(fireAt); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if !s.LastRun().Equal(fireAt) {
		t.Fatalf("last_run = %v, 期望 %v", s.LastRun(), fireAt)
	}
	if !s.NextRun().After(fireAt) {
		t.Fatalf("Advance 后 next_run = %v, 必须严格晚于触发时刻 %v", s.NextRun(), fireAt)
	}
	// 推进两次，触发点逐日前移且不回退
	prev := s.NextRun()
	if err := s.Advance(prev); err != nil {
		t.Fatal(err)
	}
	if !s.NextRun().After(prev) {
		t.Fatal("连续 Advance 必须持续推进")
	}
}
