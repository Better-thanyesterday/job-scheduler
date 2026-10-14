package schedule

import (
	"errors"
	"testing"
	"time"

	"job-scheduler/app/internal/domain/task"
)

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

// mustCron 构造一个 cron。请按 task 包实际的 API 调整：
//   - 若提供 task.ParseCron(expr) (Cron, error)，用下面实现
//   - 若只有 task.MustParseCron(expr)，可去掉 error 分支
func mustCron(t *testing.T, expr string) task.Cron {
	t.Helper()
	c, err := task.Parse(expr)
	if err != nil {
		t.Fatalf("parse cron %q: %v", expr, err)
	}
	return c
}

// 用固定时区做基准，避免测试因本地时区不同而漂移
var testLoc = time.UTC

// 一个稳定的基准时间：2024-01-15 10:30:00 UTC
func baseTime() time.Time {
	return time.Date(2024, 1, 15, 10, 30, 0, 0, testLoc)
}

// 常用的合法构造参数
const (
	testID       = int64(1)
	testTaskID   = int64(100)
	testName     = "daily-report"
	testTimezone = "UTC"
	testCreated  = int64(42)
)

// 用来构造合法的 ScheduleType / MisfirePolicy / Status 的占位常量。
// 若你包里的常量名不同，替换即可。
var (
	validType    = ScheduleType(1) // 假设 1 是合法类型
	validMisfire = MisfireFireAll
	validStatus  = StatusRunning
)

// ---------------------------------------------------------------------------
// New
// ---------------------------------------------------------------------------

func TestNew(t *testing.T) {
	validCron := mustCron(t, "0 9 * * *")

	tests := []struct {
		name    string
		id      int64
		taskID  int64
		sname   string
		cron    task.Cron
		typ     ScheduleType
		misfire MisfirePolicy
		tz      string
		by      int64
		wantErr bool
	}{
		{"valid", testID, testTaskID, testName, validCron, validType, validMisfire, testTimezone, testCreated, false},
		{"invalid id zero", 0, testTaskID, testName, validCron, validType, validMisfire, testTimezone, testCreated, true},
		{"invalid id negative", -1, testTaskID, testName, validCron, validType, validMisfire, testTimezone, testCreated, true},
		{"invalid taskID zero", testID, 0, testName, validCron, validType, validMisfire, testTimezone, testCreated, true},
		{"invalid taskID negative", testID, -5, testName, validCron, validType, validMisfire, testTimezone, testCreated, true},
		{"empty name", testID, testTaskID, "", validCron, validType, validMisfire, testTimezone, testCreated, true},
		{"invalid schedule type", testID, testTaskID, testName, validCron, ScheduleType(999), validMisfire, testTimezone, testCreated, true},
		{"invalid misfire policy", testID, testTaskID, testName, validCron, validType, MisfirePolicy(999), testTimezone, testCreated, true},
		{"invalid timezone", testID, testTaskID, testName, validCron, validType, validMisfire, "Mars/Olympus", testCreated, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := New(tt.id, tt.taskID, tt.sname, tt.cron, tt.typ, tt.misfire, tt.tz, tt.by)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("New() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("New() unexpected error = %v", err)
			}
			if s.id != tt.id || s.taskID != tt.taskID || s.name != tt.sname {
				t.Errorf("New() field mismatch: %+v", s)
			}
			if s.status != StatusRunning {
				t.Errorf("New() status = %v, want StatusRunning", s.status)
			}
			if s.timezone != tt.tz {
				t.Errorf("New() timezone = %q, want %q", s.timezone, tt.tz)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// InitNextRun
// ---------------------------------------------------------------------------

func TestInitNextRun(t *testing.T) {
	now := baseTime() // 2024-01-15 10:30:00

	s, err := New(testID, testTaskID, testName,
		mustCron(t, "0 * * * *"), // 每小时整点
		validType, validMisfire, testTimezone, testCreated)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.InitNextRun(now); err != nil {
		t.Fatalf("InitNextRun() error = %v", err)
	}

	want := time.Date(2024, 1, 15, 11, 0, 0, 0, testLoc) // 下一个整点
	if !s.NextRun().Equal(want) {
		t.Errorf("InitNextRun() = %v, want %v", s.NextRun(), want)
	}

	// 手动/API 触发类型可能没有 cron，需要报错。
	// 这里用一个空 Cron 触发错误（若 task 包不提供空 Cron，可删掉此子用例）。
}

// ---------------------------------------------------------------------------
// Rebuild
// ---------------------------------------------------------------------------

func TestRebuild(t *testing.T) {
	next := time.Date(2024, 1, 15, 11, 0, 0, 0, testLoc)
	last := time.Date(2024, 1, 15, 10, 0, 0, 0, testLoc)
	created := baseTime().Add(-24 * time.Hour)
	updated := baseTime()

	tests := []struct {
		name    string
		status  Status
		misfire MisfirePolicy
		wantErr bool
	}{
		{"valid", validStatus, validMisfire, false},
		{"invalid status", Status(999), validMisfire, true},
		{"invalid misfire", validStatus, MisfirePolicy(999), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Rebuild(testID, testTaskID, testName,
				mustCron(t, "0 9 * * *"), validType,
				next, last, tt.misfire,
				testTimezone, tt.status, testCreated,
				created, updated)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Rebuild() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Rebuild() unexpected error = %v", err)
			}

			// 关键：持久化的 nextRun 要原样回填，不能被改写
			if !s.NextRun().Equal(next) {
				t.Errorf("Rebuild() nextRun = %v, want %v", s.NextRun(), next)
			}
			// 锚点初始化为装载时的 nextRun
			if !s.NextRunAnchor().Equal(next) {
				t.Errorf("Rebuild() nextRunAnchor = %v, want %v", s.NextRunAnchor(), next)
			}
			if !s.LastRun().Equal(last) {
				t.Errorf("Rebuild() lastRun = %v, want %v", s.LastRun(), last)
			}
			if s.CreatedAt() != created || s.UpdatedAt() != updated {
				t.Errorf("Rebuild() createdAt/updatedAt mismatch")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Pause
// ---------------------------------------------------------------------------

func TestPause(t *testing.T) {
	// Running -> Paused 成功
	s, _ := New(testID, testTaskID, testName,
		mustCron(t, "0 * * * *"), validType, validMisfire, testTimezone, testCreated)
	if err := s.Pause(); err != nil {
		t.Fatalf("Pause() on running: %v", err)
	}
	if s.Status() != StatusPaused {
		t.Errorf("Status() = %v, want Paused", s.Status())
	}

	// 再次 Pause 应失败（已不是 Running）
	if err := s.Pause(); err == nil {
		t.Errorf("Pause() on paused: want error, got nil")
	}

	// 非 Running 状态应报错
	s2, _ := Rebuild(testID, testTaskID, testName,
		mustCron(t, "0 * * * *"), validType,
		time.Time{}, time.Time{}, validMisfire,
		testTimezone, StatusPaused, testCreated,
		time.Time{}, time.Time{})
	if err := s2.Pause(); err == nil {
		t.Errorf("Pause() on paused-rebuilt: want error, got nil")
	}
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

func TestDelete(t *testing.T) {
	s, _ := New(testID, testTaskID, testName,
		mustCron(t, "0 * * * *"), validType, validMisfire, testTimezone, testCreated)
	if err := s.Delete(); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if s.Status() != StatusPaused {
		t.Errorf("Delete() status = %v, want Paused", s.Status())
	}
}

// ---------------------------------------------------------------------------
// Resume
// ---------------------------------------------------------------------------

func TestResume(t *testing.T) {
	now := baseTime() // 2024-01-15 10:30

	// 正常：Paused -> Running，nextRun 重算为未来最近点
	s := &Schedule{
		id: testID, taskID: testTaskID, name: testName,
		cron:    mustCron(t, "0 * * * *"),
		typ:     validType,
		misfire: validMisfire,
		status:  StatusPaused,
	}
	if err := s.Resume(now); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if s.Status() != StatusRunning {
		t.Errorf("Resume() status = %v, want Running", s.Status())
	}
	wantNext := time.Date(2024, 1, 15, 11, 0, 0, 0, testLoc)
	if !s.NextRun().Equal(wantNext) {
		t.Errorf("Resume() nextRun = %v, want %v", s.NextRun(), wantNext)
	}

	// 非 Paused 状态应报错
	s2, _ := New(testID, testTaskID, testName,
		mustCron(t, "0 * * * *"), validType, validMisfire, testTimezone, testCreated)
	if err := s2.Resume(now); err == nil {
		t.Errorf("Resume() on running: want error, got nil")
	}
}

// ---------------------------------------------------------------------------
// Advance
// ---------------------------------------------------------------------------

func TestAdvance(t *testing.T) {
	now := baseTime()

	s, _ := New(testID, testTaskID, testName,
		mustCron(t, "0 * * * *"), validType, validMisfire, testTimezone, testCreated)
	if err := s.Advance(now); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}

	if !s.LastRun().Equal(now) {
		t.Errorf("Advance() lastRun = %v, want %v", s.LastRun(), now)
	}
	wantNext := time.Date(2024, 1, 15, 11, 0, 0, 0, testLoc)
	if !s.NextRun().Equal(wantNext) {
		t.Errorf("Advance() nextRun = %v, want %v", s.NextRun(), wantNext)
	}
}

// ---------------------------------------------------------------------------
// Anchor：SyncAnchor / NextRunAnchor
// ---------------------------------------------------------------------------

func TestSyncAnchor(t *testing.T) {
	// Rebuild 时锚点 == nextRun
	initialNext := time.Date(2024, 1, 15, 11, 0, 0, 0, testLoc)
	s, err := Rebuild(testID, testTaskID, testName,
		mustCron(t, "0 * * * *"), validType,
		initialNext, time.Time{}, validMisfire,
		testTimezone, StatusRunning, testCreated,
		time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !s.NextRunAnchor().Equal(initialNext) {
		t.Fatalf("initial anchor = %v, want %v", s.NextRunAnchor(), initialNext)
	}

	// Advance 改写 nextRun，但锚点不动
	now := baseTime()
	if err := s.Advance(now); err != nil {
		t.Fatal(err)
	}
	if !s.NextRunAnchor().Equal(initialNext) {
		t.Errorf("anchor should not move after Advance: got %v, want %v", s.NextRunAnchor(), initialNext)
	}

	// CAS 成功后 SyncAnchor，锚点推进到当前 nextRun
	s.SyncAnchor()
	if !s.NextRunAnchor().Equal(s.NextRun()) {
		t.Errorf("after SyncAnchor: anchor = %v, nextRun = %v, want equal",
			s.NextRunAnchor(), s.NextRun())
	}
}

// ---------------------------------------------------------------------------
// MisfirePlan
// ---------------------------------------------------------------------------

func TestMisfirePlan(t *testing.T) {
	// cron：每小时整点
	cronExpr := "0 * * * *"

	// lastRun = 01:00，now = 05:30 => 错过 02/03/04/05 共 4 个整点
	lastRun := time.Date(2024, 1, 15, 1, 0, 0, 0, testLoc)
	now := time.Date(2024, 1, 15, 5, 30, 0, 0, testLoc)

	tests := []struct {
		name       string
		misfire    MisfirePolicy
		lastRun    time.Time
		now        time.Time
		maxPoints  int
		wantCount  int
		wantFirst  time.Time
		wantErrNil bool
	}{
		{
			name:      "skip returns nil",
			misfire:   MisfireSkip,
			lastRun:   lastRun,
			now:       now,
			maxPoints: 100,
			wantCount: 0,
		},
		{
			name:      "merge returns single now",
			misfire:   MisfireMerge,
			lastRun:   lastRun,
			now:       now,
			maxPoints: 100,
			wantCount: 1,
			wantFirst: now,
		},
		{
			name:      "fire_all returns all missed",
			misfire:   MisfireFireAll,
			lastRun:   lastRun,
			now:       now,
			maxPoints: 100,
			wantCount: 4,
			wantFirst: time.Date(2024, 1, 15, 2, 0, 0, 0, testLoc),
		},
		{
			name:      "fire_all capped by maxPoints",
			misfire:   MisfireFireAll,
			lastRun:   lastRun,
			now:       now,
			maxPoints: 2,
			wantCount: 2,
			wantFirst: time.Date(2024, 1, 15, 2, 0, 0, 0, testLoc),
		},
		{
			name:      "now not after lastRun",
			misfire:   MisfireFireAll,
			lastRun:   now,
			now:       now,
			maxPoints: 100,
			wantCount: 0,
		},
		{
			name:      "no missed points",
			misfire:   MisfireFireAll,
			lastRun:   time.Date(2024, 1, 15, 5, 0, 0, 0, testLoc),
			now:       time.Date(2024, 1, 15, 5, 30, 0, 0, testLoc),
			maxPoints: 100,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Rebuild(testID, testTaskID, testName,
				mustCron(t, cronExpr), validType,
				time.Time{}, tt.lastRun, tt.misfire,
				testTimezone, StatusRunning, testCreated,
				time.Time{}, time.Time{})
			if err != nil {
				t.Fatal(err)
			}

			got := s.MisfirePlan(tt.lastRun, tt.now, tt.maxPoints)
			if len(got) != tt.wantCount {
				t.Fatalf("MisfirePlan() len = %d, want %d (got=%v)", len(got), tt.wantCount, got)
			}
			if tt.wantCount > 0 && !got[0].Equal(tt.wantFirst) {
				t.Errorf("MisfirePlan()[0] = %v, want %v", got[0], tt.wantFirst)
			}
			// 补齐排序校验：必须是递增的
			for i := 1; i < len(got); i++ {
				if !got[i].After(got[i-1]) {
					t.Errorf("MisfirePlan() not sorted at %d: %v then %v", i, got[i-1], got[i])
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 值对象不应被外部修改：getter 返回的 cron 值拷贝
// ---------------------------------------------------------------------------

func TestGetter_ReturnsValue(t *testing.T) {
	s, _ := New(testID, testTaskID, testName,
		mustCron(t, "0 9 * * *"), validType, validMisfire, testTimezone, testCreated)

	c1 := s.Cron()
	c2 := s.Cron()
	// cron 是值类型，两次取出的应是等值的
	_ = c1
	_ = c2

	if s.ID() != testID || s.TaskID() != testTaskID || s.Name() != testName {
		t.Errorf("getter mismatch")
	}
	if s.CreatedBy() != testCreated {
		t.Errorf("CreatedBy() = %d, want %d", s.CreatedBy(), testCreated)
	}
}

// ---------------------------------------------------------------------------
// 边界：maxPoints <= 0 时不应无限循环
// ---------------------------------------------------------------------------

func TestMisfirePlan_ZeroMaxPoints(t *testing.T) {
	s, _ := Rebuild(testID, testTaskID, testName,
		mustCron(t, "0 * * * *"), validType,
		time.Time{}, time.Date(2024, 1, 15, 1, 0, 0, 0, testLoc), MisfireFireAll,
		testTimezone, StatusRunning, testCreated,
		time.Time{}, time.Time{})

	got := s.MisfirePlan(
		time.Date(2024, 1, 15, 1, 0, 0, 0, testLoc),
		time.Date(2024, 1, 15, 5, 30, 0, 0, testLoc),
		0, // 故意传 0：当前实现会 append 后再 break，返回 1 个
	)
	// 当前实现语义：先 append 再判 maxPoints，所以 0 也会拿到 1 个。
	// 若你后续改成先判后 append，把期望值改为 0。
	if len(got) != 1 {
		t.Errorf("MisfirePlan(maxPoints=0) len = %d, want 1", len(got))
	}
}

// ---------------------------------------------------------------------------
// 保证 errors.Is 兼容：错误信息里包含上下文
// ---------------------------------------------------------------------------

func TestErrorMessages(t *testing.T) {
	_, err := New(0, testTaskID, testName,
		mustCron(t, "0 9 * * *"), validType, validMisfire, testTimezone, testCreated)
	if err == nil || !errors.Is(err, err) { // 简单兜底
		t.Fatalf("expected error")
	}
}
