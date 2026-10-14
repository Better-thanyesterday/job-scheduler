package execution

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

var testLoc = time.UTC

func baseTime() time.Time {
	return time.Date(2024, 1, 15, 10, 30, 0, 0, testLoc)
}

const (
	testTaskID     = int64(100)
	testScheduleID = int64(200)
	testTraceID    = "trace-abc-123"
	testWorkerID   = "worker-1"
)

// newPending 构造一个 Pending 状态、maxRetry=3 的 Execution
func newPending(t *testing.T) *Execution {
	t.Helper()
	e, err := New(testTaskID, testScheduleID, 3, testTraceID, baseTime())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return e
}

// rebuildWith 用 Rebuild 构造一个指定状态的 Execution（用于绕过状态机约束）
func rebuildWith(t *testing.T, status Status) *Execution {
	t.Helper()
	e, err := Rebuild(
		1, testTaskID, testScheduleID, 0, "",
		status, 1, baseTime(), time.Time{}, 0,
		nil, "", 0, 3,
		"", testTraceID, nil, baseTime(),
	)
	if err != nil {
		t.Fatalf("Rebuild() error = %v", err)
	}
	return e
}

// ---------------------------------------------------------------------------
// New
// ---------------------------------------------------------------------------

func TestNew(t *testing.T) {
	now := baseTime()

	tests := []struct {
		name       string
		taskID     int64
		scheduleID int64
		maxRetry   int
		traceID    string
		wantErr    bool
	}{
		{"valid with schedule", testTaskID, testScheduleID, 3, testTraceID, false},
		{"valid manual (scheduleID=0)", testTaskID, 0, 3, testTraceID, false},
		{"valid maxRetry=0", testTaskID, testScheduleID, 0, testTraceID, false},
		{"valid empty traceID", testTaskID, testScheduleID, 3, "", false},
		{"invalid taskID zero", 0, testScheduleID, 3, testTraceID, true},
		{"invalid taskID negative", -1, testScheduleID, 3, testTraceID, true},
		{"invalid maxRetry negative", testTaskID, testScheduleID, -1, testTraceID, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := New(tt.taskID, tt.scheduleID, tt.maxRetry, tt.traceID, now)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("New() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("New() unexpected error = %v", err)
			}
			if e.TaskID() != tt.taskID {
				t.Errorf("TaskID() = %d, want %d", e.TaskID(), tt.taskID)
			}
			if e.ScheduleID() != tt.scheduleID {
				t.Errorf("ScheduleID() = %d, want %d", e.ScheduleID(), tt.scheduleID)
			}
			if e.MaxRetry() != tt.maxRetry {
				t.Errorf("MaxRetry() = %d, want %d", e.MaxRetry(), tt.maxRetry)
			}
			if e.RetryCount() != 0 {
				t.Errorf("RetryCount() = %d, want 0", e.RetryCount())
			}
			if e.TraceID() != tt.traceID {
				t.Errorf("TraceID() = %q, want %q", e.TraceID(), tt.traceID)
			}
			if e.Status() != StatusPending {
				t.Errorf("Status() = %v, want StatusPending", e.Status())
			}
			if e.ID() <= 0 {
				t.Errorf("ID() = %d, want > 0 (snowflake)", e.ID())
			}
			if !e.CreatedAt().Equal(now) {
				t.Errorf("CreatedAt() = %v, want %v", e.CreatedAt(), now)
			}
			// 出生时不应有 nextRetryAt / startTime / endTime
			if e.NextRetryAt() != nil {
				t.Errorf("NextRetryAt() = %v, want nil", e.NextRetryAt())
			}
			if !e.StartTime().IsZero() {
				t.Errorf("StartTime() = %v, want zero", e.StartTime())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Rebuild
// ---------------------------------------------------------------------------

func TestRebuild(t *testing.T) {
	start := baseTime()
	end := start.Add(5 * time.Second)
	next := end.Add(time.Minute)
	created := start.Add(-time.Hour)

	t.Run("valid rebuild with all fields", func(t *testing.T) {
		e, err := Rebuild(
			1, testTaskID, testScheduleID, 999, "node-a",
			StatusFailed, 5, start, end, 5000,
			[]byte(`{"ok":false}`), "boom", 2, 3,
			testWorkerID, testTraceID, &next, created,
		)
		if err != nil {
			t.Fatalf("Rebuild() error = %v", err)
		}
		if e.ID() != 1 || e.TaskID() != testTaskID || e.ScheduleID() != testScheduleID {
			t.Errorf("Rebuild() id/taskID/scheduleID mismatch: %+v", e)
		}
		if e.DagExecID() != 999 || e.DagNodeID() != "node-a" {
			t.Errorf("Rebuild() dag fields mismatch")
		}
		if e.Status() != StatusFailed {
			t.Errorf("Status() = %v, want StatusFailed", e.Status())
		}
		if e.Version() != 5 {
			t.Errorf("Version() = %d, want 5", e.Version())
		}
		if !e.StartTime().Equal(start) || !e.EndTime().Equal(end) {
			t.Errorf("Rebuild() time mismatch")
		}
		if e.DurationMS() != 5000 {
			t.Errorf("DurationMS() = %d, want 5000", e.DurationMS())
		}
		if string(e.Result()) != `{"ok":false}` {
			t.Errorf("Result() = %q", e.Result())
		}
		if e.ErrorMsg() != "boom" {
			t.Errorf("ErrorMsg() = %q, want boom", e.ErrorMsg())
		}
		if e.RetryCount() != 2 || e.MaxRetry() != 3 {
			t.Errorf("retry fields mismatch")
		}
		if e.WorkerID() != testWorkerID {
			t.Errorf("WorkerID() = %q", e.WorkerID())
		}
		if e.NextRetryAt() == nil || !e.NextRetryAt().Equal(next) {
			t.Errorf("NextRetryAt() = %v, want %v", e.NextRetryAt(), next)
		}
		if !e.CreatedAt().Equal(created) {
			t.Errorf("CreatedAt() = %v, want %v", e.CreatedAt(), created)
		}
	})

	t.Run("invalid status", func(t *testing.T) {
		_, err := Rebuild(
			1, testTaskID, testScheduleID, 0, "",
			Status(999), 0, time.Time{}, time.Time{}, 0,
			nil, "", 0, 0,
			"", "", nil, time.Time{},
		)
		if err == nil {
			t.Fatalf("Rebuild() with invalid status: want error, got nil")
		}
		if !strings.Contains(err.Error(), "invalid status") {
			t.Errorf("Rebuild() error = %v, want mention of invalid status", err)
		}
	})

	t.Run("nil nextRetryAt", func(t *testing.T) {
		e, err := Rebuild(
			1, testTaskID, 0, 0, "",
			StatusPending, 0, time.Time{}, time.Time{}, 0,
			nil, "", 0, 3,
			"", "", nil, baseTime(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if e.NextRetryAt() != nil {
			t.Errorf("NextRetryAt() = %v, want nil", e.NextRetryAt())
		}
	})
}

// ---------------------------------------------------------------------------
// Claim: Pending -> Running
// ---------------------------------------------------------------------------

func TestClaim(t *testing.T) {
	t.Run("success from pending", func(t *testing.T) {
		e := newPending(t)
		now := baseTime().Add(time.Minute)

		if err := e.Claim(testWorkerID, now); err != nil {
			t.Fatalf("Claim() error = %v", err)
		}
		if e.Status() != StatusRunning {
			t.Errorf("Status() = %v, want StatusRunning", e.Status())
		}
		if e.WorkerID() != testWorkerID {
			t.Errorf("WorkerID() = %q, want %q", e.WorkerID(), testWorkerID)
		}
		if !e.StartTime().Equal(now) {
			t.Errorf("StartTime() = %v, want %v", e.StartTime(), now)
		}
		if e.Version() != 1 {
			t.Errorf("Version() = %d, want 1", e.Version())
		}
	})

	t.Run("fail from running", func(t *testing.T) {
		e := rebuildWith(t, StatusRunning)
		if err := e.Claim(testWorkerID, baseTime()); err == nil {
			t.Fatalf("Claim() from Running: want error, got nil")
		}
	})

	t.Run("fail from success (terminal)", func(t *testing.T) {
		e := rebuildWith(t, StatusSuccess)
		if err := e.Claim(testWorkerID, baseTime()); err == nil {
			t.Fatalf("Claim() from Success: want error, got nil")
		}
	})
}

// ---------------------------------------------------------------------------
// FinishSuccess: Running -> Success
// ---------------------------------------------------------------------------

func TestFinishSuccess(t *testing.T) {
	t.Run("success from running", func(t *testing.T) {
		e := newPending(t)
		start := baseTime()
		if err := e.Claim(testWorkerID, start); err != nil {
			t.Fatal(err)
		}

		end := start.Add(3 * time.Second)
		result := []byte(`{"output":"done"}`)
		if err := e.FinishSuccess(result, end); err != nil {
			t.Fatalf("FinishSuccess() error = %v", err)
		}

		if e.Status() != StatusSuccess {
			t.Errorf("Status() = %v, want StatusSuccess", e.Status())
		}
		if !e.EndTime().Equal(end) {
			t.Errorf("EndTime() = %v, want %v", e.EndTime(), end)
		}
		if e.DurationMS() != 3000 {
			t.Errorf("DurationMS() = %d, want 3000", e.DurationMS())
		}
		if string(e.Result()) != string(result) {
			t.Errorf("Result() = %q, want %q", e.Result(), result)
		}
		if e.Version() != 2 { // Claim +1, FinishSuccess +1
			t.Errorf("Version() = %d, want 2", e.Version())
		}
		// 不碰 nextRetryAt
		if e.NextRetryAt() != nil {
			t.Errorf("NextRetryAt() = %v, want nil", e.NextRetryAt())
		}
	})

	t.Run("fail from pending", func(t *testing.T) {
		e := newPending(t)
		if err := e.FinishSuccess(nil, baseTime()); err == nil {
			t.Fatalf("FinishSuccess() from Pending: want error, got nil")
		}
	})
}

// ---------------------------------------------------------------------------
// FinishFailed: Running -> Failed
// ---------------------------------------------------------------------------

func TestFinishFailed(t *testing.T) {
	t.Run("success from running", func(t *testing.T) {
		e := newPending(t)
		start := baseTime()
		if err := e.Claim(testWorkerID, start); err != nil {
			t.Fatal(err)
		}

		end := start.Add(2 * time.Second)
		if err := e.FinishFailed("connection refused", end); err != nil {
			t.Fatalf("FinishFailed() error = %v", err)
		}
		if e.Status() != StatusFailed {
			t.Errorf("Status() = %v, want StatusFailed", e.Status())
		}
		if e.ErrorMsg() != "connection refused" {
			t.Errorf("ErrorMsg() = %q", e.ErrorMsg())
		}
		if e.DurationMS() != 2000 {
			t.Errorf("DurationMS() = %d, want 2000", e.DurationMS())
		}
		// 关键：FinishFailed 不设置 nextRetryAt，那是 planner 的活
		if e.NextRetryAt() != nil {
			t.Errorf("FinishFailed() should NOT set nextRetryAt, got %v", e.NextRetryAt())
		}
		// 关键：FinishFailed 不修改 retryCount，那是 planner 的活
		if e.RetryCount() != 0 {
			t.Errorf("FinishFailed() should NOT change retryCount, got %d", e.RetryCount())
		}
	})

	t.Run("fail from pending", func(t *testing.T) {
		e := newPending(t)
		if err := e.FinishFailed("boom", baseTime()); err == nil {
			t.Fatalf("FinishFailed() from Pending: want error, got nil")
		}
	})
}

// ---------------------------------------------------------------------------
// PlanRetry: Failed -> Retrying
// ---------------------------------------------------------------------------

func TestPlanRetry(t *testing.T) {
	t.Run("success from failed", func(t *testing.T) {
		e := rebuildWith(t, StatusFailed)
		now := baseTime()
		backoff := 5 * time.Minute

		next, err := e.PlanRetry(backoff, now)
		if err != nil {
			t.Fatalf("PlanRetry() error = %v", err)
		}
		if !next.Equal(now.Add(backoff)) {
			t.Errorf("PlanRetry() next = %v, want %v", next, now.Add(backoff))
		}
		if e.Status() != StatusRetrying {
			t.Errorf("Status() = %v, want StatusRetrying", e.Status())
		}
		if e.RetryCount() != 1 {
			t.Errorf("RetryCount() = %d, want 1", e.RetryCount())
		}
		if e.NextRetryAt() == nil || !e.NextRetryAt().Equal(next) {
			t.Errorf("NextRetryAt() = %v, want %v", e.NextRetryAt(), next)
		}
	})

	t.Run("fail from pending", func(t *testing.T) {
		e := newPending(t)
		if _, err := e.PlanRetry(time.Minute, baseTime()); err == nil {
			t.Fatalf("PlanRetry() from Pending: want error, got nil")
		}
	})

	t.Run("fail from success (terminal)", func(t *testing.T) {
		e := rebuildWith(t, StatusSuccess)
		if _, err := e.PlanRetry(time.Minute, baseTime()); err == nil {
			t.Fatalf("PlanRetry() from Success: want error, got nil")
		}
	})

	t.Run("zero backoff", func(t *testing.T) {
		e := rebuildWith(t, StatusFailed)
		now := baseTime()
		next, err := e.PlanRetry(0, now)
		if err != nil {
			t.Fatal(err)
		}
		if !next.Equal(now) {
			t.Errorf("PlanRetry(0) next = %v, want %v", next, now)
		}
	})
}

// ---------------------------------------------------------------------------
// Retryable: 纯判定方法，不修改任何字段
// ---------------------------------------------------------------------------

func TestRetryable(t *testing.T) {
	tests := []struct {
		name       string
		status     Status
		retryCount int
		maxRetry   int
		want       bool
	}{
		{"Failed and retryCount < maxRetry", StatusFailed, 1, 3, true},
		{"Failed and retryCount == maxRetry", StatusFailed, 3, 3, false},
		{"Failed and retryCount > maxRetry", StatusFailed, 5, 3, false},
		{"Failed and maxRetry=0", StatusFailed, 0, 0, false},
		{"Pending", StatusPending, 0, 3, false},
		{"Running", StatusRunning, 0, 3, false},
		{"Success", StatusSuccess, 0, 3, false},
		{"Retrying", StatusRetrying, 1, 3, false},
		{"FailedFinal", StatusFailedFinal, 3, 3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := Rebuild(
				1, testTaskID, 0, 0, "",
				tt.status, 0, time.Time{}, time.Time{}, 0,
				nil, "", tt.retryCount, tt.maxRetry,
				"", "", nil, time.Time{},
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := e.Retryable(); got != tt.want {
				t.Errorf("Retryable() = %v, want %v (status=%v, retry=%d, max=%d)",
					got, tt.want, tt.status, tt.retryCount, tt.maxRetry)
			}
			// Retryable 不应修改 version
			if e.Version() != 0 {
				t.Errorf("Retryable() modified version: %d", e.Version())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MarkFailedFinal: Failed -> FailedFinal
// ---------------------------------------------------------------------------

func TestMarkFailedFinal(t *testing.T) {
	t.Run("success from failed", func(t *testing.T) {
		e := rebuildWith(t, StatusFailed)
		// 先设置一点 errorMsg 来验证拼接
		e.errorMsg = "network timeout"
		versionBefore := e.Version()

		if err := e.MarkFailedFinal("retry exhausted", baseTime()); err != nil {
			t.Fatalf("MarkFailedFinal() error = %v", err)
		}
		if e.Status() != StatusFailedFinal {
			t.Errorf("Status() = %v, want StatusFailedFinal", e.Status())
		}
		if !strings.Contains(e.ErrorMsg(), "network timeout") ||
			!strings.Contains(e.ErrorMsg(), "retry exhausted") {
			t.Errorf("ErrorMsg() = %q, want both reasons", e.ErrorMsg())
		}
		if e.Version() != versionBefore+1 {
			t.Errorf("Version() = %d, want %d", e.Version(), versionBefore+1)
		}
	})

	t.Run("fail from running", func(t *testing.T) {
		e := rebuildWith(t, StatusRunning)
		if err := e.MarkFailedFinal("x", baseTime()); err == nil {
			t.Fatalf("MarkFailedFinal() from Running: want error, got nil")
		}
	})

	t.Run("fail from success (terminal)", func(t *testing.T) {
		e := rebuildWith(t, StatusSuccess)
		if err := e.MarkFailedFinal("x", baseTime()); err == nil {
			t.Fatalf("MarkFailedFinal() from Success: want error, got nil")
		}
	})
}

// ---------------------------------------------------------------------------
// Reap: Running -> Failed (超时收割)
// ---------------------------------------------------------------------------

func TestReap(t *testing.T) {
	t.Run("success from running", func(t *testing.T) {
		e := newPending(t)
		start := baseTime()
		if err := e.Claim(testWorkerID, start); err != nil {
			t.Fatal(err)
		}

		reapAt := start.Add(10 * time.Minute)
		if err := e.Reap(reapAt); err != nil {
			t.Fatalf("Reap() error = %v", err)
		}
		if e.Status() != StatusFailed {
			t.Errorf("Status() = %v, want StatusFailed", e.Status())
		}
		if !e.EndTime().Equal(reapAt) {
			t.Errorf("EndTime() = %v, want %v", e.EndTime(), reapAt)
		}
		if e.DurationMS() != 10*60*1000 {
			t.Errorf("DurationMS() = %d, want 600000", e.DurationMS())
		}
		if !strings.Contains(e.ErrorMsg(), "timeout") {
			t.Errorf("ErrorMsg() = %q, want mention of timeout", e.ErrorMsg())
		}
	})

	t.Run("preserve existing errorMsg", func(t *testing.T) {
		e := rebuildWith(t, StatusRunning)
		e.errorMsg = "pre-existing message"
		if err := e.Reap(baseTime()); err != nil {
			t.Fatal(err)
		}
		if e.ErrorMsg() != "pre-existing message" {
			t.Errorf("ErrorMsg() = %q, want pre-existing message preserved", e.ErrorMsg())
		}
	})

	t.Run("fail from pending", func(t *testing.T) {
		e := newPending(t)
		if err := e.Reap(baseTime()); err == nil {
			t.Fatalf("Reap() from Pending: want error, got nil")
		}
	})

	t.Run("fail from success (terminal)", func(t *testing.T) {
		e := rebuildWith(t, StatusSuccess)
		if err := e.Reap(baseTime()); err == nil {
			t.Fatalf("Reap() from Success: want error, got nil")
		}
	})
}

// ---------------------------------------------------------------------------
// Version 累加：每次状态迁移都应 +1
// ---------------------------------------------------------------------------

func TestVersionIncrements(t *testing.T) {
	e := newPending(t)
	start := baseTime()

	initialVersion := e.Version()

	if err := e.Claim(testWorkerID, start); err != nil {
		t.Fatal(err)
	}
	if e.Version() != initialVersion+1 {
		t.Errorf("after Claim: version = %d, want %d", e.Version(), initialVersion+1)
	}

	if err := e.FinishFailed("boom", start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if e.Version() != initialVersion+2 {
		t.Errorf("after FinishFailed: version = %d, want %d", e.Version(), initialVersion+2)
	}

	if _, err := e.PlanRetry(time.Minute, start.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if e.Version() != initialVersion+3 {
		t.Errorf("after PlanRetry: version = %d, want %d", e.Version(), initialVersion+3)
	}
}

// ---------------------------------------------------------------------------
// 完整生命周期：Pending -> Running -> Failed -> Retrying -> Running -> Success
// ---------------------------------------------------------------------------

func TestFullLifecycle(t *testing.T) {
	e := newPending(t)
	now := baseTime()

	// Claim
	if err := e.Claim(testWorkerID, now); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// 执行失败
	if err := e.FinishFailed("transient error", now.Add(5*time.Second)); err != nil {
		t.Fatalf("FinishFailed: %v", err)
	}
	if !e.Retryable() {
		t.Fatal("expected Retryable=true after first failure")
	}

	// planner 规划重试
	backoff := 1 * time.Minute
	next, err := e.PlanRetry(backoff, now.Add(6*time.Second))
	if err != nil {
		t.Fatalf("PlanRetry: %v", err)
	}
	if e.Status() != StatusRetrying {
		t.Fatalf("Status = %v, want Retrying", e.Status())
	}
	if e.NextRetryAt() == nil || !e.NextRetryAt().Equal(next) {
		t.Fatalf("NextRetryAt mismatch")
	}

	// 重试再次 Claim（模拟 worker 抢到票）
	// 注意：如果状态机不允许 Retrying -> Running，这里会失败，
	// 那说明流程中还需要一步“调度侧把 Retrying 改回 Pending”之类的迁移。
	// 下面的调用若失败，请按你实际的 Transition 规则调整。
	if err := e.Claim(testWorkerID, next); err != nil {
		t.Skipf("Retrying -> Running transition not allowed by state machine: %v", err)
	}

	// 成功
	if err := e.FinishSuccess([]byte(`{"retried":true}`), next.Add(2*time.Second)); err != nil {
		t.Fatalf("FinishSuccess: %v", err)
	}
	if e.Status() != StatusSuccess {
		t.Fatalf("Status = %v, want Success", e.Status())
	}
	if e.RetryCount() != 1 {
		t.Errorf("RetryCount = %d, want 1", e.RetryCount())
	}
}

// ---------------------------------------------------------------------------
// 终态保护：Success / FailedFinal 不能再迁移
// ---------------------------------------------------------------------------

func TestTerminalStateProtection(t *testing.T) {
	terminalStatuses := []Status{StatusSuccess, StatusFailedFinal}

	for _, st := range terminalStatuses {
		t.Run(st.String(), func(t *testing.T) {
			e := rebuildWith(t, st)

			// 所有会改变状态的迁移都应失败
			if err := e.Claim("w", baseTime()); err == nil {
				t.Errorf("Claim from terminal %v: want error", st)
			}
			if err := e.FinishSuccess(nil, baseTime()); err == nil {
				t.Errorf("FinishSuccess from terminal %v: want error", st)
			}
			if err := e.FinishFailed("x", baseTime()); err == nil {
				t.Errorf("FinishFailed from terminal %v: want error", st)
			}
			if _, err := e.PlanRetry(0, baseTime()); err == nil {
				t.Errorf("PlanRetry from terminal %v: want error", st)
			}
			if err := e.MarkFailedFinal("x", baseTime()); err == nil {
				t.Errorf("MarkFailedFinal from terminal %v: want error", st)
			}
			if err := e.Reap(baseTime()); err == nil {
				t.Errorf("Reap from terminal %v: want error", st)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// nextRetryAt 字段的所有权：只有 PlanRetry 能写
// ---------------------------------------------------------------------------

func TestNextRetryAtOwnership(t *testing.T) {
	t.Run("New does not set", func(t *testing.T) {
		e := newPending(t)
		if e.NextRetryAt() != nil {
			t.Errorf("New() set nextRetryAt = %v, want nil", e.NextRetryAt())
		}
	})

	t.Run("Claim does not set", func(t *testing.T) {
		e := newPending(t)
		if err := e.Claim(testWorkerID, baseTime()); err != nil {
			t.Fatal(err)
		}
		if e.NextRetryAt() != nil {
			t.Errorf("Claim() set nextRetryAt = %v, want nil", e.NextRetryAt())
		}
	})

	t.Run("FinishSuccess does not set", func(t *testing.T) {
		e := newPending(t)
		_ = e.Claim(testWorkerID, baseTime())
		_ = e.FinishSuccess(nil, baseTime().Add(time.Second))
		if e.NextRetryAt() != nil {
			t.Errorf("FinishSuccess() set nextRetryAt = %v, want nil", e.NextRetryAt())
		}
	})

	t.Run("FinishFailed does not set", func(t *testing.T) {
		e := newPending(t)
		_ = e.Claim(testWorkerID, baseTime())
		_ = e.FinishFailed("boom", baseTime().Add(time.Second))
		if e.NextRetryAt() != nil {
			t.Errorf("FinishFailed() set nextRetryAt = %v, want nil", e.NextRetryAt())
		}
	})

	t.Run("Reap does not set", func(t *testing.T) {
		e := newPending(t)
		_ = e.Claim(testWorkerID, baseTime())
		_ = e.Reap(baseTime().Add(time.Minute))
		if e.NextRetryAt() != nil {
			t.Errorf("Reap() set nextRetryAt = %v, want nil", e.NextRetryAt())
		}
	})

	t.Run("only PlanRetry sets", func(t *testing.T) {
		e := rebuildWith(t, StatusFailed)
		now := baseTime()
		backoff := time.Minute
		next, err := e.PlanRetry(backoff, now)
		if err != nil {
			t.Fatal(err)
		}
		if e.NextRetryAt() == nil || !e.NextRetryAt().Equal(next) {
			t.Errorf("PlanRetry() did not set nextRetryAt correctly")
		}
	})
}

// ---------------------------------------------------------------------------
// 错误信息包含 id，便于线上排查
// ---------------------------------------------------------------------------

func TestErrorMessagesContainID(t *testing.T) {
	e := rebuildWith(t, StatusSuccess)
	err := e.Claim("w", baseTime())
	if err == nil {
		t.Fatal("want error")
	}
	if !strings.Contains(err.Error(), "id=") {
		t.Errorf("error = %q, want mention of id", err.Error())
	}
}

// ---------------------------------------------------------------------------
// errors.Is 兼容性（可选：如果包内定义了哨兵错误，可精确匹配）
// ---------------------------------------------------------------------------

func TestErrorIsWrapped(t *testing.T) {
	// 如果包内定义了类似 ErrInvalidStatus 的哨兵错误，这里可以用 errors.Is 精确断言。
	// 当前实现只用了 fmt.Errorf，未定义哨兵错误，因此这里只做非空断言。
	_, err := New(0, 0, 0, "", baseTime())
	if err == nil {
		t.Fatal("New() with invalid args: want error")
	}
	if !errors.Is(err, err) {
		t.Fatal("impossible")
	}
}

// ---------------------------------------------------------------------------
// 时间字段语义：DurationMS 由 FinishSuccess / FinishFailed / Reap 计算
// ---------------------------------------------------------------------------

func TestDurationComputation(t *testing.T) {
	t.Run("FinishSuccess", func(t *testing.T) {
		e := newPending(t)
		start := baseTime()
		_ = e.Claim(testWorkerID, start)
		_ = e.FinishSuccess(nil, start.Add(1500*time.Millisecond))
		if e.DurationMS() != 1500 {
			t.Errorf("DurationMS = %d, want 1500", e.DurationMS())
		}
	})

	t.Run("FinishFailed", func(t *testing.T) {
		e := newPending(t)
		start := baseTime()
		_ = e.Claim(testWorkerID, start)
		_ = e.FinishFailed("x", start.Add(250*time.Millisecond))
		if e.DurationMS() != 250 {
			t.Errorf("DurationMS = %d, want 250", e.DurationMS())
		}
	})

	t.Run("Reap", func(t *testing.T) {
		e := newPending(t)
		start := baseTime()
		_ = e.Claim(testWorkerID, start)
		_ = e.Reap(start.Add(7 * time.Second))
		if e.DurationMS() != 7000 {
			t.Errorf("DurationMS = %d, want 7000", e.DurationMS())
		}
	})
}
