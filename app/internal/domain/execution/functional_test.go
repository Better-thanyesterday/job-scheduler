package execution_test

import (
	"testing"
	"time"

	"job-scheduler/app/internal/domain/execution"
)

// 功能验证：一次执行的完整生命周期（状态机是执行域的核心正确性）
func TestExecutionLifecycle(t *testing.T) {
	now := time.Now()

	// ---------- 成功路径 ----------
	e, err := execution.New(100, 200, 1, "trace-1", now)
	if err != nil {
		t.Fatal(err)
	}
	if e.Status() != execution.StatusPending {
		t.Fatalf("出生状态 = %v, 期望 Pending", e.Status())
	}
	// 非法迁移：Pending 不能直接成功
	if err := e.FinishSuccess(nil, now); err == nil {
		t.Fatal("Pending → Success 应当被拒绝")
	}
	if err := e.Claim("worker-1", now); err != nil {
		t.Fatalf("Pending → Running 应当合法: %v", err)
	}
	// 非法迁移：Running 不能重复 Claim
	if err := e.Claim("worker-2", now); err == nil {
		t.Fatal("Running 重复 Claim 应当被拒绝")
	}
	if err := e.FinishSuccess([]byte("ok"), now.Add(time.Second)); err != nil {
		t.Fatalf("Running → Success 应当合法: %v", err)
	}
	if !e.Status().IsTerminal() {
		t.Fatal("Success 应当是终态")
	}
	if e.DurationMS() != 1000 {
		t.Fatalf("DurationMS = %d, 期望 1000", e.DurationMS())
	}
	// 终态是黑洞
	if err := e.FinishFailed("late report", now); err == nil {
		t.Fatal("终态不可迁出")
	}

	// ---------- 失败重试路径（maxRetry=2）----------
	e2, err := execution.New(100, 200, 2, "trace-2", now)
	if err != nil {
		t.Fatal(err)
	}
	_ = e2.Claim("worker-1", now)
	_ = e2.FinishFailed("boom", now)
	if !e2.Retryable() {
		t.Fatal("首次失败且未达上限，应当可重试")
	}
	next, err := e2.PlanRetry(time.Minute, now)
	if err != nil {
		t.Fatalf("PlanRetry 应当合法: %v", err)
	}
	if !next.Equal(now.Add(time.Minute)) {
		t.Fatalf("下次重试时间 = %v, 期望 now+1m", next)
	}
	if e2.RetryCount() != 1 || e2.Status() != execution.StatusRetrying {
		t.Fatalf("PlanRetry 后 count=%d status=%v, 期望 1/Retrying", e2.RetryCount(), e2.Status())
	}

	// 第二轮：Retrying → Running → Failed（此时 retryCount=1 < maxRetry=2，仍可再重试）
	if err := e2.Claim("worker-1", now); err != nil {
		t.Fatalf("Retrying → Running 应当合法: %v", err)
	}
	_ = e2.FinishFailed("boom-2", now)
	if !e2.Retryable() {
		t.Fatal("第 2 次失败时才计划过 1 次重试（count=1 < 2），仍应可重试")
	}
	if _, err := e2.PlanRetry(time.Minute, now); err != nil {
		t.Fatalf("第二次 PlanRetry 应当合法: %v", err)
	}

	// 第三轮：第 2 次重试也失败（count=2 == maxRetry），到达终局条件
	if err := e2.Claim("worker-1", now); err != nil {
		t.Fatalf("Retrying → Running 应当合法: %v", err)
	}
	_ = e2.FinishFailed("boom-3", now)
	if e2.Retryable() {
		t.Fatal("重试次数已达上限（2/2），不应再可重试")
	}

	// 终局：Failed → FailedFinal（planner 专属），之后不可再 PlanRetry
	if err := e2.MarkFailedFinal("retry exhausted", now); err != nil {
		t.Fatalf("MarkFailedFinal 应当合法: %v", err)
	}
	if e2.Status() != execution.StatusFailedFinal {
		t.Fatalf("终局状态 = %v, 期望 FailedFinal", e2.Status())
	}
	if _, err := e2.PlanRetry(time.Minute, now); err == nil {
		t.Fatal("终态不可再 PlanRetry")
	}
}
