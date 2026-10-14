package scheduler

import (
	"context"
	"testing"
	"time"

	"job-scheduler/app/internal/domain/execution"
)

// fakeReaperRepo 只记录调用，返回预置的超时执行列表
type fakeReaperRepo struct {
	scanned []*execution.Execution
	updated []*execution.Execution
}

func (f *fakeReaperRepo) ScanTimeout(ctx context.Context, olderThan time.Time, limit int) ([]*execution.Execution, error) {
	return f.scanned, nil
}

func (f *fakeReaperRepo) UpdateTerminal(ctx context.Context, e *execution.Execution) error {
	f.updated = append(f.updated, e)
	return nil
}

// 功能验证：reaper 把 Running 超时的执行强制收割为 Failed（后续重试交给 planner）
func TestReaperReapsTimedOutExecutions(t *testing.T) {
	now := time.Now()

	// 一条 Running 了 1 小时仍未完结的执行
	hung, err := execution.New(1, 2, 0, "trace-reap", now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := hung.Claim("worker-1", now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	// 一条正常终态的执行：不应被收割
	done, err := execution.New(1, 2, 0, "trace-done", now)
	if err != nil {
		t.Fatal(err)
	}
	_ = done.Claim("worker-1", now)
	_ = done.FinishSuccess(nil, now)

	f := &fakeReaperRepo{scanned: []*execution.Execution{hung, done}}
	rp := NewReaper(f)
	rp.reapOnce(context.Background())

	// done 虽被 ScanTimeout 返回（模拟误扫），但 Reap 对终态执行会拒绝迁移，
	// reaper 正确地跳过它、只收割真正超时的那条
	if len(f.updated) != 1 {
		t.Fatalf("UpdateTerminal 调用 %d 次, 期望 1（终态执行被领域守卫拒绝）", len(f.updated))
	}
	if hung.Status() != execution.StatusFailed {
		t.Fatalf("超时执行应被收割为 Failed, 实际 %v", hung.Status())
	}
	if done.Status() != execution.StatusSuccess {
		t.Fatalf("已成功的执行不应被改状态, 实际 %v", done.Status())
	}
}
