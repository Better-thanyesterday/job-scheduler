package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/domain/schedule"
	"job-scheduler/app/internal/domain/task"
)

// ===========================================================================
// 测试辅助：fakes
// ===========================================================================

// fakeRetryPlannerRepo 实现 execution.RetryPlannerRepo
type fakeRetryPlannerRepo struct {
	mu sync.Mutex

	// ScanRetryable
	scanResult  []*execution.Execution
	scanErr     error
	scanCallN   int
	scanLastNow time.Time
	scanLastLim int

	// MarkFailedFinal
	finalMarked []*execution.Execution
	finalErr    error

	// PlanRetry
	planCalls  []*execution.Execution
	planResult bool
	planErr    error
}

var _ execution.RetryPlannerRepo = (*fakeRetryPlannerRepo)(nil)

func (f *fakeRetryPlannerRepo) ScanRetryable(
	ctx context.Context, now time.Time, limit int,
) ([]*execution.Execution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scanCallN++
	f.scanLastNow = now
	f.scanLastLim = limit
	return f.scanResult, f.scanErr
}

func (f *fakeRetryPlannerRepo) MarkFailedFinal(
	ctx context.Context, e *execution.Execution,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finalMarked = append(f.finalMarked, e)
	return f.finalErr
}

func (f *fakeRetryPlannerRepo) PlanRetry(
	ctx context.Context, e *execution.Execution,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.planCalls = append(f.planCalls, e)
	return f.planResult, f.planErr
}

// fakeTaskSource 实现 schedule.TaskSource
type fakeTaskSource struct {
	mu         sync.Mutex
	listResult []*task.Task
	listErr    error
	getResult  *task.Task
	getErr     error
}

var _ schedule.TaskSource = (*fakeTaskSource)(nil)

func (f *fakeTaskSource) ListByIDs(
	ctx context.Context, ids []int64,
) ([]*task.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listResult, f.listErr
}

func (f *fakeTaskSource) Get(
	ctx context.Context, id int64,
) (*task.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getResult, f.getErr
}

// fakePusher 实现 coordination.TaskPusher
type fakePusher struct {
	mu   sync.Mutex
	msgs []*coordination.ExecutionMessage
	err  error
}

var _ coordination.TaskPusher = (*fakePusher)(nil)

func (f *fakePusher) Push(ctx context.Context, msg *coordination.ExecutionMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, msg)
	return f.err
}

func (f *fakePusher) callN() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.msgs)
}

// pushedIDs 返回已投递消息的 ExecID 列表（按投递顺序）
func (f *fakePusher) pushedIDs() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]int64, 0, len(f.msgs))
	for _, m := range f.msgs {
		ids = append(ids, m.ExecID)
	}
	return ids
}

// ===========================================================================
// 测试辅助：构造领域对象
// ===========================================================================

// mkFailedExec 构造一个 Failed 状态的 Execution
//   - retryCount: 已重试次数
//   - maxRetry:   上限
func mkFailedExec(t *testing.T, id, taskID int64, retryCount, maxRetry int) *execution.Execution {
	t.Helper()
	e, err := execution.Rebuild(
		id, taskID, 0, 0, "",
		execution.StatusFailed, 1,
		time.Time{}, time.Time{}, 0,
		nil, "", retryCount, maxRetry,
		"", "", nil, time.Now(),
	)
	if err != nil {
		t.Fatalf("execution.Rebuild: %v", err)
	}
	return e
}

// mkTask 构造一个带 RetryPolicy 的 Task。
// planner 只依赖 t.ID() 和 t.RetryPolicy()，config 用最小合法的 HTTP 配置填充
func mkTask(t *testing.T, id int64, maxRetry int, interval time.Duration) *task.Task {
	t.Helper()
	return mkTaskWithPolicy(t, id, mustPolicy(t, maxRetry, task.StrategyFixed, interval))
}

// mkTaskWithPolicy 指定完整策略的构造版本（指数退避等场景用）
func mkTaskWithPolicy(t *testing.T, id int64, policy task.RetryPolicy) *task.Task {
	t.Helper()
	cfg, err := task.NewTaskConfig(task.TaskTypeHTTP,
		json.RawMessage(`{"url":"http://example.com/hook","method":"GET"}`))
	if err != nil {
		t.Fatalf("task.NewTaskConfig: %v", err)
	}
	// Rebuild 信任入参，不做业务校验；now 同时充当 createdAt/updatedAt
	now := time.Now()
	return task.Rebuild(id, "test-task", cfg, policy, 5, 60,
		task.TaskStatusPending, "planner test", 1, now, now, task.TaskTypeHTTP)
}

func mustPolicy(t *testing.T, maxRetry int, strategy task.Strategy, interval time.Duration) task.RetryPolicy {
	t.Helper()
	p, err := task.NewRetryPolicy(maxRetry, strategy, interval)
	if err != nil {
		t.Fatalf("task.NewRetryPolicy: %v", err)
	}
	return *p
}

// ===========================================================================
// 1. 到期重投：未耗尽，CAS 成功 → PlanRetry + Push
// ===========================================================================

func TestPlanOnce_RetryScheduled(t *testing.T) {
	exec := mkFailedExec(t, 1, 100, 0, 3) // 0/3，可重试
	tk := mkTask(t, 100, 3, time.Minute)

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{exec},
		planResult: true, // CAS 抢占成功
	}
	src := &fakeTaskSource{listResult: []*task.Task{tk}}
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)
	p.planOnce(context.Background())

	if got := len(repo.finalMarked); got != 0 {
		t.Errorf("MarkFailedFinal called %d times, want 0", got)
	}
	if got := len(repo.planCalls); got != 1 {
		t.Errorf("PlanRetry called %d times, want 1", got)
	}
	if got := pusher.callN(); got != 1 {
		t.Errorf("Push called %d times, want 1", got)
	}

	// 实体状态被正确迁移
	if exec.Status() != execution.StatusRetrying {
		t.Errorf("exec.Status = %v, want Retrying", exec.Status())
	}
	if exec.NextRetryAt() == nil {
		t.Errorf("exec.NextRetryAt = nil, want set")
	}
	if exec.RetryCount() != 1 {
		t.Errorf("exec.RetryCount = %d, want 1", exec.RetryCount())
	}

	// 重投契约：同一 execution_id，携带任务快照
	ids := pusher.pushedIDs()
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("pushed ExecIDs = %v, want [1]（重投同一 execution_id）", ids)
	}
}

// ===========================================================================
// 2. 重试耗尽判终局：retryCount >= maxRetry → MarkFailedFinal，不投递
//
//   注：真实 repo 的 ScanRetryable 带 retry_count<max_retry 过滤，
//   正常情况下捞不到耗尽的行；这个分支兜的是"策略改小 max_retry"
//   之类的边缘态，用 fake 直接驱动它验证判定逻辑本身。
// ===========================================================================

func TestPlanOnce_RetryExhausted(t *testing.T) {
	exec := mkFailedExec(t, 2, 100, 3, 3) // 3/3，耗尽
	tk := mkTask(t, 100, 3, time.Minute)

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{exec},
	}
	src := &fakeTaskSource{listResult: []*task.Task{tk}}
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)
	p.planOnce(context.Background())

	if got := len(repo.finalMarked); got != 1 {
		t.Errorf("MarkFailedFinal called %d times, want 1", got)
	}
	if got := len(repo.planCalls); got != 0 {
		t.Errorf("PlanRetry called %d times, want 0", got)
	}
	if got := pusher.callN(); got != 0 {
		t.Errorf("Push called %d times, want 0", got)
	}

	if exec.Status() != execution.StatusFailedFinal {
		t.Errorf("exec.Status = %v, want FailedFinal", exec.Status())
	}
}

// ===========================================================================
// 3. 任务已删除（taskByID 中不存在）→ 仍走 PlanRetry 但不 Push
//    （当前实现并未将"缺任务"当作终局，测试如实反映这一行为）
// ===========================================================================

func TestPlanOnce_TaskMissing(t *testing.T) {
	exec := mkFailedExec(t, 3, 999, 0, 3) // taskID=999，src 返回空
	tk := mkTask(t, 100, 3, time.Minute)  // 只返回 task 100

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{exec},
		planResult: true,
	}
	src := &fakeTaskSource{listResult: []*task.Task{tk}} // 缺 999
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)
	p.planOnce(context.Background())

	// 当前实现：仍会调 PlanRetry（backoff 用默认 1 分钟）
	if got := len(repo.planCalls); got != 1 {
		t.Errorf("PlanRetry called %d times, want 1 (task missing 仍规划)", got)
	}
	// 但因为没有 Task 对象，跳过 Push
	if got := pusher.callN(); got != 0 {
		t.Errorf("Push called %d times, want 0 (task missing 不投递)", got)
	}
	// 也不会判终局
	if got := len(repo.finalMarked); got != 0 {
		t.Errorf("MarkFailedFinal called %d times, want 0", got)
	}
}

// ===========================================================================
// 4. CAS 失败跳过：PlanRetry 返回 false → 不 Push
// ===========================================================================

func TestPlanOnce_CASFailure(t *testing.T) {
	exec := mkFailedExec(t, 4, 100, 0, 3)
	tk := mkTask(t, 100, 3, time.Minute)

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{exec},
		planResult: false, // 被其他 planner 抢先
	}
	src := &fakeTaskSource{listResult: []*task.Task{tk}}
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)
	p.planOnce(context.Background())

	if got := len(repo.planCalls); got != 1 {
		t.Errorf("PlanRetry called %d times, want 1", got)
	}
	if got := pusher.callN(); got != 0 {
		t.Errorf("Push called %d times, want 0 (CAS 失败不投递)", got)
	}
	if got := len(repo.finalMarked); got != 0 {
		t.Errorf("MarkFailedFinal called %d times, want 0", got)
	}
}

// ===========================================================================
// 5. ScanRetryable 返回错误 → 直接 return，不做后续操作
// ===========================================================================

func TestPlanOnce_ScanError(t *testing.T) {
	repo := &fakeRetryPlannerRepo{
		scanErr: errors.New("db down"),
	}
	src := &fakeTaskSource{}
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)
	p.planOnce(context.Background())

	if got := len(repo.planCalls); got != 0 {
		t.Errorf("PlanRetry called %d times, want 0", got)
	}
	if got := pusher.callN(); got != 0 {
		t.Errorf("Push called %d times, want 0", got)
	}
	if got := len(repo.finalMarked); got != 0 {
		t.Errorf("MarkFailedFinal called %d times, want 0", got)
	}
}

// ===========================================================================
// 6. ListByIDs 返回错误 → 直接 return
// ===========================================================================

func TestPlanOnce_ListTasksError(t *testing.T) {
	exec := mkFailedExec(t, 6, 100, 0, 3)

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{exec},
	}
	src := &fakeTaskSource{listErr: errors.New("task db down")}
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)
	p.planOnce(context.Background())

	if got := len(repo.planCalls); got != 0 {
		t.Errorf("PlanRetry called %d times, want 0", got)
	}
	if got := pusher.callN(); got != 0 {
		t.Errorf("Push called %d times, want 0", got)
	}
}

// ===========================================================================
// 7. 空扫描结果 → 提前 return，不调用 ListByIDs 后续
// ===========================================================================

func TestPlanOnce_NoExecs(t *testing.T) {
	repo := &fakeRetryPlannerRepo{scanResult: nil}
	src := &fakeTaskSource{}
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)
	p.planOnce(context.Background())

	if got := repo.scanCallN; got != 1 {
		t.Errorf("ScanRetryable called %d times, want 1", got)
	}
	if got := len(repo.planCalls); got != 0 {
		t.Errorf("PlanRetry called %d times, want 0", got)
	}
	if got := pusher.callN(); got != 0 {
		t.Errorf("Push called %d times, want 0", got)
	}
}

// ===========================================================================
// 8. MarkFailedFinal 持久化失败：不影响其他 exec 的继续处理
// ===========================================================================

func TestPlanOnce_FinalPersistError_Continues(t *testing.T) {
	// 两条：一条耗尽（会判终局），一条正常（会重投）
	exhausted := mkFailedExec(t, 10, 100, 3, 3)
	normal := mkFailedExec(t, 11, 100, 0, 3)
	tk := mkTask(t, 100, 3, time.Minute)

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{exhausted, normal},
		planResult: true,
		finalErr:   errors.New("final persist failed"),
	}
	src := &fakeTaskSource{listResult: []*task.Task{tk}}
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)
	p.planOnce(context.Background())

	// 耗尽那一条 persist 失败，但循环继续；正常那一条仍被 PlanRetry
	if got := len(repo.planCalls); got != 1 {
		t.Errorf("PlanRetry called %d times, want 1 (只针对 normal)", got)
	}
	if got := pusher.callN(); got != 1 {
		t.Errorf("Push called %d times, want 1 (只针对 normal)", got)
	}
	if got := len(repo.finalMarked); got != 1 {
		t.Errorf("MarkFailedFinal recorded %d times, want 1", got)
	}
	// normal 被正确迁移
	if normal.Status() != execution.StatusRetrying {
		t.Errorf("normal.Status = %v, want Retrying", normal.Status())
	}
}

// ===========================================================================
// 9. 混合场景：耗尽 + 正常 + CAS 失败 + 任务缺失
// ===========================================================================

func TestPlanOnce_Mixed(t *testing.T) {
	exhausted := mkFailedExec(t, 20, 100, 3, 3)   // → 判终局
	normal := mkFailedExec(t, 21, 100, 0, 3)      // → PlanRetry + Push
	taskMissing := mkFailedExec(t, 22, 999, 0, 3) // → PlanRetry，但不 Push

	tk := mkTask(t, 100, 3, time.Minute)

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{exhausted, normal, taskMissing},
		planResult: true, // 所有 CAS 都成功
	}
	src := &fakeTaskSource{listResult: []*task.Task{tk}} // 只有 100
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)
	p.planOnce(context.Background())

	if got := len(repo.finalMarked); got != 1 {
		t.Errorf("MarkFailedFinal called %d times, want 1", got)
	}
	if got := len(repo.planCalls); got != 2 { // normal + taskMissing
		t.Errorf("PlanRetry called %d times, want 2", got)
	}
	ids := pusher.pushedIDs()
	if len(ids) != 1 || ids[0] != 21 { // 只有 normal 有 Task
		t.Errorf("pushed ExecIDs = %v, want [21]", ids)
	}
}

// ===========================================================================
// 10. 退避计算：使用 Task 的 RetryPolicy，而不是硬编码 1 分钟
// ===========================================================================

func TestPlanOnce_UsesRetryPolicyBackoff(t *testing.T) {
	exec := mkFailedExec(t, 30, 100, 0, 3)
	// 指数退避，base=2s：NextDelay(0) = 2s
	policy := mustPolicy(t, 3, task.StrategyExponential, 2*time.Second)
	tk := mkTaskWithPolicy(t, 100, policy)

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{exec},
		planResult: true,
	}
	src := &fakeTaskSource{listResult: []*task.Task{tk}}
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)

	before := time.Now()
	p.planOnce(context.Background())
	after := time.Now()

	wantMin := before.Add(2 * time.Second)
	wantMax := after.Add(2 * time.Second)

	got := exec.NextRetryAt()
	if got == nil {
		t.Fatal("NextRetryAt is nil")
	}
	if got.Before(wantMin) || got.After(wantMax) {
		t.Errorf("NextRetryAt = %v, want in [%v, %v]", got, wantMin, wantMax)
	}
}

// ===========================================================================
// 10b. 指数退避随 retryCount 增长：retryCount=2 → base<<2 = 8s
// ===========================================================================

func TestPlanOnce_ExponentialBackoffGrows(t *testing.T) {
	exec := mkFailedExec(t, 31, 100, 2, 5) // 已重试 2 次
	policy := mustPolicy(t, 5, task.StrategyExponential, 2*time.Second)
	tk := mkTaskWithPolicy(t, 100, policy)

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{exec},
		planResult: true,
	}
	src := &fakeTaskSource{listResult: []*task.Task{tk}}
	pusher := &fakePusher{}

	before := time.Now()
	NewPlanner(repo, src, pusher).planOnce(context.Background())
	after := time.Now()

	// NextDelay(2) = 2s << 2 = 8s
	wantMin := before.Add(8 * time.Second)
	wantMax := after.Add(8 * time.Second)
	got := exec.NextRetryAt()
	if got == nil {
		t.Fatal("NextRetryAt is nil")
	}
	if got.Before(wantMin) || got.After(wantMax) {
		t.Errorf("NextRetryAt = %v, want in [%v, %v] (base<<2)", got, wantMin, wantMax)
	}
	if exec.RetryCount() != 3 {
		t.Errorf("RetryCount = %d, want 3", exec.RetryCount())
	}
}

// ===========================================================================
// 10c. 任务缺失时退避回退默认 1 分钟
// ===========================================================================

func TestPlanOnce_TaskMissing_DefaultBackoff(t *testing.T) {
	exec := mkFailedExec(t, 32, 999, 0, 3) // task 缺失

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{exec},
		planResult: true,
	}
	src := &fakeTaskSource{listResult: nil}
	pusher := &fakePusher{}

	before := time.Now()
	NewPlanner(repo, src, pusher).planOnce(context.Background())
	after := time.Now()

	wantMin := before.Add(time.Minute)
	wantMax := after.Add(time.Minute)
	got := exec.NextRetryAt()
	if got == nil {
		t.Fatal("NextRetryAt is nil")
	}
	if got.Before(wantMin) || got.After(wantMax) {
		t.Errorf("NextRetryAt = %v, want in [%v, %v] (默认 1 分钟)", got, wantMin, wantMax)
	}
}

// ===========================================================================
// 11. PlanRetry 持久化失败：不投递
// ===========================================================================

func TestPlanOnce_PlanPersistError(t *testing.T) {
	exec := mkFailedExec(t, 40, 100, 0, 3)
	tk := mkTask(t, 100, 3, time.Minute)

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{exec},
		planErr:    errors.New("cas write failed"),
	}
	src := &fakeTaskSource{listResult: []*task.Task{tk}}
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)
	p.planOnce(context.Background())

	if got := len(repo.planCalls); got != 1 {
		t.Errorf("PlanRetry called %d times, want 1", got)
	}
	if got := pusher.callN(); got != 0 {
		t.Errorf("Push called %d times, want 0 (persist error 不投递)", got)
	}
}

// ===========================================================================
// 12. Push 失败：只记录日志，不影响循环继续
// ===========================================================================

func TestPlanOnce_PushError_Continues(t *testing.T) {
	e1 := mkFailedExec(t, 50, 100, 0, 3)
	e2 := mkFailedExec(t, 51, 100, 0, 3)
	tk := mkTask(t, 100, 3, time.Minute)

	repo := &fakeRetryPlannerRepo{
		scanResult: []*execution.Execution{e1, e2},
		planResult: true,
	}
	src := &fakeTaskSource{listResult: []*task.Task{tk}}
	pusher := &fakePusher{err: errors.New("mq down")}

	p := NewPlanner(repo, src, pusher)
	p.planOnce(context.Background())

	// 两条都尝试 Push，都失败，但循环不中断
	if got := pusher.callN(); got != 2 {
		t.Errorf("Push attempted %d times, want 2", got)
	}
	if got := len(repo.planCalls); got != 2 {
		t.Errorf("PlanRetry called %d times, want 2", got)
	}
}

// ===========================================================================
// 13. ScanRetryable 的 now/limit 传参
// ===========================================================================

func TestPlanOnce_ScanArgs(t *testing.T) {
	repo := &fakeRetryPlannerRepo{scanResult: nil}
	src := &fakeTaskSource{}
	pusher := &fakePusher{}

	p := NewPlanner(repo, src, pusher)

	before := time.Now()
	p.planOnce(context.Background())
	after := time.Now()

	if repo.scanCallN != 1 {
		t.Fatalf("ScanRetryable called %d times, want 1", repo.scanCallN)
	}
	if repo.scanLastNow.Before(before) || repo.scanLastNow.After(after) {
		t.Errorf("scan now = %v, want in [%v, %v]", repo.scanLastNow, before, after)
	}
	if repo.scanLastLim != 500 {
		t.Errorf("scan limit = %d, want 500", repo.scanLastLim)
	}
}

// ===========================================================================
// 14. RunPlanner：ctx 取消后循环退出（不触发 ticker 分支）
// ===========================================================================

func TestRunPlanner_StopsOnContextCancel(t *testing.T) {
	repo := &fakeRetryPlannerRepo{}
	src := &fakeTaskSource{}
	pusher := &fakePusher{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		NewPlanner(repo, src, pusher).RunPlanner(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
		// 正常退出
	case <-time.After(2 * time.Second):
		t.Fatal("RunPlanner did not exit after ctx cancel")
	}
	// 从未到过 ticker 周期，不应有任何扫描
	if repo.scanCallN != 0 {
		t.Errorf("ScanRetryable called %d times before first tick, want 0", repo.scanCallN)
	}
}
