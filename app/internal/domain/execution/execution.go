package execution

import (
	"fmt"
	"job-scheduler/app/pkg/snowflake"
	"time"
)

type Execution struct {
	id         int64
	taskID     int64
	scheduleID int64 // Cron 触发才有；手动触发为 0
	dagExecID  int64 // DAG 子任务才有；普通任务为 0
	dagNodeID  string

	status     Status
	version    int64 // 乐观锁：实体字段，随每次迁移 +1
	startTime  time.Time
	endTime    time.Time
	durationMS int64
	result     []byte // 执行结果快照（JSON），由 worker 上报
	errorMsg   string
	retryCount int
	maxRetry   int
	workerID   string
	traceID    string

	nextRetryAt *time.Time //  planner 专属字段；worker 的方法根本不碰它
	createdAt   time.Time
}

// New 登记一次执行（api 手动触发 / engine Cron 触发共用）：
// 出生即 Pending，带 trace 上下文
func New(taskID, scheduleID int64, maxRetry int, traceID string, now time.Time) (*Execution, error) {
	if taskID <= 0 {
		return nil, fmt.Errorf("execution: invalid task_id %d", taskID)
	}
	if maxRetry < 0 {
		return nil, fmt.Errorf("execution: invalid max_retry %d", maxRetry)
	}
	return &Execution{
		id:     snowflake.GenID(),
		taskID: taskID, scheduleID: scheduleID,
		status: StatusPending, maxRetry: maxRetry,
		traceID: traceID, createdAt: now,
		retryCount: 0,
	}, nil
}

// Rebuild 从持久化重建（infra convert 唯一入口）
func Rebuild(id, taskID, scheduleID, dagExecID int64, dagNodeID string,
	status Status, version int64, start, end time.Time, durationMS int64,
	result []byte, errorMsg string, retryCount, maxRetry int,
	workerID, traceID string, nextRetryAt *time.Time, createdAt time.Time) (*Execution, error) {
	if !status.Valid() {
		return nil, fmt.Errorf("execution: rebuild id=%d invalid status %d", id, status)
	}
	return &Execution{
		id: id, taskID: taskID, scheduleID: scheduleID,
		dagExecID: dagExecID, dagNodeID: dagNodeID,
		status: status, version: version,
		startTime: start, endTime: end, durationMS: durationMS,
		result: result, errorMsg: errorMsg,
		retryCount: retryCount, maxRetry: maxRetry,
		workerID: workerID, traceID: traceID,
		nextRetryAt: nextRetryAt, createdAt: createdAt,
	}, nil
}

func (e *Execution) ID() int64               { return e.id }
func (e *Execution) TaskID() int64           { return e.taskID }
func (e *Execution) Status() Status          { return e.status }
func (e *Execution) RetryCount() int         { return e.retryCount }
func (e *Execution) MaxRetry() int           { return e.maxRetry }
func (e *Execution) NextRetryAt() *time.Time { return e.nextRetryAt } // planner 读取
func (e *Execution) Version() int64          { return e.version }     // repo 落库时的 CAS 依据
func (e *Execution) DurationMS() int64       { return e.durationMS }
func (e *Execution) ScheduleID() int64       { return e.scheduleID }
func (e *Execution) DagExecID() int64        { return e.dagExecID }
func (e *Execution) DagNodeID() string       { return e.dagNodeID }
func (e *Execution) TraceID() string         { return e.traceID }
func (e *Execution) StartTime() time.Time    { return e.startTime }
func (e *Execution) EndTime() time.Time      { return e.endTime } // repo 写终态读取
func (e *Execution) Result() []byte          { return e.result }  // worker 上报的结果快照
func (e *Execution) ErrorMsg() string        { return e.errorMsg }
func (e *Execution) WorkerID() string        { return e.workerID }
func (e *Execution) CreatedAt() time.Time    { return e.createdAt }

// ---------- exe 侧行为（只动 Running 和终态，不碰 next_retry_at）----------

// Claim 认领执行（对应 repo 的 ClaimPending 抢票成功后调用）
func (e *Execution) Claim(workerID string, now time.Time) error {
	if err := e.status.Transition(StatusRunning); err != nil {
		return fmt.Errorf("execution: claim id=%d: %w", e.id, err)
	}
	e.workerID = workerID
	e.startTime = now
	e.version++
	return nil
}

// FinishSuccess 执行成功
func (e *Execution) FinishSuccess(result []byte, now time.Time) error {
	if err := e.status.Transition(StatusSuccess); err != nil {
		return fmt.Errorf("execution: finish success id=%d: %w", e.id, err)
	}
	e.endTime = now
	e.durationMS = now.Sub(e.startTime).Milliseconds()
	e.result = result
	e.version++
	return nil
}

// FinishFailed 执行失败。注意：不设置 next_retry_at——那是 planner 的管辖字段，
// worker 只负责如实报告"我失败了"
func (e *Execution) FinishFailed(errMsg string, now time.Time) error {
	if err := e.status.Transition(StatusFailed); err != nil {
		return fmt.Errorf("execution: finish failed id=%d: %w", e.id, err)
	}
	e.endTime = now
	e.durationMS = now.Sub(e.startTime).Milliseconds()
	e.errorMsg = errMsg
	e.version++
	return nil
}

// AttachDAG 补挂 DAG 归属：dag 引擎派发节点时调用。
// 仅 Pending 可挂——执行一旦被认领/终态，归属再改就是篡改事实
func (e *Execution) AttachDAG(dagExecID int64, nodeID string) error {
	if e.status != StatusPending {
		return fmt.Errorf("execution: attach dag id=%d wrong status %d", e.id, e.status)
	}
	if dagExecID <= 0 || nodeID == "" {
		return fmt.Errorf("execution: attach dag invalid args dag=%d node=%q", dagExecID, nodeID)
	}
	e.dagExecID = dagExecID
	e.dagNodeID = nodeID
	return nil
}

// ---------- planner 侧行为（只有 planner 会调）----------

// PlanRetry 规划重试：返回下次重试时间，交给调用方投递。
// 重试次数是否超限的领域判定也在这——超限走 MarkFailedFinal
func (e *Execution) PlanRetry(backoff time.Duration, now time.Time) (time.Time, error) {
	if e.status.Transition(StatusRetrying) != nil {
		return time.Time{}, fmt.Errorf("execution: plan retry id=%d wrong status %d", e.id, e.status)
	}
	e.retryCount++
	next := now.Add(backoff)
	e.nextRetryAt = &next
	e.version++
	return next, nil
}

// Retryable 是否还可重试（planner 扫描后的第一道判定）
func (e *Execution) Retryable() bool {
	return e.status == StatusFailed && e.retryCount < e.maxRetry
}

// MarkFailedFinal 重试超限的终局
func (e *Execution) MarkFailedFinal(reason string, now time.Time) error {
	if err := e.status.Transition(StatusFailedFinal); err != nil {
		return fmt.Errorf("execution: mark final id=%d: %w", e.id, err)
	}
	e.errorMsg = fmt.Sprintf("%s; %s", e.errorMsg, reason)
	e.version++
	return nil
}

// ---------- reaper 侧行为 ----------

// Reap 超时收割：Running 太久未完结，强制置为 Failed（后续由 planner 决定重试）
func (e *Execution) Reap(now time.Time) error {
	if err := e.status.Transition(StatusFailed); err != nil {
		return fmt.Errorf("execution: reap id=%d: %w", e.id, err)
	}
	e.endTime = now
	e.durationMS = now.Sub(e.startTime).Milliseconds()
	if e.errorMsg == "" {
		e.errorMsg = "execution timeout (reaped)"
	}
	e.version++
	return nil
}
