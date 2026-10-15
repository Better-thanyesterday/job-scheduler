// Package handler 一条执行消息的完整生命周期。
//
// gRPC 推送模式：返回 (Outcome, nil) = 送达成功（含执行失败）；
// 返回 (Outcome{}, err) = executor 侧基础设施故障，推送方换实例重投，
// ClaimPending 原子抢票保证幂等
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/executor/executor"
	"job-scheduler/app/internal/executor/resultstore"
	"job-scheduler/app/pkg/metrics"

	"go.uber.org/zap"
)

// Outcome 一次推送的处理结果（映射到 gRPC ExecuteResponse）
type Outcome struct {
	Claimed  bool   // false = 重复投递/状态非法，调用方无需重试
	Success  bool   // 执行终态是否成功（仅 Claimed=true 时有意义）
	ErrorMsg string // 失败摘要（Success=false 时）
}

// ResultNotifier 结果回执端口：终态落库后 best-effort 回调一次，
// 失败由实现方消化（回执丢失不影响正确性，DB 终态是事实源）。nil 时静默跳过
type ResultNotifier interface {
	NotifyResult(ctx context.Context, r *coordination.ExecutionResult)
}

type Handler struct {
	claim    execution.ExecutorRepo
	exec     executor.Registry
	workerID string
	running  atomic.Int32
	notifier ResultNotifier
	// spill 溢出存储：结果超过 maxInlineBytes 时直传对象存储，result 列落信封。
	// nil = 不启用溢出
	spill          ResultStorer
	maxInlineBytes int
}

// defaultInlineMaxBytes 结果内联阈值：超过即走溢出存储
const defaultInlineMaxBytes = 32 << 10 // 32KB

// byteHead / byteTail 按字节切头尾。切点可能落在多字节字符/转义序列中间，
// 产物必须经 json.Marshal 消费才是合法 JSON
func byteHead(s string, each int) string {
	if len(s) <= each {
		return s
	}
	return s[:each]
}

func byteTail(s string, each int) string {
	if len(s) <= each {
		return s
	}
	return s[len(s)-each:]
}

// degradedResult 溢出上传失败时的降级产物：自描述摘要（写 JSON 列不炸）
type degradedResult struct {
	Degraded bool   `json:"degraded"`
	Reason   string `json:"reason"`
	Head     string `json:"head"`
	Tail     string `json:"tail"`
	OrigSize int    `json:"orig_size"`
}

// ResultStorer 结果溢出存储端口（resultstore.Store 实现）：
// Store 上传全量结果返回信封；Load 信封回源全量原文，非信封原样透传
type ResultStorer interface {
	Store(ctx context.Context, execID int64, result []byte) (envelope []byte, err error)
	Load(ctx context.Context, resultOrEnvelope []byte) ([]byte, error)
}

func NewHandler(claim execution.ExecutorRepo, exec executor.Registry, workerID string,
	notifier ResultNotifier) *Handler {
	return &Handler{claim: claim, exec: exec, workerID: workerID, notifier: notifier,
		maxInlineBytes: defaultInlineMaxBytes}
}

// EnableSpill 启用结果溢出存储。storer 为 nil 时不启用
func (h *Handler) EnableSpill(storer ResultStorer, maxInlineBytes int) {
	if maxInlineBytes <= 0 {
		maxInlineBytes = defaultInlineMaxBytes
	}
	h.spill = storer
	h.maxInlineBytes = maxInlineBytes
}

// Running 当前执行中的任务数（etcd 状态上报读取）
func (h *Handler) Running() int32 { return h.running.Load() }

// Handle 处理一条推送。返回 err 仅代表 executor 侧基础设施故障（需重投），
// 执行本身的成败一律走 Outcome + 终态落库
func (h *Handler) Handle(ctx context.Context, msg *coordination.ExecutionMessage) (Outcome, error) {
	now := time.Now()

	ok, err := h.claim.ClaimPending(ctx, msg.ExecID, h.workerID, now)
	if err != nil {
		return Outcome{}, err
	}
	if !ok {
		return Outcome{Claimed: false}, nil // 已被认领/状态非法 → 幂等丢弃
	}
	h.running.Add(1)
	defer func() {
		h.running.Add(-1)
		metrics.SetExecutorRunning(float64(h.running.Load()))
	}()
	metrics.SetExecutorRunning(float64(h.running.Load()))
	if !msg.EnqueuedAt.IsZero() {
		metrics.ObserveResultQueueWait(time.Since(msg.EnqueuedAt).Seconds())
	}

	// 重建实体（行已由调度侧登记，不能走 execution.New——那会另发新 id）+ 流转到 Running
	maxRetry := int(msg.MaxRetry)
	if maxRetry < 0 {
		maxRetry = 0
	}
	e, err := execution.Rebuild(msg.ExecID, msg.TaskID, msg.ScheduleID, msg.DagExecID, msg.DagNodeID,
		execution.StatusPending, 0, time.Time{}, time.Time{}, 0, nil, "",
		0, maxRetry, "", msg.TraceID, nil, now)
	if err != nil {
		// 实体建不出来只能等 reaper 收割；重投也救不了非法快照
		return Outcome{Claimed: true, Success: false, ErrorMsg: "invalid execution snapshot: " + err.Error()}, nil
	}
	if err := e.Claim(h.workerID, now); err != nil {
		return Outcome{Claimed: true, Success: false, ErrorMsg: err.Error()}, nil
	}

	// 上游输入回源：上游输出可能是溢出信封，喂给执行器前统一还原成全量原文。
	// 失败按执行失败处理交 planner 重试（不走 infra err：本行已 Running，重投会被幂等丢弃）
	if len(msg.NodeInputs) > 0 {
		if err := h.resolveInputs(ctx, msg); err != nil {
			errMsg := "resolve upstream outputs: " + err.Error()
			_ = e.FinishFailed(errMsg, time.Now())
			metrics.IncTaskExecuted("failed")
			if ferr := h.finish(ctx, e); ferr != nil {
				return Outcome{}, ferr
			}
			h.notifyResult(msg, e, false, coordination.FailureClassExecution, errMsg)
			return Outcome{Claimed: true, Success: false, ErrorMsg: errMsg}, nil
		}
	}

	execCtx, cancel := context.WithTimeout(ctx, time.Duration(msg.TimeoutSec)*time.Second)
	defer cancel()

	ex, ok := h.exec[msg.TaskType]
	if !ok {
		errMsg := fmt.Sprintf("unknown task_type %d", msg.TaskType)
		_ = e.FinishFailed(errMsg, time.Now())
		metrics.IncTaskExecuted("failed")
		metrics.ObserveTaskDuration(msg.TaskType, time.Since(now).Seconds())
		if err := h.finish(ctx, e); err != nil {
			return Outcome{}, err
		}
		h.notifyResult(msg, e, false, coordination.FailureClassExecution, errMsg)
		return Outcome{Claimed: true, Success: false, ErrorMsg: errMsg}, nil
	}
	result, execErr := ex.Execute(execCtx, msg)

	// 结果溢出：超阈值的全量结果直传对象存储，result 列落信封。
	// 只对成功结果溢出；上传失败降级为合法 JSON 摘要（执行不受影响）
	if execErr == nil && h.spill != nil && len(result) > h.maxInlineBytes {
		envelope, err := h.spill.Store(ctx, msg.ExecID, result)
		if err != nil {
			zap.L().Warn("结果溢出上传失败，降级为 degraded 摘要",
				zap.Int64("execution_id", msg.ExecID),
				zap.Int("size", len(result)), zap.Error(err))
			// 头尾按字节切可能落在转义序列/多字节字符中间，必须走 Marshal 产合法 JSON
			degraded, derr := json.Marshal(degradedResult{
				Degraded: true,
				Reason:   "resultstore upload failed: " + err.Error(),
				Head:     byteHead(string(result), h.maxInlineBytes/2),
				Tail:     byteTail(string(result), h.maxInlineBytes/2),
				OrigSize: len(result),
			})
			if derr != nil {
				degraded = []byte(`{"degraded":true,"error":"marshal degraded result failed"}`)
			}
			metrics.IncResultSpillDegraded()
			result = degraded
		} else {
			metrics.IncResultSpill()
			result = envelope
		}
	}

	// 写终态。失败按成因分类：ctx 到点 = 超时，其余 = 执行失败
	failureClass := coordination.FailureClassExecution
	if execErr != nil {
		if errors.Is(execErr, context.DeadlineExceeded) {
			failureClass = coordination.FailureClassTimeout
		}
		if err := e.FinishFailed(execErr.Error(), time.Now()); err == nil {
			metrics.IncTaskExecuted("failed")
		}
	} else {
		if err := e.FinishSuccess(result, time.Now()); err == nil {
			metrics.IncTaskExecuted("success")
		}
	}
	metrics.ObserveTaskDuration(msg.TaskType, time.Since(now).Seconds())

	// 终态没写成（DB 抖动）→ 返回 err 让调用方重投
	if err := h.finish(ctx, e); err != nil {
		return Outcome{}, err
	}

	h.notifyResult(msg, e, execErr == nil, failureClass, errMsgOf(execErr))
	return Outcome{Claimed: true, Success: execErr == nil, ErrorMsg: errMsgOf(execErr)}, nil
}

// notifyResult best-effort 结果回执；notifier 为 nil 时静默跳过
func (h *Handler) notifyResult(msg *coordination.ExecutionMessage, e *execution.Execution,
	success bool, failureClass string, errMsg string) {
	if h.notifier == nil {
		return
	}
	h.notifier.NotifyResult(context.Background(), &coordination.ExecutionResult{
		ExecID:       e.ID(),
		TaskID:       e.TaskID(),
		ScheduleID:   e.ScheduleID(),
		DagExecID:    e.DagExecID(),
		DagNodeID:    e.DagNodeID(),
		Success:      success,
		FailureClass: failureClass,
		ErrorMsg:     errMsg,
		DurationMS:   e.DurationMS(),
		WorkerID:     h.workerID,
		TraceID:      e.TraceID(),
		FinishedAt:   time.Now(),
	})
}

func (h *Handler) finish(ctx context.Context, e *execution.Execution) error {
	return h.claim.UpdateTerminal(ctx, e)
}

// resolveInputs 就地规整 NodeInputs：
//  1. 溢出信封 → 回源对象存储取全量结果（executor↔MinIO 直连，调度器不扛载荷）；
//  2. execResult 包装（{"ok":true,"detail":{...}}）→ 抽 detail.output 裸输出，
//     B 的 stdin 拿到的是上游真实产出而非包装 JSON；
//  3. 非包装形态原样保留。
// 检测到信封但未启用结果存储（配置漂移）视为执行失败——
// 拿元数据跑出错误结果比跑失败更糟
func (h *Handler) resolveInputs(ctx context.Context, msg *coordination.ExecutionMessage) error {
	for nodeID, raw := range msg.NodeInputs {
		if env, ok := resultstore.ParseEnvelope(raw); ok {
			if h.spill == nil {
				return fmt.Errorf("node %q: 上游输出是溢出信封，但本 executor 未启用结果存储（检查 executor.minio 配置）", nodeID)
			}
			full, err := h.spill.Load(ctx, raw)
			if err != nil {
				return fmt.Errorf("node %q: 回源 %s: %w", nodeID, env.Key, err)
			}
			raw = full
		}
		msg.NodeInputs[nodeID] = json.RawMessage(unwrapExecResult(raw))
	}
	return nil
}

// unwrapExecResult 上游执行器的 result 统一是 execResult 包装
// （{"ok":true,"detail":{...}}），抽取其中的"裸输出"给下游：
//   - 脚本 → detail.output（stdout 原文）
//   - HTTP → detail.body（响应体）
//   - SQL  → detail.rows（结果行 JSON）
//
// 都没有（消息任务等）→ 透传整个包装原文。
// 非 execResult 形态（纯文本/其他 JSON）同样原样透传
func unwrapExecResult(full []byte) []byte {
	var probe struct {
		OK     bool `json:"ok"`
		Detail struct {
			Output string          `json:"output"`
			Body   string          `json:"body"`
			Rows   json.RawMessage `json:"rows"`
		} `json:"detail"`
	}
	if err := json.Unmarshal(full, &probe); err != nil || !probe.OK {
		return full
	}
	switch {
	case probe.Detail.Output != "":
		return []byte(probe.Detail.Output)
	case probe.Detail.Body != "":
		return []byte(probe.Detail.Body)
	case len(probe.Detail.Rows) > 0:
		return probe.Detail.Rows
	}
	return full
}

func errMsgOf(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
