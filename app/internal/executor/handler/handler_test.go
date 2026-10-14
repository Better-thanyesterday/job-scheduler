package handler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/executor/executor"
)

// 端到端验证"A→B 直传与溢出信封并存"：上游输出是溢出信封时，
// handler 在执行前回源成全量原文——B 的 stdin 拿到的是 A 的真实输出而非信封元数据。
// 用 bash `cat` 当执行器：stdin 原样进 stdout，执行结果里就能断言输入内容

// ---------- 测试替身 ----------

type fakeClaim struct {
	claimed  bool
	terminal *execution.Execution
}

func (f *fakeClaim) ClaimPending(ctx context.Context, id int64, workerID string, now time.Time) (bool, error) {
	f.claimed = true
	return true, nil
}
func (f *fakeClaim) UpdateTerminal(ctx context.Context, e *execution.Execution) error {
	f.terminal = e
	return nil
}

// fakeStorer 记录回源调用；信封 → 预置全量原文，非信封透传
type fakeStorer struct {
	loaded    [][]byte
	fullBytes string
}

func (f *fakeStorer) Store(ctx context.Context, execID int64, result []byte) ([]byte, error) {
	return json.Marshal(map[string]any{"storage": "minio", "key": "executions/1/result.json.gz"})
}
func (f *fakeStorer) Load(ctx context.Context, resultOrEnvelope []byte) ([]byte, error) {
	f.loaded = append(f.loaded, resultOrEnvelope)
	var m map[string]any
	if err := json.Unmarshal(resultOrEnvelope, &m); err != nil {
		return resultOrEnvelope, nil
	}
	if _, ok := m["storage"]; ok {
		return []byte(f.fullBytes), nil
	}
	return resultOrEnvelope, nil
}

type failLoadStorer struct{}

func (f *failLoadStorer) Store(ctx context.Context, execID int64, result []byte) ([]byte, error) {
	return nil, errors.New("not used")
}
func (f *failLoadStorer) Load(ctx context.Context, resultOrEnvelope []byte) ([]byte, error) {
	return nil, errors.New("minio unreachable")
}

// ---------- 用例 ----------

func dagMsg(execID int64, inputs json.RawMessage) *coordination.ExecutionMessage {
	return &coordination.ExecutionMessage{
		ExecID: execID, TaskID: 1, TaskType: 2, MaxRetry: 1,
		TimeoutSec: 10, TraceID: "t",
		Config:     json.RawMessage(`{"interpreter":"bash","content":"cat"}`),
		NodeInputs: map[string]json.RawMessage{"a": inputs},
	}
}

// 上游输出是溢出信封：执行前必须回源，stdin 拿到全量原文
func TestHandleResolvesEnvelopeInputs(t *testing.T) {
	claim := &fakeClaim{}
	storer := &fakeStorer{fullBytes: "UPSTREAM-FULL-OUTPUT"}
	h := NewHandler(claim, executor.Registry{2: executor.NewScriptExecutor()}, "worker-test", nil)
	h.EnableSpill(storer, 32<<10)

	out, err := h.Handle(context.Background(), dagMsg(42, envelopeJSON("executions/41/result.json.gz")))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !out.Claimed || !out.Success {
		t.Fatalf("执行应成功: %+v err=%v", out, err)
	}
	if len(storer.loaded) != 1 {
		t.Fatalf("信封应被回源恰好一次，实际 %d", len(storer.loaded))
	}
	if claim.terminal == nil ||
		!strings.Contains(string(claim.terminal.Result()), "UPSTREAM-FULL-OUTPUT") {
		t.Fatalf("脚本输出应含回源后的全量原文，实际: %s",
			string(claim.terminal.Result()))
	}
}

// 非信封输入：原样透传，不触发回源（既有 A→B 主路径零影响）
func TestHandlePassthroughInlineInputs(t *testing.T) {
	claim := &fakeClaim{}
	storer := &fakeStorer{fullBytes: "SHOULD-NOT-BE-USED"}
	h := NewHandler(claim, executor.Registry{2: executor.NewScriptExecutor()}, "worker-test", nil)
	h.EnableSpill(storer, 32<<10)

	out, err := h.Handle(context.Background(), dagMsg(43, json.RawMessage(`"inline-upstream-output"`)))
	if err != nil || !out.Success {
		t.Fatalf("执行应成功: %+v err=%v", out, err)
	}
	if len(storer.loaded) != 0 {
		t.Fatal("非信封输入不应触发回源")
	}
	if !strings.Contains(string(claim.terminal.Result()), "inline-upstream-output") {
		t.Fatalf("inline 输入应原样进 stdin: %s", claim.terminal.Result())
	}
}

// 回源失败（MinIO 不可达等）：按执行失败写终态，交 planner 按策略重试；
// 绝不能返回 infra err——本行已 Running，重投会被 ClaimPending 幂等丢弃
func TestHandleResolveFailureFailsExecution(t *testing.T) {
	claim := &fakeClaim{}
	storer := &failLoadStorer{}
	h := NewHandler(claim, executor.Registry{2: executor.NewScriptExecutor()}, "worker-test", nil)
	h.EnableSpill(storer, 32<<10)

	out, err := h.Handle(context.Background(), dagMsg(44, envelopeJSON("executions/41/result.json.gz")))
	if err != nil {
		t.Fatalf("回源失败应是执行失败而非 infra err: %v", err)
	}
	if out.Success || !strings.Contains(out.ErrorMsg, "resolve upstream outputs") {
		t.Fatalf("应返回带回源原因的执行失败: %+v", out)
	}
	if claim.terminal == nil || claim.terminal.Status() != execution.StatusFailed {
		t.Fatalf("应写 Failed 终态: %+v", claim.terminal)
	}
}

// 未启用溢出（spill=nil）但消息里出现信封：配置漂移，按执行失败处理，
// 报错指明配置问题而不是拿元数据跑出错误结果
func TestHandleEnvelopeWithoutSpillFailsExecution(t *testing.T) {
	claim := &fakeClaim{}
	h := NewHandler(claim, executor.Registry{2: executor.NewScriptExecutor()}, "worker-test", nil)

	out, err := h.Handle(context.Background(), dagMsg(45, envelopeJSON("executions/41/result.json.gz")))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out.Success || !strings.Contains(out.ErrorMsg, "未启用结果存储") {
		t.Fatalf("应报配置漂移失败: %+v", out)
	}
}

func envelopeJSON(key string) json.RawMessage {
	return json.RawMessage(`{"storage":"minio","bucket":"b","key":"` + key +
		`","size":10,"orig_size":64,"sha256":"x","preview":"...","stored_at":"2026-01-01T00:00:00Z","ttl_days":30}`)
}

// 降级路径（溢出上传失败）：产物必须是合法 JSON——
// 头尾按字节切可能切在转义序列/多字节字符中间，裸拼曾产出非法 JSON
// 导致 JSON 列写入失败、执行卡 Running 等 reaper
func TestHandleSpillFailureProducesValidJSON(t *testing.T) {
	claim := &fakeClaim{}
	failStore := &failStoreStorer{}
	h := NewHandler(claim, executor.Registry{2: executor.NewScriptExecutor()}, "worker-test", nil)
	h.EnableSpill(failStore, 1024) // 小阈值强制触发溢出路径

	// 产出带多字节字符 + 引号转义的大结果，头尾切大概率落在字节序列中间
	bigScript := `printf '` + strings.Repeat("中文\"quote\\", 200) + `'`
	msg := &coordination.ExecutionMessage{
		ExecID: 46, TaskID: 1, TaskType: 2, MaxRetry: 1,
		TimeoutSec: 10, TraceID: "t",
		Config: json.RawMessage(`{"interpreter":"bash","content":` + mustJSON(bigScript) + `}`),
	}
	out, err := h.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !out.Success {
		t.Fatalf("降级后执行仍应成功: %+v", out)
	}
	if claim.terminal == nil {
		t.Fatal("应有终态")
	}
	var decoded map[string]any
	if err := json.Unmarshal(claim.terminal.Result(), &decoded); err != nil {
		t.Fatalf("降级产物必须是合法 JSON: %v\nraw=%s", err, claim.terminal.Result())
	}
	if decoded["degraded"] != true {
		t.Fatalf("降级产物应带 degraded 标记: %v", decoded)
	}
}

// 溢出成功路径：信封入列
func TestHandleSpillSuccessStoresEnvelope(t *testing.T) {
	claim := &fakeClaim{}
	storer := &fakeStorer{fullBytes: "ignored"}
	h := NewHandler(claim, executor.Registry{2: executor.NewScriptExecutor()}, "worker-test", nil)
	h.EnableSpill(storer, 16) // 极小阈值强制溢出

	msg := &coordination.ExecutionMessage{
		ExecID: 47, TaskID: 1, TaskType: 2, MaxRetry: 1,
		TimeoutSec: 10, TraceID: "t",
		Config: json.RawMessage(`{"interpreter":"bash","content":"echo spilled"}`),
	}
	out, err := h.Handle(context.Background(), msg)
	if err != nil || !out.Success {
		t.Fatalf("执行应成功: %+v err=%v", out, err)
	}
	if len(storer.loaded) != 0 {
		t.Fatal("Store 路径不应触发 Load")
	}
	var decoded map[string]any
	if err := json.Unmarshal(claim.terminal.Result(), &decoded); err != nil {
		t.Fatalf("信封必须是合法 JSON: %v", err)
	}
	if decoded["storage"] != "minio" {
		t.Fatalf("result 列应落信封: %s", claim.terminal.Result())
	}
}

// failStoreStorer Store 恒失败（触发降级路径）
type failStoreStorer struct{}

func (f *failStoreStorer) Store(ctx context.Context, execID int64, result []byte) ([]byte, error) {
	return nil, errors.New("minio put failed")
}
func (f *failStoreStorer) Load(ctx context.Context, resultOrEnvelope []byte) ([]byte, error) {
	return resultOrEnvelope, nil
}

// mustJSON 把字符串编码成 JSON 字符串字面量（安全嵌入 Content）
func mustJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
