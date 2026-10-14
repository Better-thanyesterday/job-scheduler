package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/task"
)

func msgWithInputs(inputs map[string]json.RawMessage) *coordination.ExecutionMessage {
	cfg, _ := json.Marshal(task.ScriptConfig{Content: "", Interpreter: "bash"})
	return &coordination.ExecutionMessage{Config: cfg, NodeInputs: inputs}
}

// 数据流契约：单上游 → stdin 拿到上游输出原文；多上游 → stdin 拿映射 JSON；env 恒为完整映射
func TestNodeInputPayload(t *testing.T) {
	a := json.RawMessage(`{"rows":3}`)
	b := json.RawMessage(`[1,2]`)
	if got := NodeInputPayload(nil); got != nil {
		t.Fatal("nil 消息应无输入")
	}
	if got := NodeInputPayload(msgWithInputs(map[string]json.RawMessage{"a": a})); string(got) != string(a) {
		t.Fatalf("单上游应直传原文, got %s", got)
	}
	got := NodeInputPayload(msgWithInputs(map[string]json.RawMessage{"a": a, "b": b}))
	if !bytes.HasPrefix(got, []byte("{")) {
		t.Fatalf("多上游应是映射 JSON, got %s", got)
	}
	if env := NodeInputsEnv(msgWithInputs(map[string]json.RawMessage{"a": a})); !strings.Contains(env, `"a"`) {
		t.Fatalf("env 应为完整映射, got %s", env)
	}
}

// 端到端（进程内）：A 的输出经 NodeInputs 进 B 的 stdin——B 用 cat 读 stdin 并回显
func TestScriptExecutorConsumesNodeInputs(t *testing.T) {
	s := NewScriptExecutor()
	upstreamOutput := `{"rows":3,"table":"orders"}`
	msg := msgWithInputs(map[string]json.RawMessage{"step-a": json.RawMessage(upstreamOutput)})
	msg.Config, _ = json.Marshal(task.ScriptConfig{Content: "cat", Interpreter: "bash"})
	res, err := s.Execute(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	var detail struct {
		Output string `json:"output"`
	}
	var r struct {
		Detail struct {
			Output string `json:"output"`
		} `json:"detail"`
	}
	if err := json.Unmarshal(res, &r); err != nil {
		t.Fatal(err)
	}
	_ = detail
	if !strings.Contains(r.Detail.Output, "rows") || !strings.Contains(r.Detail.Output, "orders") {
		t.Fatalf("B 的 stdin 应为 A 的输出原文, got %s", r.Detail.Output)
	}
}

func TestScriptExecutorEnvInjection(t *testing.T) {
	s := NewScriptExecutor()
	msg := msgWithInputs(map[string]json.RawMessage{"a": json.RawMessage(`{"n":1}`)})
	msg.Config, _ = json.Marshal(task.ScriptConfig{Content: "echo $JOB_NODE_INPUTS", Interpreter: "bash"})
	res, err := s.Execute(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Detail struct {
			Output string `json:"output"`
		} `json:"detail"`
	}
	if err := json.Unmarshal(res, &r); err != nil {
		t.Fatal(err)
	}
	// env 里是完整映射 JSON（不是单上游的原文直传）
	if !strings.Contains(r.Detail.Output, `"a"`) || !strings.Contains(r.Detail.Output, `"n":1`) {
		t.Fatalf("env 注入缺失, got %s", r.Detail.Output)
	}
}
