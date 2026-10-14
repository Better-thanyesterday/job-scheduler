package executor

import (
	"encoding/json"
	"time"

	"job-scheduler/app/internal/domain/coordination"
)

// maxDetailLen 结果快照中文本字段的上限：executions.result 是 JSON 列，
// 大输出直接塞进去会撑爆数据库，统一在这里截断
const maxDetailLen = 4096

// EnvNodeInputs 脚本任务的输入注入环境变量名：值是完整的上游输出映射 JSON。
// stdin 的注入规则见 NodeInputPayload（单上游给原文，多上游给映射），
// 两者并存：stdin 服务"A→B 直传"的主路径，env 服务需要多个上游的脚本
const EnvNodeInputs = "JOB_NODE_INPUTS"

// NodeInputPayload 把消息里的上游输出集合装配成给执行器的输入字节流：
// 恰有一个上游产出 → 直传该输出原文（A→B 场景 B 的 stdin 拿到的就是 A 的输出）；
// 零个或多个上游 → 整个映射的 JSON（key = 上游 node_id）。msg 为 nil/无输入返回 nil
func NodeInputPayload(msg *coordination.ExecutionMessage) []byte {
	if msg == nil || len(msg.NodeInputs) == 0 {
		return nil
	}
	if len(msg.NodeInputs) == 1 {
		for _, v := range msg.NodeInputs {
			return v
		}
	}
	b, err := json.Marshal(msg.NodeInputs)
	if err != nil {
		return nil
	}
	return b
}

// NodeInputsEnv 组装环境变量值（完整映射 JSON）；无输入返回空串
func NodeInputsEnv(msg *coordination.ExecutionMessage) string {
	if msg == nil || len(msg.NodeInputs) == 0 {
		return ""
	}
	b, err := json.Marshal(msg.NodeInputs)
	if err != nil {
		return ""
	}
	return string(b)
}

// truncate 截断文本并标记
func truncate(s string) string {
	if len(s) > maxDetailLen {
		return s[:maxDetailLen] + "...(truncated)"
	}
	return s
}

// execResult 统一的执行结果快照结构
type execResult struct {
	OK         bool  `json:"ok"`
	DurationMS int64 `json:"duration_ms"`
	Detail     any   `json:"detail,omitempty"`
}

func marshalResult(r execResult) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		return []byte(`{"ok":false,"error":"marshal result failed"}`)
	}
	return b
}

func since(start time.Time) int64 { return time.Since(start).Milliseconds() }
