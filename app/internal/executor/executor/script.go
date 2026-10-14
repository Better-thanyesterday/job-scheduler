package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/task"
)

// ScriptExecutor 执行脚本任务：interpreter -c 内容。
// sh/bash/python3 都支持 -c 传入内联脚本，免去落临时文件的清理问题。
// 超时与取消由 ctx 兜底（handler 按 timeout_sec 包了 WithTimeout）。
// maxOutput 输出上限字节：>0 时超限截断（无溢出存储的传统模式）；
// 0 = 返回全量，由 handler 做三段式溢出决策
type ScriptExecutor struct {
	maxOutput int
}

func NewScriptExecutor() *ScriptExecutor { return &ScriptExecutor{maxOutput: maxDetailLen} }

func (s *ScriptExecutor) Execute(ctx context.Context, msg *coordination.ExecutionMessage) ([]byte, error) {
	var cfg task.ScriptConfig
	if err := json.Unmarshal(msg.Config, &cfg); err != nil {
		return nil, fmt.Errorf("executor: script config unmarshal: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("executor: script config invalid: %w", err)
	}

	start := time.Now()
	cmd := exec.CommandContext(ctx, cfg.Interpreter, "-c", cfg.Content)
	// 数据流注入：上游输出进 stdin（单上游直传原文）+ JOB_NODE_INPUTS 环境变量（完整映射）。
	// 无输入时 stdin 留空、不设环境变量
	if payload := NodeInputPayload(msg); payload != nil {
		cmd.Stdin = bytes.NewReader(payload)
	}
	if envVal := NodeInputsEnv(msg); envVal != "" {
		cmd.Env = append(os.Environ(), EnvNodeInputs+"="+envVal)
	}
	// 脚本可能再起子进程（如 sleep/管道），ctx 到期只杀直接子进程不够，
	// 子进程会一直占着 stdout 管道让 Run 卡到天荒地老——按进程树清理
	cmd.SysProcAttr = processGroupSysProcAttr()
	cmd.Cancel = func() error { return killProcessTree(cmd.Process) }
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("executor: script run: %w\n%s", err, truncate(out.String()))
	}
	return marshalResult(execResult{
		OK:         true,
		DurationMS: since(start),
		Detail: scriptDetail{
			Output: clampOutput(out.String(), s.maxOutput),
		},
	}), nil
}

// clampOutput 按上限截断；cap<=0 表示不设限（溢出模式下全量交给 handler 决策）
func clampOutput(s string, cap int) string {
	if cap > 0 && len(s) > cap {
		return s[:cap] + "...(truncated)"
	}
	return s
}

type scriptDetail struct {
	Output string `json:"output"`
}
