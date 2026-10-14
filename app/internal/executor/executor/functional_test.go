package executor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/task"
)

func scriptMsg(content, interpreter string) *coordination.ExecutionMessage {
	cfg, err := json.Marshal(task.ScriptConfig{Content: content, Interpreter: interpreter})
	if err != nil {
		panic(err)
	}
	return &coordination.ExecutionMessage{Config: cfg}
}

// 功能验证：脚本执行三件套——成功、失败、超时进程树击杀
func TestScriptExecutorFunctional(t *testing.T) {
	s := NewScriptExecutor()

	// 1. 成功：捕获 stdout
	res, err := s.Execute(context.Background(), scriptMsg("echo executor-functional-ok", "sh"))
	if err != nil {
		t.Fatalf("正常脚本应当执行成功: %v", err)
	}
	if res == nil {
		t.Fatal("成功时应当返回结果快照")
	}

	// 2. 失败：非零退出码必须报错，且带回输出
	if _, err := s.Execute(context.Background(), scriptMsg("echo before-fail; exit 3", "sh")); err == nil {
		t.Fatal("非零退出码应当返回 error")
	}

	// 3. 非法 config：领域校验拦截
	if _, err := s.Execute(context.Background(), scriptMsg("", "sh")); err == nil {
		t.Fatal("content 为空应当被校验拦截")
	}

	// 4. 超时：ctx 到期后进程树被击杀，Execute 快速返回而不是等满 30s
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	if _, err := s.Execute(ctx, scriptMsg("sleep 30", "sh")); err == nil {
		t.Fatal("超时脚本应当返回 error")
	}
	elapsed := time.Since(start)
	if elapsed > 10*time.Second {
		t.Fatalf("超时后 %v 才返回, 进程树击杀疑似失效（应 ~1.2s）", elapsed)
	}
}
