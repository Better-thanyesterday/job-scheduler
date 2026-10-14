package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/task"
)

// HTTPExecutor 执行 HTTP 任务：消息里的 config 是请求快照，原样执行，不回查任务表。
// 超时双层兜底：外层 ctx（handler 按 timeout_sec 设置）+ 快照里的 timeout 覆盖值
type HTTPExecutor struct {
	client *http.Client
}

func NewHTTPExecutor() *HTTPExecutor {
	return &HTTPExecutor{client: &http.Client{}}
}

// maxResponseBody 读入内存的响应体上限，更大只读一部分
const maxResponseBody = 1 << 20 // 1MB

func (e *HTTPExecutor) Execute(ctx context.Context, msg *coordination.ExecutionMessage) ([]byte, error) {
	var cfg task.HTTPConfig
	if err := json.Unmarshal(msg.Config, &cfg); err != nil {
		return nil, fmt.Errorf("executor: http config unmarshal: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("executor: http config invalid: %w", err)
	}

	start := time.Now()
	// 数据流注入：DAG 节点且快照 body 为空时，上游输出充当请求体
	//（单上游直传原文，多上游传映射 JSON）。显式配置的 body 优先
	body := cfg.Body
	if body == "" {
		if payload := NodeInputPayload(msg); payload != nil {
			body = string(payload)
		}
	}
	req, err := http.NewRequestWithContext(ctx, cfg.Method, cfg.URL, bytes.NewReader([]byte(body)))
	if err != nil {
		return nil, fmt.Errorf("executor: http build request: %w", err)
	}
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}

	client := e.client
	if cfg.TimeoutOverride > 0 {
		client = &http.Client{Timeout: cfg.TimeoutOverride}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executor: http request: %w", err)
	}
	defer resp.Body.Close()

	// 超 1MB 显式判失败（静默截断会把不完整响应当成功结果，污染 A→B 数据流）
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody+1))
	if err != nil {
		return nil, fmt.Errorf("executor: http read body: %w", err)
	}
	if len(raw) > maxResponseBody {
		return nil, fmt.Errorf("executor: http response exceeds %d bytes", maxResponseBody)
	}

	// 状态码不符合配置的预期 → 本次执行失败（ExpectedCodes 空则默认 2xx）
	if !cfg.IsSuccess(resp.StatusCode) {
		return nil, fmt.Errorf("executor: http unexpected status %d: %s",
			resp.StatusCode, truncate(string(raw)))
	}
	return marshalResult(execResult{
		OK:         true,
		DurationMS: since(start),
		Detail: httpDetail{
			Status: resp.StatusCode,
			Body:   truncate(string(raw)),
		},
	}), nil
}

type httpDetail struct {
	Status int    `json:"status_code"`
	Body   string `json:"body"`
}
