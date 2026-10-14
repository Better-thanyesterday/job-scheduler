package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/task"
)

// MessagePublisher 投递任意载荷的最小端口。
// 现有 infra/mq 的 Producer 只投 *coordination.ExecutionMessage（要求 ExecID/TaskID 等
// 执行语义字段），消息任务要投的是业务载荷，语义不同，故这里单独定义端口，
// 由 cmd/wiring 层注入适配实现
type MessagePublisher interface {
	Publish(ctx context.Context, topic string, body []byte) error
}

// MessageExecutor 执行消息任务：把快照里的 payload 投到配置的 topic
type MessageExecutor struct {
	publisher MessagePublisher
}

func NewMessageExecutor(p MessagePublisher) *MessageExecutor {
	return &MessageExecutor{publisher: p}
}

func (m *MessageExecutor) Execute(ctx context.Context, msg *coordination.ExecutionMessage) ([]byte, error) {
	var cfg task.MessageConfig
	if err := json.Unmarshal(msg.Config, &cfg); err != nil {
		return nil, fmt.Errorf("executor: message config unmarshal: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("executor: message config invalid: %w", err)
	}
	if m.publisher == nil {
		return nil, fmt.Errorf("executor: message publisher not configured")
	}

	start := time.Now()
	if err := m.publisher.Publish(ctx, cfg.Topic, cfg.Payload); err != nil {
		return nil, fmt.Errorf("executor: message publish: %w", err)
	}
	return marshalResult(execResult{
		OK:         true,
		DurationMS: since(start),
		Detail: messageDetail{
			Topic: cfg.Topic,
			Bytes: len(cfg.Payload),
		},
	}), nil
}

type messageDetail struct {
	Topic string `json:"topic"`
	Bytes int    `json:"bytes"`
}
