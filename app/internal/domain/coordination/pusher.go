package coordination

import (
	"context"
	"encoding/json"
	"time"
)

// ExecutionMessage 跨进程执行消息。
//
// 纪律：
//  1. 字段只增、不删、不改义；新增字段必须 omitempty（消费端容忍式反序列化）
//  2. config 是执行时刻的配置快照——worker 不回查 tasks 表，
//     任务被禁用/删除后，已入队执行按快照跑完
//  3. 不携带 retry_count / max_retry / retry_policy——重试是 scheduler/retry
//     的职责，消息不承载重试语义（§7.3）
type ExecutionMessage struct {
	ExecID     int64           `json:"execution_id"`
	TaskID     int64           `json:"task_id"`
	ScheduleID int64           `json:"schedule_id,omitempty"` // 手动触发为 0
	DagExecID  int64           `json:"dag_execution_id,omitempty"`
	DagNodeID  string          `json:"dag_node_id,omitempty"`
	TaskType   int32           `json:"task_type"`
	MaxRetry   int32           `json:"max_retry"`
	Config     json.RawMessage `json:"config"`   // 快照，原样透传给 executor
	Priority   int32           `json:"priority"` // 1-10，数字越小优先级越高
	TimeoutSec int32           `json:"timeout_sec"`
	EnqueuedAt time.Time       `json:"enqueued_at"` // 队列滞留监控 / reaper 超时参考
	TraceID    string          `json:"trace_id"`
	// NodeInputs DAG 节点的上游输出集合（key = 上游 node_id）。
	// 仅 DAG 节点消息携带，executor 自行决定是否消费；普通任务消息恒空
	NodeInputs map[string]json.RawMessage `json:"node_inputs,omitempty"`
}

// Validate 投递前的最后一道领域校验（pusher 实现和调用方都能依赖）
func (m *ExecutionMessage) Validate() error {
	if m.ExecID <= 0 {
		return ErrInvalidMessage
	}
	if m.TaskID <= 0 {
		return ErrInvalidMessage
	}
	if m.TaskType <= 0 {
		return ErrInvalidMessage
	}
	if len(m.Config) == 0 {
		return ErrInvalidMessage
	}
	if m.TimeoutSec <= 0 {
		m.TimeoutSec = 300 // 与 tasks 表默认值对齐，兜底不拒投
	}
	return nil
}

// Body 序列化为 JSON
func (m *ExecutionMessage) Body() ([]byte, error) {
	return json.Marshal(m)
}

// FailureClass 失败分类（结果回执的 failure_class 字段值）。
// 消费方可按类分流处理：超时多指向资源/容量问题，执行失败多指向业务问题
const (
	FailureClassTimeout   = "timeout"   // 执行超时（executor ctx 到点 / reaper 收割）
	FailureClassExecution = "execution" // 执行本身的失败
)

// ExecutionResult 执行结果回执（executor → 下游消费者，经 RabbitMQ）。
// 队列按来源分 dag / cron 两族，再按成功 / 超时失败 / 执行失败分流（路由键见 infra/mq）。
// 纪律与 ExecutionMessage 相同：字段只增、不删、不改义，新增字段必须 omitempty。
// ★ best-effort：结果通知失败不影响执行本身，消费端不应假设恰达一次；
// ★ failed 回执是"单次失败"而非"重试耗尽终局"——重试归 planner 管，
//
//	消费方按 execution_id 自行聚合，且不得拿它直接驱动任何终态判定
type ExecutionResult struct {
	ExecID       int64     `json:"execution_id"`
	TaskID       int64     `json:"task_id"`
	ScheduleID   int64     `json:"schedule_id,omitempty"`      // cron 来源才有
	DagExecID    int64     `json:"dag_execution_id,omitempty"` // dag 来源才有
	DagNodeID    string    `json:"dag_node_id,omitempty"`
	Success      bool      `json:"success"`
	FailureClass string    `json:"failure_class,omitempty"` // 仅失败时有值：timeout / execution
	ErrorMsg     string    `json:"error,omitempty"`         // 失败时必带
	DurationMS   int64     `json:"duration_ms"`
	WorkerID     string    `json:"worker_id,omitempty"`
	TraceID      string    `json:"trace_id,omitempty"`
	FinishedAt   time.Time `json:"finished_at"`
}

// FromDag 结果是否来自 DAG 节点（决定路由到 dag 族还是 cron 族队列）
func (r *ExecutionResult) FromDag() bool { return r.DagExecID > 0 }

// Body 序列化为 JSON
func (r *ExecutionResult) Body() ([]byte, error) {
	return json.Marshal(r)
}

// TaskPusher 执行投递的唯一出口。
// 谁实现：infra/mq/pusher.go（RabbitMQ，producer confirm 模式）
// 谁消费：api/logic（手动触发）、scheduler/engine（Cron 触发）、scheduler/retry（重试投递）
// 路由语义（实现方按 Priority 分流，调用方不感知队列名）：
//
//	priority ≤ 3 → job.queue.high
//	priority 4-7 → job.queue.normal
//	priority ≥ 8 → job.queue.low
type TaskPusher interface {
	// Push 投递一条执行消息。
	//   - 实现方必须先 Validate，非法消息返回 ErrInvalidMessage
	//   - 必须开启 producer confirm：confirm 未收到或 broker NACK 时返回 error，
	//     保证"返回 nil ≈ 消息已持久化到队列"（文档②§一致性：任务不丢失）
	//   - 至少一次投递语义：调用方须保证 ExecID 幂等（消费端 ClaimPending 兜底）
	Push(ctx context.Context, msg *ExecutionMessage) error
}
