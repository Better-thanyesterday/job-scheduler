// internal/domain/coordination/errors.go
package coordination

import "errors"

var (
	// ErrLockNotAcquired 锁获取失败（被其他实例持有）。
	// ★ 对 engine 这不是错误而是正常分支：记 debug 日志后跳过，
	//   Alertmanager 不应因它告警——所以用独立类型，与真故障区分
	ErrLockNotAcquired = errors.New("coordination: lock not acquired")

	// ErrLockLost 持有期间锁丢失（续期失败/已易主），持有方必须立即中止当前动作
	ErrLockLost = errors.New("coordination: lock lost")

	// ErrLockUnavailable 锁服务本身不可达（etcd 全挂/网络分区等）。
	// ★ 与前两者语义不同：它不代表"别的实例在干活"，也不代表"锁已丢失"，
	//   而是协调能力暂时缺失。调用方可选择 fail-open（无锁继续，靠 DB 侧
	//   状态守卫兜底）或 fail-closed（暂停动作），按自身正确性模型决定
	ErrLockUnavailable = errors.New("coordination: lock service unavailable")

	// ErrInvalidMessage ExecutionMessage 未通过 Validate
	ErrInvalidMessage = errors.New("coordination: invalid execution message")

	// ErrPushFailed 投递失败（confirm 超时 / broker NACK / 队列不可达）。
	// engine 侧语义：登记已完成的执行记录进入"已登记未投递"状态，
	// 由 reaper/人工重投——登记与投递分离正是为此兜底
	ErrPushFailed = errors.New("coordination: push failed")
)
