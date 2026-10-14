package schedule

import "errors"

var (
	// ---------- 构造期（New / Rebuild）----------

	// ErrNotFound 调度配置不存在（repo Get 未命中；api 返回 404 语义）
	ErrNotFound = errors.New("schedule: not found")

	// ErrTaskNotFound 关联的任务不存在或已删除（New 时 taskID 校验失败的前置，
	// 更常见于 repo 层装配时发现 taskID 无对应记录）
	ErrTaskNotFound = errors.New("schedule: referenced task not found")

	// ErrInvalidCron 持有非法 Cron 表达式（Rebuild 时 Parse 失败——
	// 拦截库里被手改坏的数据；正常创建路径 New 里就 Parse 了）
	ErrInvalidCron = errors.New("schedule: invalid cron expression")

	// ErrInvalidPolicy misfire_policy / schedule_type 超出枚举范围
	ErrInvalidPolicy = errors.New("schedule: invalid misfire policy or schedule type")

	// ErrInvalidTimezone 时区不合法（LoadLocation 失败）
	ErrInvalidTimezone = errors.New("schedule: invalid timezone")

	// ---------- 状态流转期（Pause / Resume / Advance）----------

	// ErrWrongStatus 状态机拒绝：如对已暂停的调度再 Pause、
	// 对已删除的调度做任何流转（对应 §5.1.1 写权限思想）
	ErrWrongStatus = errors.New("schedule: operation not allowed in current status")

	// ErrConflict 乐观锁冲突：UpdateNextRun / SetStatus 的 CAS 失败
	// （engine 与 api 同时改同一条调度）。调用方应放弃本次、下次重读
	ErrConflict = errors.New("schedule: concurrent modification, retry")
)
