package scheduler

import "errors"

var (
	// ErrTriggerFailed 投递失败（exec 已登记，等待补偿重投，不回滚）
	// 注：抢锁失败 ErrLockNotAcquired / 推进冲突 ErrConflict 是 domain 包的，
	// 这里不重复定义——errors.Is 判定时直接用 domain 包的 sentinel
	ErrTriggerFailed = errors.New("scheduler: trigger failed")

	// ErrNoTaskSnapshot 构建消息时任务配置取不到（任务被并发删除）
	// 处置：记 warn + 登记的 execution 置 Failed_final（没有配置的执行没有意义）
	ErrNoTaskSnapshot = errors.New("scheduler: task snapshot not found")

	// errSkipTrigger 本实例跳过本次触发的内部信号（锁被其他实例持有），
	// 对调用方表现为"无事发生"
	errSkipTrigger = errors.New("scheduler: trigger skipped (lock held)")
)
