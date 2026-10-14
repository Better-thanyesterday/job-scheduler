// internal/domain/coordination/lock.go
package coordination

import (
	"context"
	"time"
)

// LockHandle 一次成功获取的锁句柄。释放必须经过它，防止释放的是其他锁
type LockHandle struct {
	key       string
	token     string // 实现方自派；MySQL 实现里用不上，Redis 实现用于安全释放
	expiresAt time.Time
}

// NewLockHandle 供 infra 实现方构造句柄。token 由实现方自派
// （MySQL = CONNECTION_ID），domain 不解释其内容
func NewLockHandle(key, token string, expiresAt time.Time) *LockHandle {
	return &LockHandle{key: key, token: token, expiresAt: expiresAt}
}

func (h *LockHandle) Key() string          { return h.key }
func (h *LockHandle) Token() string        { return h.token }
func (h *LockHandle) ExpiresAt() time.Time { return h.expiresAt }

// LockManager 调度唯一性端口。
// 谁实现：infra/mysql/lock.go（GET_LOCK）和 infra/etcd/lock.go 共同保证
// 谁消费：scheduler/engine（触发前抢锁）
type LockManager interface {
	// Acquire 尝试获取命名锁。
	//   - 非阻塞：拿不到立即返回 ErrLockNotAcquired（engine 的语义是
	//     "别的实例已处理，跳过"，不是等待）
	//   - key 约定："schedule:{id}"（调用方拼前缀，实现方不管业务前缀）
	//   - ttl 到期自动释放，防止持有方崩溃导致死锁；
	//     长任务场景由调用方 Refresh 续期
	Acquire(ctx context.Context, key string, ttl time.Duration) (*LockHandle, error)

	// Release 主动释放（触发流程完成后调用）。
	// 释放非本人持有的锁返回 error；重复释放返回 nil（幂等，让调用方少写判断）
	Release(ctx context.Context, h *LockHandle) error

	// Refresh 续期（engine 的触发动作慢于 ttl 时防过期）。
	// 锁已易主/已过期返回 ErrLockNotAcquired——调用方应立刻停止触发流程
	Refresh(ctx context.Context, h *LockHandle, ttl time.Duration) error
}
