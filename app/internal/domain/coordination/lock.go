// internal/domain/coordination/lock.go
package coordination

import (
	"context"
	"time"
)

// LockHandle 一次成功获取的锁句柄。释放必须经过它，而不是裸传 key——
// 防止"拿到的是 A 锁、释放的是 B 锁"的低级错误，也方便实现方内部
// 绑定 token（如将来 Redis 实现的 owner 校验）
type LockHandle struct {
	key       string
	token     string // 实现方自派；MySQL 实现里用不上，Redis 实现用于安全释放
	expiresAt time.Time
}

// NewLockHandle 供 infra 实现方构造句柄。token 由实现方自派
// （MySQL = CONNECTION_ID，Redis = 随机串），domain 不解释其内容
func NewLockHandle(key, token string, expiresAt time.Time) *LockHandle {
	return &LockHandle{key: key, token: token, expiresAt: expiresAt}
}

func (h *LockHandle) Key() string          { return h.key }
func (h *LockHandle) Token() string        { return h.token }
func (h *LockHandle) ExpiresAt() time.Time { return h.expiresAt }

// LockManager 调度唯一性端口。
//
// 谁实现：MVP = infra/mysql/lock.go（GET_LOCK，不依赖 Redis）
//         P2  = infra/redis/lock.go（Redlock，接口不变直接替换）
//         P3  = infra/redis/lock.go（Redlock，接口不变直接替换）

// 谁消费：仅 scheduler/engine（触发前抢锁）
//
// 正确性地位：本文档定义的正确性三件套之一（MySQL + RabbitMQ + LockManager）。
// 要求：锁对"所有进程形态"互斥——包括 cmd/server 合体模式下的内嵌 engine，
// 不得退化为进程内 mutex
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
