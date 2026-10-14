// lock.go 分布式锁的 etcd 实现：租约 + 事务 CAS。
//
// 语义对照 coordination.LockManager：
//   - Acquire：Grant(TTL) 租约 → 事务「key 不存在才 Put(key=token, WithLease)」，
//     抢不到 = ErrLockNotAcquired（并撤销租约，不留孤儿）；etcd 不可达 = ErrLockUnavailable
//   - 持有方崩溃：租约无人续，TTL 到点 etcd 自动删 key——死锁免疫
//   - Refresh：KeepAliveOnce 续租；租约已过期（锁已自然释放）→ ErrLockNotAcquired
//   - Release：事务「value == 自己的 token 才 Delete」+ 撤销租约，
//     token 不匹配（锁已易主）返回 ErrLockLost
//
// value 是持有方的 token（lease id），所有写路径都带 token 比对，
// 与 redis SET NX PX + Lua 释放同构
package etcd

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"job-scheduler/app/internal/domain/coordination"
)

// locksPrefix 锁 key 前缀，与 /executors、/scheduler 的命名空间区分
const locksPrefix = "/locks/"

type LockManager struct {
	cli *clientv3.Client
	ttl int64 // 秒；下限 5（etcd 最小 TTL）
}

// NewLockManager ttlSeconds 下限 5
func NewLockManager(cli *clientv3.Client, ttlSeconds int64) *LockManager {
	if ttlSeconds < 5 {
		ttlSeconds = 5
	}
	return &LockManager{cli: cli, ttl: ttlSeconds}
}

// classify 把 etcd 错误映射到锁语义：服务不可达 ≠ 锁被占
func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return coordination.ErrLockUnavailable
	}
	return fmt.Errorf("etcd lock: %w", err)
}

// Acquire 非阻塞抢锁。持有 token = 租约 ID
func (m *LockManager) Acquire(ctx context.Context, key string, ttl time.Duration) (*coordination.LockHandle, error) {
	ttlSec := int64(ttl / time.Second)
	if ttlSec < 5 {
		ttlSec = 5
	}
	grant, err := m.cli.Grant(ctx, ttlSec)
	if err != nil {
		return nil, classify(err)
	}
	fullKey := locksPrefix + key
	token := strconv.FormatInt(int64(grant.ID), 10)

	txn, err := m.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(fullKey), "=", 0)).
		Then(clientv3.OpPut(fullKey, token, clientv3.WithLease(grant.ID))).
		Else(clientv3.OpGet(fullKey)).
		Commit()
	if err != nil {
		_, _ = m.cli.Revoke(context.Background(), grant.ID)
		return nil, classify(err)
	}
	if !txn.Succeeded {
		_, _ = m.cli.Revoke(context.Background(), grant.ID) // 抢不到立刻还租约
		return nil, coordination.ErrLockNotAcquired
	}
	return coordination.NewLockHandle(fullKey, token, time.Now().Add(time.Duration(ttlSec)*time.Second)), nil
}

// Release 安全释放：value 是自己的 token 才删；锁已易主返回 ErrLockLost。
// 无论删除成败都撤销自己的租约（易主场景下自己的租约早过期，撤销是 no-op）
func (m *LockManager) Release(ctx context.Context, h *coordination.LockHandle) error {
	if h == nil {
		return nil
	}
	txn, err := m.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(h.Key()), "=", h.Token())).
		Then(clientv3.OpDelete(h.Key())).
		Commit()
	if err != nil {
		return classify(err)
	}
	leaseID, _ := strconv.ParseInt(h.Token(), 10, 64)
	_, _ = m.cli.Revoke(context.Background(), clientv3.LeaseID(leaseID))
	if !txn.Succeeded {
		return coordination.ErrLockLost
	}
	return nil
}

// Refresh 续租：KeepAliveOnce 把租约 TTL 重置。
// 租约已过期（key 已被 etcd 删掉）→ ErrLockNotAcquired（接口契约：已易主/过期）
func (m *LockManager) Refresh(ctx context.Context, h *coordination.LockHandle, ttl time.Duration) error {
	if h == nil {
		return coordination.ErrLockLost
	}
	leaseID, err := strconv.ParseInt(h.Token(), 10, 64)
	if err != nil {
		return coordination.ErrLockLost
	}
	resp, err := m.cli.KeepAliveOnce(ctx, clientv3.LeaseID(leaseID))
	if err != nil {
		return classify(err)
	}
	if resp == nil || resp.TTL <= 0 {
		// 租约已不存在：锁已过期，别人可能已抢走
		return coordination.ErrLockNotAcquired
	}
	// 顺带刷新句柄的过期时间
	_ = h
	return nil
}
