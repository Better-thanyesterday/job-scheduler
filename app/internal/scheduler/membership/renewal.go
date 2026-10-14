// Package membership 基于 etcd 的调度器多实例协调（多活跃分片模型）。
//
//	membership  成员注册——key 绑 TTL 租约，崩溃自动退出成员表
//	load        负载心跳——周期上报 CPU/内存/待处理量
//	rebalance   成员表 watch + HRW 确定性分片，成员变动自动再均衡
//
// 没有 leader：所有活跃节点对等，触发权按分片归属，正确性由
// GET_LOCK + nextRun CAS + 抢票幂等兜底（旧选主器已随主备模型移除）。
//
// key 布局（本包所有文件共用，勿散落魔法串）：
//
//	/scheduler/nodes/<nodeID>  成员注册 + 负载心跳（租约绑定）
package membership

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Registration 把一个 key 绑定到 TTL 租约上的注册句柄：
//
//   - 持有期：内部 KeepAlive 自动续租；续租通道断开（网络抖动等）时
//     以指数退避自动重新注册，value 原样恢复
//   - 进程崩溃：租约无人续，TTL 到点 etcd 自动删 key——成员表自动退出，
//     rebalance 据此感知节点离线
//   - 优雅退出：Close 主动 Revoke，成员立刻消失，不等 TTL
//
// "成员表里有谁"——两类租约独立，互不影响。
type Registration struct {
	cli *clientv3.Client
	key string
	ttl int64

	mu      sync.Mutex
	value   string
	leaseID clientv3.LeaseID
	ka      <-chan *clientv3.LeaseKeepAliveResponse

	done      chan struct{}
	closeOnce sync.Once
}

// Register 注册 key=value 并开始续租。ttlSeconds 下限 5（etcd 最小 TTL）。
// ctx 取消时自动注销并停止续租（与 Close 等效，幂等）
func Register(ctx context.Context, cli *clientv3.Client, key, value string, ttlSeconds int64) (*Registration, error) {
	if ttlSeconds < 5 {
		ttlSeconds = 5
	}
	r := &Registration{
		cli:  cli,
		key:  key,
		ttl:  ttlSeconds,
		done: make(chan struct{}),
	}
	if err := r.claim(ctx, value); err != nil {
		return nil, err
	}
	go r.pump(ctx)
	return r, nil
}

// claim 申请租约 + 写入 key + 建立续租流。
// 写入或续租建立失败时撤销租约，不留孤儿租约
func (r *Registration) claim(ctx context.Context, value string) error {
	grant, err := r.cli.Grant(ctx, r.ttl)
	if err != nil {
		return fmt.Errorf("membership: grant lease for %s: %w", r.key, err)
	}
	rollback := func() { _, _ = r.cli.Revoke(context.Background(), grant.ID) }

	if _, err := r.cli.Put(ctx, r.key, value, clientv3.WithLease(grant.ID)); err != nil {
		rollback()
		return fmt.Errorf("membership: put %s: %w", r.key, err)
	}
	ka, err := r.cli.KeepAlive(ctx, grant.ID)
	if err != nil {
		rollback()
		return fmt.Errorf("membership: keepalive %s: %w", r.key, err)
	}
	r.mu.Lock()
	r.value = value
	r.leaseID = grant.ID
	r.ka = ka
	r.mu.Unlock()
	return nil
}

// pump 续租守护：消费 KeepAlive 响应；通道关闭（连接断/租约失效）时
// 退避重注册，直到成功或 ctx 结束
func (r *Registration) pump(ctx context.Context) {
	const maxBackoff = 10 * time.Second
	backoff := time.Second
	for {
		r.mu.Lock()
		ka := r.ka
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			_ = r.Close(context.Background())
			return
		case resp, ok := <-ka:
			if ok {
				// resp.TTL 是剩余存活秒数，接监控可在此打点
				_ = resp
				continue
			}
			// 通道关闭：优先确认是不是主动关闭（Close/ctx），不是才重注册
			select {
			case <-r.done:
				return
			case <-ctx.Done():
				_ = r.Close(context.Background())
				return
			default:
			}
		}

		zap.L().Warn("membership: 租约续租中断，重新注册", zap.String("key", r.key))
		reregistered := false
		for !reregistered {
			if err := r.claim(ctx, r.snapshotValue()); err != nil {
				if ctx.Err() != nil {
					_ = r.Close(context.Background())
					return
				}
				zap.L().Error("membership: 重新注册失败，退避重试",
					zap.String("key", r.key), zap.Error(err), zap.Duration("backoff", backoff))
				if !sleepCtx(ctx, backoff) {
					_ = r.Close(context.Background())
					return
				}
				backoff = nextBackoff(backoff, maxBackoff)
				continue
			}
			backoff = time.Second
			reregistered = true
		}
	}
}

// Update 原位刷新 value：写同一 key、绑同一租约——成员身份不变，
// 只更新心跳内容（LoadReporter 走这条路周期上报负载）
func (r *Registration) Update(ctx context.Context, value string) error {
	r.mu.Lock()
	leaseID := r.leaseID
	r.mu.Unlock()
	if _, err := r.cli.Put(ctx, r.key, value, clientv3.WithLease(leaseID)); err != nil {
		return fmt.Errorf("membership: update %s: %w", r.key, err)
	}
	r.mu.Lock()
	r.value = value
	r.mu.Unlock()
	return nil
}

// Close 主动注销：撤销租约（etcd 立即删 key）并停止续租。幂等。
// 注：与"claim 正在重注册"并发调用存在极端窗口（claim 晚于 Revoke 完成），
// MVP 接受——下一次 claim 建立的租约仍会被后续 Close/进程退出收回
func (r *Registration) Close(ctx context.Context) error {
	var firstErr error
	r.closeOnce.Do(func() {
		close(r.done) // 先停 pump 的重注册路径
		r.mu.Lock()
		leaseID := r.leaseID
		r.mu.Unlock()
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if _, err := r.cli.Revoke(cctx, leaseID); err != nil && ctx.Err() == nil {
			firstErr = fmt.Errorf("membership: revoke %s: %w", r.key, err)
		}
	})
	return firstErr
}

func (r *Registration) snapshotValue() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.value
}

// sleepCtx 可取消的 sleep：ctx 取消返回 false
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// nextBackoff 指数退避
func nextBackoff(cur, max time.Duration) time.Duration {
	if cur *= 2; cur > max {
		return max
	}
	return cur
}
