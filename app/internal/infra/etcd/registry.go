// Package etcd 执行器的注册与发现：
//
//	reporter   executor 侧——把自身信息（地址/并发/执行中数量）挂在 TTL 租约上，
//	           崩溃后租约到点 etcd 自动删 key，成员表自动退出
//	discovery  scheduler 侧——列出在线 executor，供推送方选择目标
//
// key 布局（与 scheduler/leader 的 /scheduler/* 前缀区分）：
//
//	/executors/<workerID>  执行器注册 + 状态心跳（租约绑定）
package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// ExecutorsPrefix 执行器注册 key 前缀，完整 key 为 /executors/<workerID>
const ExecutorsPrefix = "/executors/"

// DefaultLeaseTTL 租约时长：executor 崩溃后最迟这么久从成员表消失。
// 下限 5（etcd 最小 TTL），建议给 keepalive 中断留足余量
const DefaultLeaseTTL = 15

// ExecutorInfo 单个执行器的状态快照，挂在注册租约的 value 上。
// 字段只增不删，新增必须 omitempty（discovery 侧容忍式反序列化）
type ExecutorInfo struct {
	WorkerID   string    `json:"worker_id"`
	Addr       string    `json:"addr"` // scheduler 可达的 gRPC 地址
	MaxConcurr int32     `json:"max_concurrency"`
	Running    int32     `json:"running"` // 当前执行中的任务数，推送方按此择优
	Version    string    `json:"version,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	UpdateAt   time.Time `json:"update_at,omitempty"`
}

// NewEtcdClient 建立 etcd 连接（executor 与 scheduler 共用的入口）。
// endpoint 全部不可达时返回错误
func NewEtcdClient(endpoints []string) (*clientv3.Client, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("etcd: empty endpoints")
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd: new client: %w", err)
	}
	// clientv3 懒连接，这里主动探一次，把"配置错/etcd 没起"挡在启动期
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Get(ctx, ExecutorsPrefix); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("etcd: probe %v: %w", endpoints, err)
	}
	return cli, nil
}

// Reporter 把 ExecutorInfo 绑定到 TTL 租约上周期上报：
//
//   - 持有期：内部 KeepAlive 自动续租；续租通道断开（网络抖动等）时
//     以指数退避自动重新注册
//   - 进程崩溃：租约无人续，TTL 到点 etcd 自动删 key，scheduler 发现
//     即刻停止向该实例推送
//   - 优雅退出：Close 主动 Revoke，成员立刻消失，不等 TTL
//
// 状态内容经 snapshot 函数动态获取（Running 等数值每次刷新都要最新值），
// 本包不关心数据来源
type Reporter struct {
	cli      *clientv3.Client
	key      string
	ttl      int64
	interval time.Duration
	snapshot func() ExecutorInfo

	mu      sync.Mutex
	value   string
	leaseID clientv3.LeaseID
	ka      <-chan *clientv3.LeaseKeepAliveResponse

	done      chan struct{}
	closeOnce sync.Once
}

// NewReporter ttlSeconds 下限 5（etcd 最小 TTL）；interval<=0 取 5s
func NewReporter(cli *clientv3.Client, info ExecutorInfo, ttlSeconds int64, interval time.Duration, snapshot func() ExecutorInfo) *Reporter {
	if ttlSeconds < 5 {
		ttlSeconds = 5
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	info.WorkerID = firstNonEmpty(info.WorkerID, "executor-unknown")
	return &Reporter{
		cli:      cli,
		key:      ExecutorsPrefix + info.WorkerID,
		ttl:      ttlSeconds,
		interval: interval,
		snapshot: snapshot,
		done:     make(chan struct{}),
	}
}

// Run 阻塞循环（调用方放独立 goroutine）：注册 + 续租守护 + 周期状态刷新。
// ctx 取消时自动注销（与 Close 等效，幂等）
func (r *Reporter) Run(ctx context.Context) {
	const maxBackoff = 10 * time.Second
	backoff := time.Second
	for {
		if err := r.claim(ctx); err != nil {
			if ctx.Err() != nil {
				_ = r.Close(context.Background())
				return
			}
			zap.L().Error("etcd: executor 注册失败，退避重试",
				zap.String("key", r.key), zap.Error(err), zap.Duration("backoff", backoff))
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		backoff = time.Second
		zap.L().Info("etcd: executor 已注册", zap.String("key", r.key))

		if !r.pump(ctx) {
			return
		}
		// pump 返回说明续租通道断开（非主动关闭），重注册
		zap.L().Warn("etcd: executor 租约续租中断，重新注册", zap.String("key", r.key))
		if !sleepCtx(ctx, backoff) {
			return
		}
	}
}

// pump 续租守护：消费 KeepAlive 响应、按 interval 刷新状态 value。
// 返回 false 表示该退出（ctx 结束/主动 Close）；返回 true 表示通道断开需重注册
func (r *Reporter) pump(ctx context.Context) bool {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		r.mu.Lock()
		ka := r.ka
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			_ = r.Close(context.Background())
			return false
		case <-r.done:
			return false
		case <-ticker.C:
			if err := r.update(ctx); err != nil {
				zap.L().Warn("etcd: executor 状态上报失败", zap.String("key", r.key), zap.Error(err))
			}
		case resp, ok := <-ka:
			if ok {
				_ = resp // resp.TTL 为剩余存活秒数，接监控可在此打点
				continue
			}
			// 通道关闭：优先确认是不是主动关闭，不是才重注册
			select {
			case <-r.done:
				return false
			case <-ctx.Done():
				_ = r.Close(context.Background())
				return false
			default:
			}
			return true
		}
	}
}

// claim 申请租约 + 写入 key + 建立续租流。失败时撤销租约，不留孤儿租约
func (r *Reporter) claim(ctx context.Context) error {
	value, err := r.marshalInfo()
	if err != nil {
		return err
	}
	grant, err := r.cli.Grant(ctx, r.ttl)
	if err != nil {
		return fmt.Errorf("etcd: grant lease for %s: %w", r.key, err)
	}
	rollback := func() { _, _ = r.cli.Revoke(context.Background(), grant.ID) }

	if _, err := r.cli.Put(ctx, r.key, value, clientv3.WithLease(grant.ID)); err != nil {
		rollback()
		return fmt.Errorf("etcd: put %s: %w", r.key, err)
	}
	ka, err := r.cli.KeepAlive(ctx, grant.ID)
	if err != nil {
		rollback()
		return fmt.Errorf("etcd: keepalive %s: %w", r.key, err)
	}
	r.mu.Lock()
	r.value = value
	r.leaseID = grant.ID
	r.ka = ka
	r.mu.Unlock()
	return nil
}

// update 原位刷新状态：写同一 key、绑同一租约——成员身份不变，只刷新心跳内容
func (r *Reporter) update(ctx context.Context) error {
	value, err := r.marshalInfo()
	if err != nil {
		return err
	}
	r.mu.Lock()
	leaseID := r.leaseID
	r.mu.Unlock()
	if _, err := r.cli.Put(ctx, r.key, value, clientv3.WithLease(leaseID)); err != nil {
		return fmt.Errorf("etcd: update %s: %w", r.key, err)
	}
	r.mu.Lock()
	r.value = value
	r.mu.Unlock()
	return nil
}

func (r *Reporter) marshalInfo() (string, error) {
	info := r.snapshot()
	if info.UpdateAt.IsZero() {
		info.UpdateAt = time.Now()
	}
	body, err := json.Marshal(info)
	if err != nil {
		return "", fmt.Errorf("etcd: marshal executor info: %w", err)
	}
	return string(body), nil
}

// Close 主动注销：撤销租约（etcd 立即删 key）并停止续租。幂等。
// 注：与 claim 重注册并发调用存在极端窗口（claim 晚于 Revoke 完成），
// MVP 接受——后续 claim 建立的租约仍会被再次 Close/进程退出收回
func (r *Reporter) Close(ctx context.Context) error {
	var firstErr error
	r.closeOnce.Do(func() {
		close(r.done) // 先停 pump 的刷新与重注册路径
		r.mu.Lock()
		leaseID := r.leaseID
		r.mu.Unlock()
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if _, err := r.cli.Revoke(cctx, leaseID); err != nil && ctx.Err() == nil {
			firstErr = fmt.Errorf("etcd: revoke %s: %w", r.key, err)
		}
		zap.L().Info("etcd: executor 已注销", zap.String("key", r.key))
	})
	return firstErr
}

// ==================== discovery：scheduler 侧 ====================

// ListExecutors 列出当前在线的 executor（注册租约存活的都在表里）。
// 返回顺序不保证，调用方自行择优
func ListExecutors(ctx context.Context, cli *clientv3.Client) ([]ExecutorInfo, error) {
	resp, err := cli.Get(ctx, ExecutorsPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcd: list %s: %w", ExecutorsPrefix, err)
	}
	out := make([]ExecutorInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var info ExecutorInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			// 单条脏数据不拦整体：记日志跳过
			zap.L().Warn("etcd: executor 信息反序列化失败，跳过",
				zap.String("key", string(kv.Key)), zap.Error(err))
			continue
		}
		if info.Addr == "" {
			continue
		}
		out = append(out, info)
	}
	return out, nil
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	if cur *= 2; cur > max {
		return max
	}
	return cur
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
