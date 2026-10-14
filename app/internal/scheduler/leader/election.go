// Package leader 基于 etcd 的调度器多实例协调：
//
//	election  选主——谁当选谁跑触发循环，崩溃后其余实例自动补位
//	renewal   成员注册——key 绑定 TTL 租约，崩溃自动退出成员表
//	load      负载心跳——周期上报 CPU/内存/待处理量
//	rebalance leader 侧监听成员变化，做确定性分片均衡
//
// key 布局（本包所有文件共用，勿散落魔法串）：
//
//	/scheduler/election/       选主目录（concurrency.Election）
//	/scheduler/nodes/<nodeID>  成员注册 + 负载心跳（租约绑定）
package leader

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

// ElectionPrefix 选主 key 前缀
const ElectionPrefix = "/scheduler/election/"

// Elector 选主器。
//
// 用 concurrency.Election 而不是一次性 Mutex：leadership 是长期状态，
// Campaign 当选后持有到租约失效；进程崩溃租约到点自动卸任，其余实例的
// 竞选循环自动补位，不需要 watch 某个 key 再去抢（原 electAndwacth 的
// Get-then-Lock 在两实例同时发现 key 空时会同时 Lock 成功前的窗口竞争，
// Election 内部用事务保证只有一个 Campaign 胜出）。
type Elector struct {
	cli    *clientv3.Client
	nodeID string
	ttl    int64
}

// NewElector ttlSeconds 是选主租约时长：leader 崩溃后，其余实例最迟
// 这么久后接管。下限 5（etcd 最小 TTL），建议 ≥10s 给 CPU 打满留余量
func NewElector(cli *clientv3.Client, nodeID string, ttlSeconds int64) *Elector {
	if ttlSeconds < 5 {
		ttlSeconds = 5
	}
	return &Elector{cli: cli, nodeID: nodeID, ttl: ttlSeconds}
}

// Run 竞选循环（阻塞，调用方放独立 goroutine）：
//
//   - 当选 → 调用 onElected(ctx)；它返回即视为该实例的调度职责结束，
//     随后守着会话等卸任。要长期持有就在 onElected 里常驻到 ctx 取消
//   - 租约失效（崩溃恢复/网络分区）→ onLost → 退避后重新竞选
//   - ctx 取消 → 主动 Resign + 关 Session，立刻让位，不等 TTL
//
// 任何路径下 ctx 结束都返回 nil。
func (el *Elector) Run(ctx context.Context, onElected func(ctx context.Context), onLost func()) error {
	const maxBackoff = 10 * time.Second
	backoff := time.Second
	for {
		sess, err := concurrency.NewSession(el.cli,
			concurrency.WithTTL(int(el.ttl)), concurrency.WithContext(ctx))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			zap.L().Error("leader: 创建选举会话失败",
				zap.String("node_id", el.nodeID), zap.Error(err), zap.Duration("backoff", backoff))
			if !sleepCtx(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		e := concurrency.NewElection(sess, ElectionPrefix)
		// Campaign 阻塞直到当选或 ctx 取消；竞选值是 nodeID，
		// 观察者 Get 选主目录即可知道 leader 是谁
		if err := e.Campaign(ctx, el.nodeID); err != nil {
			_ = sess.Close()
			if ctx.Err() != nil {
				return nil
			}
			zap.L().Error("leader: 竞选失败",
				zap.String("node_id", el.nodeID), zap.Error(err), zap.Duration("backoff", backoff))
			if !sleepCtx(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		backoff = time.Second

		zap.L().Info("leader: 当选", zap.String("node_id", el.nodeID))
		if onElected != nil {
			onElected(ctx)
		}

		// 当选期间守着会话：lease 结束（TTL 到点/连接断开被 etcd 撤销）即卸任
		select {
		case <-ctx.Done():
			// 优雅退出先 Resign：其余实例立刻补位，不等 TTL
			_ = e.Resign(context.Background())
			_ = sess.Close()
			zap.L().Info("leader: 主动卸任退出", zap.String("node_id", el.nodeID))
			return nil
		case <-sess.Done():
			zap.L().Warn("leader: 会话失效，卸任等待重选", zap.String("node_id", el.nodeID))
			if onLost != nil {
				onLost()
			}
			if !sleepCtx(ctx, backoff) {
				return nil
			}
		}
	}
}

// Leader 查询当前 leader 的 nodeID；无人当选返回空串
func (el *Elector) Leader(ctx context.Context) (string, error) {
	resp, err := el.cli.Get(ctx, ElectionPrefix, clientv3.WithPrefix())
	if err != nil {
		return "", fmt.Errorf("leader: query election: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	// Election 目录下当前 leader 的 key-value：value 即 Campaign 写入的 nodeID
	return string(resp.Kvs[0].Value), nil
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// 退避
func nextBackoff(cur, max time.Duration) time.Duration {
	if cur *= 2; cur > max {
		return max
	}
	return cur
}
