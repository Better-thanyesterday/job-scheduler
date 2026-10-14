package scheduler

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	infraetcd "job-scheduler/app/internal/infra/etcd"

	"go.uber.org/zap"
)

const (
	// routeRefresh 成员表快照刷新周期：etcd 心跳本身 5s，路由快照取一半，
	// 兼顾新鲜度与 etcd 压力
	routeRefresh = 2 * time.Second
	// failCooldown RPC 失败后的熔断降温时长：窗口内该实例被排到所有
	// 无失败候选之后（不完全剔除——可能只有它一台活着）
	failCooldown = 10 * time.Second
	// failPenalty 熔断惩罚的得分增量：足够盖过负载差，又小于"无候选可用"
	failPenalty = 10.0
)

// router 负载感知的 executor 路由。
//
// 得分模型（越低越优）：
//
//	score = (etcd Running + 本进程在途) / MaxConcurr + 熔断惩罚
//
// 三层信息互补：
//   - etcd 快照：全集群视角，但有心跳间隔的滞后
//   - 在途增量：本进程刚推出去、心跳还没反映的量，弥补滞后
//   - 失败熔断：刚 RPC 失败的实例大概率还在抖，降权但保留（全挂时仍可兜底）
//
// 并列去歧义用 HRW：score 相同的候选按 fnv(execID, addr) 确定性地分散，
// 高频触发不会全部砸向同一台。
type router struct {
	list func(ctx context.Context) ([]infraetcd.ExecutorInfo, error)

	mu       sync.Mutex
	cache    []infraetcd.ExecutorInfo
	fetched  time.Time
	inflight map[string]int       // addr → 本进程在途推送数
	failedAt map[string]time.Time // addr → 最近一次 RPC 失败时刻
}

func newRouter(list func(ctx context.Context) ([]infraetcd.ExecutorInfo, error)) *router {
	return &router{
		list:     list,
		inflight: make(map[string]int),
		failedAt: make(map[string]time.Time),
	}
}

// candidates 返回按得分从优到劣的候选列表（供 Push 逐台降级重试）
func (r *router) candidates(ctx context.Context, execID int64) ([]infraetcd.ExecutorInfo, error) {
	infos, err := r.snapshot(ctx)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	at := now() // 包级注入时钟（勿用局部 time.Now 遮蔽——冷却判定要可测）
	type scored struct {
		info  infraetcd.ExecutorInfo
		score float64
		hrw   uint64
	}
	list := make([]scored, 0, len(infos))
	for _, in := range infos {
		if in.Addr == "" {
			continue
		}
		capacity := in.MaxConcurr
		if capacity <= 0 {
			capacity = 1 // 信息缺失按满负载保守处理，不放大权重
		}
		util := float64(in.Running+int32(r.inflight[in.Addr])) / float64(capacity)
		score := util
		if r.failedAt[in.Addr].After(at.Add(-failCooldown)) {
			score += failPenalty
		}
		list = append(list, scored{info: in, score: score, hrw: hrwKey(execID, in.Addr)})
	}
	if len(list) == 0 {
		return nil, coordErrNoExecutor()
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].score != list[j].score {
			return list[i].score < list[j].score
		}
		return list[i].hrw < list[j].hrw
	})
	out := make([]infraetcd.ExecutorInfo, 0, len(list))
	for _, s := range list {
		out = append(out, s.info)
	}
	return out, nil
}

// acquire 推送前登记在途（心跳滞后期内的真实负载）
func (r *router) acquire(addr string) {
	r.mu.Lock()
	r.inflight[addr]++
	r.mu.Unlock()
}

// release 推送返回后销账；失败记录熔断起点（用注入时钟，测试可推演冷却窗口）
func (r *router) release(addr string, ok bool) {
	r.mu.Lock()
	if r.inflight[addr] > 0 {
		r.inflight[addr]--
	}
	if !ok {
		r.failedAt[addr] = now()
	}
	r.mu.Unlock()
}

// snapshot 带缓存的成员表读取
func (r *router) snapshot(ctx context.Context) ([]infraetcd.ExecutorInfo, error) {
	r.mu.Lock()
	fresh := now().Sub(r.fetched) < routeRefresh
	cached := r.cache
	r.mu.Unlock()
	if fresh && len(cached) > 0 {
		return cached, nil
	}
	infos, err := r.list(ctx)
	if err != nil {
		// 拉新失败但还有旧快照：降级用旧的（成员表短暂抖动不该停触发）
		if len(cached) > 0 {
			zap.L().Warn("路由成员表刷新失败，使用旧快照", zap.Error(err), zap.Int("stale_n", len(cached)))
			return cached, nil
		}
		return nil, err
	}
	r.mu.Lock()
	r.cache = infos
	r.fetched = now()
	r.mu.Unlock()
	return infos, nil
}

// now 独立出来便于测试注入
var now = time.Now

// hrwKey HRW 去歧义键：同一 execID 在不同 addr 上得到稳定的伪随机次序
func hrwKey(execID int64, addr string) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(execID))
	_, _ = h.Write(buf[:])
	_, _ = h.Write([]byte(addr))
	return h.Sum64()
}

func coordErrNoExecutor() error {
	return coordination.ErrPushFailed
}
