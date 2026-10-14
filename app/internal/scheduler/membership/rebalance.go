package membership

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Membership 成员表：全集群节点负载视图（本节点视角各自维护）。
// 由 WatchNodeInfo 维护，读走 Snapshot（返回排序副本）
type Membership struct {
	mu    sync.Mutex
	nodes map[string]NodeLoad
	gen   uint64 // 成员变动代数：每次 Put/Delete/重置 +1，调用方借此触发 Balance
}

func newMembership() *Membership {
	return &Membership{nodes: make(map[string]NodeLoad)}
}

// apply 应用一条成员表事件
func (m *Membership) apply(isPut bool, key string, value []byte) {
	nodeID := strings.TrimPrefix(key, NodesPrefix)
	if nodeID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if isPut {
		var load NodeLoad
		if err := json.Unmarshal(value, &load); err != nil {
			// 脏数据只跳过不落表，避免坏 json 冒充成员参与分片
			zap.L().Warn("membership: 成员负载解析失败", zap.String("key", key), zap.Error(err))
			return
		}
		if load.NodeID == "" {
			load.NodeID = nodeID
		}
		m.nodes[load.NodeID] = load
	} else {
		delete(m.nodes, nodeID)
	}
	m.gen++
}

// reset 全量重建（定期校准用）：watch 断线窗口漏掉的事件在这里自愈
func (m *Membership) reset(kvs []*mvccpb.KeyValue) {
	fresh := make(map[string]NodeLoad, len(kvs))
	for _, kv := range kvs {
		nodeID := strings.TrimPrefix(string(kv.Key), NodesPrefix)
		if nodeID == "" {
			continue
		}
		var load NodeLoad
		if err := json.Unmarshal(kv.Value, &load); err != nil {
			zap.L().Warn("membership: 成员负载解析失败（重置）", zap.String("key", string(kv.Key)), zap.Error(err))
			continue
		}
		if load.NodeID == "" {
			load.NodeID = nodeID
		}
		fresh[load.NodeID] = load
	}
	m.mu.Lock()
	m.nodes = fresh
	m.gen++
	m.mu.Unlock()
}

// Snapshot 当前成员负载快照，按 nodeID 排序——排序保证分片结果稳定
func (m *Membership) Snapshot() []NodeLoad {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]NodeLoad, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// Gen 成员代数
func (m *Membership) Gen() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gen
}

// resyncInterval 全量校准周期：clientv3 的 watch 断线会自动重建，但
// 重建起点之后的缺口事件无法凭空补回，靠周期全量 Get 兜底
const resyncInterval = 30 * time.Second

// WatchNodeInfo 维护成员表（阻塞启动，事件消费在后台 goroutine）：
// 先全量拉取建表，再以拉取 revision+1 起点监听增量（避免 Get 与 Watch
// 之间的事件缺口），并周期全量校准。
// onChange 在每次成员变动后被调用（同步调用：慢回调会拖住事件循环，
// 只做轻活；要重活请起 goroutine）
func WatchNodeInfo(ctx context.Context, cli *clientv3.Client, onChange func(*Membership)) (*Membership, error) {
	m := newMembership()
	resp, err := cli.Get(ctx, NodesPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("membership: 拉取成员表: %w", err)
	}
	for _, kv := range resp.Kvs {
		m.apply(true, string(kv.Key), kv.Value)
	}

	// 从拉取到的 revision 之后开始 watch，无缝衔接
	wch := cli.Watch(ctx, NodesPrefix, clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))
	go func() {
		resync := time.NewTicker(resyncInterval)
		defer resync.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-resync.C:
				r, err := cli.Get(ctx, NodesPrefix, clientv3.WithPrefix())
				if err != nil {
					zap.L().Warn("membership: 成员表全量校准失败", zap.Error(err))
					continue
				}
				m.reset(r.Kvs)
				if onChange != nil {
					onChange(m)
				}
			case wresp, ok := <-wch:
				// 通道关闭 = ctx 取消（连接级断线由 clientv3 内部自动恢复）
				if !ok {
					return
				}
				for _, ev := range wresp.Events {
					m.apply(ev.IsCreate() || ev.IsModify(), string(ev.Kv.Key), ev.Kv.Value)
				}
				if onChange != nil {
					onChange(m)
				}
			}
		}
	}()
	return m, nil
}

// Assign 确定性分片：scheduleID → 节点。
// 节点集合不变时结果永远一致——所有实例各自可算，无需协商
func (m *Membership) Assign(scheduleID int64) (string, error) {
	nodes := m.Snapshot()
	if len(nodes) == 0 {
		return "", fmt.Errorf("membership: 成员表为空，无法分片 schedule=%d", scheduleID)
	}
	return BalanceOne(scheduleID, nodeIDs(nodes)), nil
}

// Balance 全量分配：所有调度 ID → 节点
func Balance(scheduleIDs []int64, nodeIDs []string) map[int64]string {
	out := make(map[int64]string, len(scheduleIDs))
	for _, id := range scheduleIDs {
		out[id] = BalanceOne(id, nodeIDs)
	}
	return out
}

// BalanceOne 单点分片：Rendezvous（HRW）哈希——对每个节点算
// fnv64a(scheduleID||nodeID) 权重，取权重最高者。
//   - 结果只依赖节点集合，与传入顺序无关（权重并列再按名字取小，完全确定）
//   - 节点增减时只有受该节点影响的分片会迁移，其余不动
func BalanceOne(scheduleID int64, nodeIDs []string) string {
	if len(nodeIDs) == 0 {
		return ""
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(scheduleID))
	best := ""
	var bestWeight uint64
	for _, n := range nodeIDs {
		h := fnv.New64a()
		_, _ = h.Write(buf[:])
		_, _ = h.Write([]byte(n))
		if w := h.Sum64(); best == "" || w > bestWeight || (w == bestWeight && n < best) {
			best, bestWeight = n, w
		}
	}
	return best
}

func nodeIDs(nodes []NodeLoad) []string {
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.NodeID)
	}
	sort.Strings(ids)
	return ids
}
