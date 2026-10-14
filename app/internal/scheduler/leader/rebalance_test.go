package leader

import (
	"encoding/json"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// ---------------------------------------------------------------------------
// BalanceOne / Balance：确定性分片
// ---------------------------------------------------------------------------

func TestBalanceOne_Deterministic(t *testing.T) {
	nodes := []string{"node-a", "node-b", "node-c"}
	first := BalanceOne(42, nodes)
	for i := 0; i < 100; i++ {
		if got := BalanceOne(42, nodes); got != first {
			t.Fatalf("BalanceOne(42) unstable: %v then %v", first, got)
		}
	}
}

func TestBalanceOne_IndependentOfNodeOrder(t *testing.T) {
	// 结果只依赖 scheduleID 与节点集合，不依赖列表顺序
	a := []string{"node-a", "node-b", "node-c"}
	b := []string{"node-c", "node-a", "node-b"}
	if BalanceOne(7, a) != BalanceOne(7, b) {
		t.Fatal("BalanceOne result changed with node order")
	}
}

func TestBalanceOne_EmptyNodes(t *testing.T) {
	if got := BalanceOne(1, nil); got != "" {
		t.Errorf("BalanceOne on empty nodes = %q, want empty string", got)
	}
}

func TestBalance_CoversAllIDsAndAllNodesReachable(t *testing.T) {
	nodes := []string{"node-a", "node-b", "node-c", "node-d"}
	ids := make([]int64, 0, 1000)
	for i := int64(1); i <= 1000; i++ {
		ids = append(ids, i)
	}
	assign := Balance(ids, nodes)

	if len(assign) != len(ids) {
		t.Fatalf("Balance assigned %d ids, want %d", len(assign), len(ids))
	}
	hit := map[string]bool{}
	for id, node := range assign {
		found := false
		for _, n := range nodes {
			if node == n {
				found = true
			}
		}
		if !found {
			t.Fatalf("id %d assigned to unknown node %q", id, node)
		}
		hit[node] = true
	}
	// 1000 个 ID 打 4 个节点，全部命 中才算分片在正常工作
	if len(hit) != len(nodes) {
		t.Errorf("only %d nodes reached, want %d", len(hit), len(nodes))
	}
}

// ---------------------------------------------------------------------------
// Membership：事件应用 / 全量重置 / 快照排序
// ---------------------------------------------------------------------------

func nodeLoadJSON(t *testing.T, n NodeLoad) []byte {
	t.Helper()
	b, err := json.Marshal(n)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMembership_ApplyPutDelete(t *testing.T) {
	m := newMembership()
	n1 := NodeLoad{NodeID: "node-1", CpuUsage: 0.5}
	n2 := NodeLoad{NodeID: "node-2", CpuUsage: 0.2}

	m.apply(true, NodesPrefix+"node-1", nodeLoadJSON(t, n1))
	if m.Gen() != 1 {
		t.Fatalf("gen after first put = %d, want 1", m.Gen())
	}
	m.apply(true, NodesPrefix+"node-2", nodeLoadJSON(t, n2))
	m.apply(true, NodesPrefix+"node-2", nodeLoadJSON(t, NodeLoad{NodeID: "node-2", CpuUsage: 0.9})) // 心跳更新，同代数语义上仍是一次变更
	if got := len(m.Snapshot()); got != 2 {
		t.Fatalf("snapshot size = %d, want 2", got)
	}

	m.apply(false, NodesPrefix+"node-1", nil)
	snap := m.Snapshot()
	if len(snap) != 1 || snap[0].NodeID != "node-2" {
		t.Fatalf("after delete snapshot = %+v, want only node-2", snap)
	}
	if snap[0].CpuUsage != 0.9 {
		t.Errorf("node-2 CpuUsage = %v, want 0.9 (heartbeat updated)", snap[0].CpuUsage)
	}
}

func TestMembership_SnapshotSorted(t *testing.T) {
	m := newMembership()
	for _, id := range []string{"node-c", "node-a", "node-b"} {
		m.apply(true, NodesPrefix+id, nodeLoadJSON(t, NodeLoad{NodeID: id}))
	}
	snap := m.Snapshot()
	want := []string{"node-a", "node-b", "node-c"}
	for i, n := range snap {
		if n.NodeID != want[i] {
			t.Fatalf("snapshot[%d] = %s, want %s (sorted)", i, n.NodeID, want[i])
		}
	}
}

func TestMembership_ApplyRejectsBadJSON(t *testing.T) {
	m := newMembership()
	m.apply(true, NodesPrefix+"node-x", []byte("{not-json"))
	if got := len(m.Snapshot()); got != 0 {
		t.Fatalf("bad json should not join membership, got %d members", got)
	}
}

func TestMembership_ResetReplaces(t *testing.T) {
	m := newMembership()
	m.apply(true, NodesPrefix+"stale-node", nodeLoadJSON(t, NodeLoad{NodeID: "stale-node"}))

	// 全量校准返回的成员表已不含 stale-node，reset 后视图必须整体替换
	fresh := NodeLoad{NodeID: "fresh-node"}
	kv := &mvccpb.KeyValue{}
	kv.Key = []byte(NodesPrefix + "fresh-node")
	kv.Value = nodeLoadJSON(t, fresh)

	m.reset([]*mvccpb.KeyValue{kv})
	snap := m.Snapshot()
	if len(snap) != 1 || snap[0].NodeID != "fresh-node" {
		t.Fatalf("after reset snapshot = %+v, want only fresh-node", snap)
	}
}
