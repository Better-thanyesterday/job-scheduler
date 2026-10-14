package dag

import (
	"strings"
	"testing"
	"time"
)

// diamond 菱形图：a → b,c → d（全 Success 边；边类型语义由
// TestFailureAndAlwaysEdges 单独覆盖）
func diamond(t *testing.T) *Dag {
	t.Helper()
	nodeA, _ := NewNode("a", 1, "A")
	nodeB, _ := NewNode("b", 2, "B")
	nodeC, _ := NewNode("c", 3, "C")
	nodeD := dagNodeD(t)
	ab, _ := NewEdge("a", "b", EdgeStatusSuccess)
	ac, _ := NewEdge("a", "c", EdgeStatusSuccess)
	bd, _ := NewEdge("b", "d", EdgeStatusSuccess)
	cd, _ := NewEdge("c", "d", EdgeStatusSuccess)
	d, err := New("demo", "菱形图", 0, true, 7,
		[]*DagNode{nodeA, nodeB, nodeC, nodeD}, []Edge{*ab, *ac, *bd, *cd})
	if err != nil {
		t.Fatalf("build diamond: %v", err)
	}
	return d
}

func dagNodeD(t *testing.T) *DagNode {
	t.Helper()
	n, err := NewNode("d", 4, "D")
	if err != nil {
		t.Fatalf("new node d: %v", err)
	}
	return n
}

func nodeIDs(nodes []*DagNode) []string {
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.NodeID())
	}
	return ids
}

func joinIDs(nodes []*DagNode) string { return strings.Join(nodeIDs(nodes), ",") }

// ---------- 边合法性校验 ----------

func TestNewEdgeValidation(t *testing.T) {
	cases := []struct {
		name    string
		from    string
		to      string
		typ     EdgeStatus
		wantSub string
	}{
		{"空起点", "", "b", EdgeStatusSuccess, "endpoint is empty"},
		{"空终点", "a", "", EdgeStatusSuccess, "endpoint is empty"},
		{"自环", "a", "a", EdgeStatusSuccess, "self loop"},
		{"非法边类型", "a", "b", EdgeStatus(99), "invalid edge type"},
		{"零值边类型", "a", "b", EdgeStatus(0), "invalid edge type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewEdge(tc.from, tc.to, tc.typ)
			if err == nil {
				t.Fatalf("want error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("want error containing %q, got %v", tc.wantSub, err)
			}
		})
	}
}

func TestNewEdgeValid(t *testing.T) {
	e, err := NewEdge("a", "b", EdgeStatusAlways)
	if err != nil {
		t.Fatalf("valid edge rejected: %v", err)
	}
	if e.FromNodeID() != "a" || e.ToNodeID() != "b" || e.EdgeType() != EdgeStatusAlways {
		t.Fatalf("edge fields wrong: %+v", e)
	}
	// 三种边类型都必须合法
	for _, typ := range []EdgeStatus{EdgeStatusSuccess, EdgeStatusFailure, EdgeStatusAlways} {
		if _, err := NewEdge("a", "b", typ); err != nil {
			t.Fatalf("edge type %v should be valid: %v", typ, err)
		}
	}
}

// TestDagEdgeReferenceCheck 边必须引用已定义的节点
func TestDagEdgeReferenceCheck(t *testing.T) {
	nodeA, _ := NewNode("a", 1, "A")
	nodeB, _ := NewNode("b", 2, "B")

	t.Run("边引用未定义的起点", func(t *testing.T) {
		e, _ := NewEdge("ghost", "b", EdgeStatusSuccess)
		_, err := New("x", "", 0, false, 1, []*DagNode{nodeA, nodeB}, []Edge{*e})
		if err == nil || !strings.Contains(err.Error(), "unknown node") {
			t.Fatalf("want unknown-node error, got %v", err)
		}
	})
	t.Run("边引用未定义的终点", func(t *testing.T) {
		e, _ := NewEdge("a", "ghost", EdgeStatusSuccess)
		_, err := New("x", "", 0, false, 1, []*DagNode{nodeA, nodeB}, []Edge{*e})
		if err == nil || !strings.Contains(err.Error(), "unknown node") {
			t.Fatalf("want unknown-node error, got %v", err)
		}
	})
}

// ---------- 节点合法性校验 ----------

func TestNewNodeValidation(t *testing.T) {
	if _, err := NewNode("", 1, "A"); err == nil || !strings.Contains(err.Error(), "node_id is empty") {
		t.Fatalf("empty node_id should be rejected, got %v", err)
	}
	if _, err := NewNode("a", 0, "A"); err == nil || !strings.Contains(err.Error(), "invalid task_id") {
		t.Fatalf("zero task_id should be rejected, got %v", err)
	}
	if _, err := NewNode("a", -1, "A"); err == nil {
		t.Fatalf("negative task_id should be rejected")
	}
}

func TestDagNodeIDUniqueness(t *testing.T) {
	nodeA, _ := NewNode("a", 1, "A")
	dup, _ := NewNode("a", 2, "A2") // 同 nodeID 不同 taskID
	_, err := New("x", "", 0, false, 1, []*DagNode{nodeA, dup}, nil)
	if err == nil || !strings.Contains(err.Error(), "duplicate node_id") {
		t.Fatalf("want duplicate node_id error, got %v", err)
	}
}

// ---------- 环检测 ----------

func TestCycleDetection(t *testing.T) {
	nodeA, _ := NewNode("a", 1, "A")
	nodeB, _ := NewNode("b", 2, "B")
	nodeC, _ := NewNode("c", 3, "C")

	t.Run("三节点环", func(t *testing.T) {
		ab, _ := NewEdge("a", "b", EdgeStatusSuccess)
		bc, _ := NewEdge("b", "c", EdgeStatusSuccess)
		ca, _ := NewEdge("c", "a", EdgeStatusAlways)
		_, err := New("cycle", "", 0, false, 1, []*DagNode{nodeA, nodeB, nodeC},
			[]Edge{*ab, *bc, *ca})
		if err == nil || !strings.Contains(err.Error(), "dependency cycle") {
			t.Fatalf("want cycle error, got %v", err)
		}
	})

	t.Run("两节点互相依赖", func(t *testing.T) {
		ab, _ := NewEdge("a", "b", EdgeStatusSuccess)
		ba, _ := NewEdge("b", "a", EdgeStatusSuccess)
		_, err := New("cycle2", "", 0, false, 1, []*DagNode{nodeA, nodeB}, []Edge{*ab, *ba})
		if err == nil || !strings.Contains(err.Error(), "dependency cycle") {
			t.Fatalf("want cycle error, got %v", err)
		}
	})
}

func TestAcyclicGraphsPass(t *testing.T) {
	t.Run("菱形图", func(t *testing.T) {
		d := diamond(t)
		if len(d.Nodes()) != 4 || len(d.Edges()) != 4 {
			t.Fatalf("diamond shape wrong: %d nodes %d edges", len(d.Nodes()), len(d.Edges()))
		}
	})

	t.Run("Always 边方向的环同样被拒", func(t *testing.T) {
		// 环检测不分边类型：c → a 即便声明为 Always 也构成先后约束
		nodeA, _ := NewNode("a", 1, "A")
		nodeC, _ := NewNode("c", 3, "C")
		ac, _ := NewEdge("a", "c", EdgeStatusSuccess)
		ca, _ := NewEdge("c", "a", EdgeStatusAlways)
		if _, err := New("mixed", "", 0, false, 1, []*DagNode{nodeA, nodeC},
			[]Edge{*ac, *ca}); err == nil {
			t.Fatal("cycle across edge types should be rejected")
		}
	})

	t.Run("长链", func(t *testing.T) {
		n1, _ := NewNode("n1", 1, "N1")
		n2, _ := NewNode("n2", 2, "N2")
		n3, _ := NewNode("n3", 3, "N3")
		e12, _ := NewEdge("n1", "n2", EdgeStatusSuccess)
		e23, _ := NewEdge("n2", "n3", EdgeStatusSuccess)
		if _, err := New("chain", "", 0, false, 1, []*DagNode{n1, n2, n3},
			[]Edge{*e12, *e23}); err != nil {
			t.Fatalf("chain should be valid: %v", err)
		}
	})
}

// ---------- 拓扑排序 ----------

func TestTopoOrder(t *testing.T) {
	t.Run("菱形图同层保持定义顺序", func(t *testing.T) {
		d := diamond(t)
		got := joinIDs(d.TopoOrder())
		if got != "a,b,c,d" {
			t.Fatalf("topo order = %q, want a,b,c,d", got)
		}
	})

	t.Run("入口不在队首时仍按依赖层出列", func(t *testing.T) {
		// 定义顺序故意打乱：d 在 a 前面，拓扑序必须仍是 a → b,c → d
		nodeA, _ := NewNode("a", 1, "A")
		nodeB, _ := NewNode("b", 2, "B")
		nodeC, _ := NewNode("c", 3, "C")
		nodeD, _ := NewNode("d", 4, "D")
		ab, _ := NewEdge("a", "b", EdgeStatusSuccess)
		ac, _ := NewEdge("a", "c", EdgeStatusSuccess)
		bd, _ := NewEdge("b", "d", EdgeStatusSuccess)
		cd, _ := NewEdge("c", "d", EdgeStatusSuccess)
		d, err := New("shuffled", "", 0, false, 1,
			[]*DagNode{nodeD, nodeC, nodeB, nodeA}, []Edge{*ab, *ac, *bd, *cd})
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if got := joinIDs(d.TopoOrder()); got != "a,b,c,d" {
			t.Fatalf("topo order = %q, want a,b,c,d", got)
		}
	})

	t.Run("单节点", func(t *testing.T) {
		only, _ := NewNode("solo", 1, "S")
		d, err := New("solo", "", 0, false, 1, []*DagNode{only}, nil)
		if err != nil {
			t.Fatalf("single node should be valid: %v", err)
		}
		if got := joinIDs(d.TopoOrder()); got != "solo" {
			t.Fatalf("topo order = %q, want solo", got)
		}
	})
}

// ---------- 装配后的查询 ----------

func TestIncomingEdges(t *testing.T) {
	d := diamond(t)
	in := d.Incoming("d")
	if len(in) != 2 {
		t.Fatalf("node d should have 2 incoming edges, got %d", len(in))
	}
	for _, e := range in {
		if e.ToNodeID() != "d" {
			t.Fatalf("incoming edge must end at d, got %+v", e)
		}
	}
	if len(d.Incoming("a")) != 0 {
		t.Fatal("root node should have no incoming edges")
	}
}

func TestNodeLookup(t *testing.T) {
	d := diamond(t)
	if n := d.Node("b"); n == nil || n.TaskID() != 2 {
		t.Fatalf("lookup b failed: %+v", n)
	}
	if d.Node("ghost") != nil {
		t.Fatal("unknown node should return nil")
	}
}

// ---------- 其他定义不变量 ----------

func TestDefinitionValidation(t *testing.T) {
	nodeA, _ := NewNode("a", 1, "A")
	if _, err := New("", "", 0, false, 1, []*DagNode{nodeA}, nil); err == nil ||
		!strings.Contains(err.Error(), "name is empty") {
		t.Fatalf("empty name should be rejected, got %v", err)
	}
	if _, err := New("x", "", -1, false, 1, []*DagNode{nodeA}, nil); err == nil ||
		!strings.Contains(err.Error(), "negative parallel_limit") {
		t.Fatalf("negative parallel_limit should be rejected, got %v", err)
	}
	if _, err := New("x", "", 0, false, 1, nil, nil); err == nil ||
		!strings.Contains(err.Error(), "no nodes") {
		t.Fatalf("empty node set should be rejected, got %v", err)
	}
}

func TestRebuildRevalidates(t *testing.T) {
	nodeA, _ := NewNode("a", 1, "A")
	nodeB, _ := NewNode("b", 2, "B")
	ab, _ := NewEdge("a", "b", EdgeStatusSuccess)
	ba, _ := NewEdge("b", "a", EdgeStatusSuccess) // 落库后被人改出环
	if _, err := Rebuild(42, "broken", "", 0, false, DefEnabled, 1, zero(), zero(),
		[]*DagNode{nodeA, nodeB}, []Edge{*ab, *ba}); err == nil {
		t.Fatal("rebuild should revalidate and reject the cycle")
	}
}

// ---------- 定义态状态机（启停删） ----------

func TestDefStatusLifecycle(t *testing.T) {
	d := diamond(t)
	if !d.Active() {
		t.Fatal("new dag should be active")
	}
	now := time.Now()
	if err := d.MarkDeleted(now); err != nil {
		t.Fatalf("mark deleted: %v", err)
	}
	if d.Active() {
		t.Fatal("deleted dag should not be active")
	}
	if err := d.Enable(now); err == nil {
		t.Fatal("deleted dag cannot be re-enabled")
	}
	if err := d.Disable(now); err == nil {
		t.Fatal("deleted dag cannot be disabled")
	}
}
