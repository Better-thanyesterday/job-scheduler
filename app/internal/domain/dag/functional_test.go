package dag_test

import (
	"testing"

	"job-scheduler/app/internal/domain/dag"
)

// 功能验证：DAG 定义构建 + 依赖环检测（编排合法性的核心）
func TestDagBuildAndCycleDetection(t *testing.T) {
	node := func(id string) *dag.DagNode {
		n, err := dag.NewNode(id, 1000+int64(len(id)), "node-"+id)
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		return n
	}
	edge := func(from, to string) dag.Edge {
		e, err := dag.NewEdge(from, to, dag.EdgeStatusSuccess)
		if err != nil {
			t.Fatalf("NewEdge(%s→%s): %v", from, to, err)
		}
		return *e
	}

	// 合法链路 a→b→c
	d, err := dag.New("order-pipeline", "三节点流水线", 2, true, 42,
		[]*dag.DagNode{node("a"), node("b"), node("c")},
		[]dag.Edge{edge("a", "b"), edge("b", "c")})
	if err != nil {
		t.Fatalf("合法 DAG 构建失败: %v", err)
	}
	if len(d.Nodes()) != 3 || len(d.Edges()) != 2 {
		t.Fatalf("nodes=%d edges=%d, 期望 3/2", len(d.Nodes()), len(d.Edges()))
	}
	// 依赖查询：b 的上游只有 a
	inc := d.Incoming("b")
	if len(inc) != 1 {
		t.Fatalf("b 的入边 = %d 条, 期望 1", len(inc))
	}

	// 环 a→b→a：必须构建不出来（编排死锁在源头拒绝）
	_, err = dag.New("cycle", "有环图", 2, false, 42,
		[]*dag.DagNode{node("x"), node("y")},
		[]dag.Edge{edge("x", "y"), edge("y", "x")})
	if err == nil {
		t.Fatal("含环 DAG 应当被拒绝")
	}

	// 非法边：引用不存在的节点
	_, err = dag.New("dangling", "悬空边", 2, false, 42,
		[]*dag.DagNode{node("a")},
		[]dag.Edge{edge("a", "ghost")})
	if err == nil {
		t.Fatal("引用不存在节点的边应当被拒绝")
	}
}
