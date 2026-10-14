package logic

import (
	"strings"
	"testing"

	"job-scheduler/app/internal/api/types"
	"job-scheduler/app/internal/domain/dag"
)

func yamlIn(name string, nodes []types.DagNodeYAML, edges []types.DagEdgeYAML) *types.DagYAML {
	return &types.DagYAML{Name: name, Nodes: nodes, Edges: edges}
}

func node(id string, taskID int64) types.DagNodeYAML {
	return types.DagNodeYAML{ID: id, TaskID: taskID, Name: "node-" + id}
}

func edge(from, to, typ string) types.DagEdgeYAML {
	return types.DagEdgeYAML{From: from, To: to, Type: typ}
}

func TestBuildDagFromYAML(t *testing.T) {
	t.Run("合法菱形图（缺省边类型=success、缺省 fail_fast=true）", func(t *testing.T) {
		in := yamlIn("pipe",
			[]types.DagNodeYAML{node("a", 1), node("b", 2), node("c", 3), node("d", 4)},
			[]types.DagEdgeYAML{edge("a", "b", ""), edge("a", "c", "success"), edge("b", "d", "ALWAYS"), edge("c", "d", "failure")})
		d, err := buildDagFromYAML(in, 42)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if d.Name() != "pipe" || !d.FailFast() || d.CreatedBy() != 42 {
			t.Fatalf("fields wrong: %+v", d)
		}
		if len(d.Nodes()) != 4 || len(d.Edges()) != 4 {
			t.Fatalf("shape wrong: %d nodes %d edges", len(d.Nodes()), len(d.Edges()))
		}
		// 缺省/大小写混合的边类型都要解析对
		if got := d.Edges()[0].EdgeType(); got != dag.EdgeStatusSuccess {
			t.Fatalf("empty type should default to success, got %v", got)
		}
		if got := d.Edges()[2].EdgeType(); got != dag.EdgeStatusAlways {
			t.Fatalf("ALWAYS should parse case-insensitively, got %v", got)
		}
		if got := d.Edges()[3].EdgeType(); got != dag.EdgeStatusFailure {
			t.Fatalf("failure should parse, got %v", got)
		}
	})

	t.Run("fail_fast 显式关闭", func(t *testing.T) {
		in := yamlIn("x", []types.DagNodeYAML{node("a", 1)}, nil)
		f := false
		in.FailFast = &f
		d, err := buildDagFromYAML(in, 1)
		if err != nil || d.FailFast() {
			t.Fatalf("fail_fast=false not honored: %v %v", d.FailFast(), err)
		}
	})

	t.Run("依赖环被拒且错误标记为输入非法", func(t *testing.T) {
		in := yamlIn("cycle",
			[]types.DagNodeYAML{node("a", 1), node("b", 2)},
			[]types.DagEdgeYAML{edge("a", "b", "success"), edge("b", "a", "success")})
		_, err := buildDagFromYAML(in, 1)
		if err == nil {
			t.Fatal("cycle should be rejected")
		}
		if !strings.Contains(err.Error(), "dependency cycle") {
			t.Fatalf("want cycle detail, got %v", err)
		}
	})

	t.Run("悬空边被拒", func(t *testing.T) {
		in := yamlIn("dangling",
			[]types.DagNodeYAML{node("a", 1)},
			[]types.DagEdgeYAML{edge("a", "ghost", "success")})
		if _, err := buildDagFromYAML(in, 1); err == nil {
			t.Fatal("dangling edge should be rejected")
		}
	})

	t.Run("重复 nodeID 被拒", func(t *testing.T) {
		in := yamlIn("dup",
			[]types.DagNodeYAML{node("a", 1), node("a", 2)}, nil)
		if _, err := buildDagFromYAML(in, 1); err == nil {
			t.Fatal("duplicate node id should be rejected")
		}
	})

	t.Run("未知边类型被拒", func(t *testing.T) {
		in := yamlIn("badtype",
			[]types.DagNodeYAML{node("a", 1), node("b", 2)},
			[]types.DagEdgeYAML{edge("a", "b", "sometimes")})
		_, err := buildDagFromYAML(in, 1)
		if err == nil || !strings.Contains(err.Error(), "未知边类型") {
			t.Fatalf("want unknown-type error, got %v", err)
		}
	})

	t.Run("空名/空节点集被拒", func(t *testing.T) {
		if _, err := buildDagFromYAML(yamlIn("", []types.DagNodeYAML{node("a", 1)}, nil), 1); err == nil {
			t.Fatal("empty name should be rejected")
		}
		if _, err := buildDagFromYAML(yamlIn("x", nil, nil), 1); err == nil {
			t.Fatal("empty nodes should be rejected")
		}
	})
}
