package dag

import (
	"strings"
	"testing"
	"time"

	"job-scheduler/app/internal/domain/execution"
)

func mustExec(t *testing.T, taskID int64) *execution.Execution {
	t.Helper()
	e, err := execution.New(taskID, 0, 0, "trace", time.Now())
	if err != nil {
		t.Fatalf("new execution: %v", err)
	}
	return e
}

func mustRun(t *testing.T) *DagExecution {
	t.Helper()
	de, err := NewExecution(diamond(t), nil, TriggerManual, time.Now())
	if err != nil {
		t.Fatalf("new dag execution: %v", err)
	}
	return de
}

// finishNode 走完整合法路径：Waiting → Running(Bind) → 终态
func finishNode(t *testing.T, de *DagExecution, nodeID string, success bool) {
	t.Helper()
	now := time.Now()
	if err := de.Node(nodeID).Bind(mustExec(t, 1), now); err != nil {
		t.Fatalf("bind %s: %v", nodeID, err)
	}
	var err error
	if success {
		err = de.Node(nodeID).FinishSuccess(nil, now)
	} else {
		err = de.Node(nodeID).FinishFailed(now)
	}
	if err != nil {
		t.Fatalf("finish %s: %v", nodeID, err)
	}
}

// ---------- 运行态构造 ----------

func TestNewExecutionAllWaiting(t *testing.T) {
	de := mustRun(t)
	if de.Status() != DagExecStatusRunning {
		t.Fatalf("initial status = %s, want Running", de.Status())
	}
	if len(de.NodeExecutions()) != 4 {
		t.Fatalf("want 4 node executions, got %d", len(de.NodeExecutions()))
	}
	for _, ne := range de.NodeExecutions() {
		if ne.Status() != DagNodeStatusWaiting || ne.ExecID() != 0 {
			t.Fatalf("node %s should be Waiting without execution, got %s/%d",
				ne.NodeID(), ne.Status(), ne.ExecID())
		}
	}
}

func TestNewExecutionValidation(t *testing.T) {
	d := diamond(t)
	if _, err := NewExecution(nil, nil, TriggerManual, time.Now()); err == nil {
		t.Fatal("nil definition should be rejected")
	}
	if _, err := NewExecution(d, nil, TriggerType(99), time.Now()); err == nil {
		t.Fatal("invalid triggered_by should be rejected")
	}
}

func TestRebuildExecutionNodeConsistency(t *testing.T) {
	d := diamond(t)

	t.Run("缺少节点记录", func(t *testing.T) {
		nodeExecs := []*NodeExecution{
			{nodeID: "a", status: DagNodeStatusSuccess},
			{nodeID: "b", status: DagNodeStatusSuccess},
		}
		if _, err := RebuildExecution(d, 9, DagExecStatusRunning, TriggerCron, nil,
			time.Now(), time.Time{}, 0, nodeExecs); err == nil {
			t.Fatal("missing node records should be rejected")
		}
	})

	t.Run("重复节点记录", func(t *testing.T) {
		nodeExecs := []*NodeExecution{
			{nodeID: "a", status: DagNodeStatusSuccess},
			{nodeID: "a", status: DagNodeStatusFailed},
		}
		if _, err := RebuildExecution(d, 9, DagExecStatusRunning, TriggerCron, nil,
			time.Now(), time.Time{}, 0, nodeExecs); err == nil {
			t.Fatal("duplicate node records should be rejected")
		}
	})

	t.Run("定义之外的节点记录", func(t *testing.T) {
		nodeExecs := []*NodeExecution{
			{nodeID: "a", status: DagNodeStatusSuccess},
			{nodeID: "b", status: DagNodeStatusWaiting},
			{nodeID: "c", status: DagNodeStatusWaiting},
			{nodeID: "ghost", status: DagNodeStatusWaiting},
		}
		if _, err := RebuildExecution(d, 9, DagExecStatusRunning, TriggerCron, nil,
			time.Now(), time.Time{}, 0, nodeExecs); err == nil {
			t.Fatal("unknown node records should be rejected")
		}
	})

	t.Run("合法重建", func(t *testing.T) {
		nodeExecs := []*NodeExecution{
			{nodeID: "a", status: DagNodeStatusSuccess},
			{nodeID: "b", status: DagNodeStatusWaiting},
			{nodeID: "c", status: DagNodeStatusWaiting},
			{nodeID: "d", status: DagNodeStatusWaiting},
		}
		de, err := RebuildExecution(d, 9, DagExecStatusRunning, TriggerCron, nil,
			time.Now(), time.Time{}, 0, nodeExecs)
		if err != nil {
			t.Fatalf("rebuild: %v", err)
		}
		if de.DagID() != d.ID() {
			t.Fatalf("dag id = %d, want %d", de.DagID(), d.ID())
		}
	})
}

// ---------- 依赖判定与派发 ----------

func TestReadyNodesRespectsDependencies(t *testing.T) {
	de := mustRun(t)

	if got := joinIDs(de.ReadyNodes()); got != "a" {
		t.Fatalf("only root should be ready, got %q", got)
	}

	// a 运行中 → b/c 仍不能跑
	if err := de.Node("a").Bind(mustExec(t, 1), time.Now()); err != nil {
		t.Fatalf("bind a: %v", err)
	}
	if len(de.ReadyNodes()) != 0 {
		t.Fatalf("no node should be ready while a is running, got %v", nodeIDs(de.ReadyNodes()))
	}

	// a 成功 → b、c 就绪
	if err := de.Node("a").FinishSuccess(nil, time.Now()); err != nil {
		t.Fatalf("finish a: %v", err)
	}
	if got := joinIDs(de.ReadyNodes()); got != "b,c" {
		t.Fatalf("b and c should be ready, got %q", got)
	}

	// 只完成 b 时 d 仍差 c
	finishNode(t, de, "b", true)
	if got := joinIDs(de.ReadyNodes()); got != "c" {
		t.Fatalf("only c should be ready, got %q", got)
	}

	// c 成功 → d 就绪（d 的另一条入边 b→d 已满足）
	finishNode(t, de, "c", true)
	if got := joinIDs(de.ReadyNodes()); got != "d" {
		t.Fatalf("d should be ready, got %q", got)
	}
}

func TestReadyNodesHonorsParallelLimit(t *testing.T) {
	nodeA, _ := NewNode("a", 1, "A")
	nodeB, _ := NewNode("b", 2, "B")
	nodeC, _ := NewNode("c", 3, "C")
	d, err := New("limited", "", 2, false, 1, []*DagNode{nodeA, nodeB, nodeC}, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	de, _ := NewExecution(d, nil, TriggerManual, time.Now())

	// 三个根节点，上限 2 → 只放行 2 个
	if got := len(de.ReadyNodes()); got != 2 {
		t.Fatalf("parallel limit 2 should cap ready nodes at 2, got %d", got)
	}

	// a 完成释放名额后，0 个运行中、2 个等待 → b、c 都放行
	finishNode(t, de, "a", true)
	if got := len(de.ReadyNodes()); got != 2 {
		t.Fatalf("b and c should be admitted after a finished, got %d", got)
	}

	// 两个名额都被占用后，不再放行
	if err := de.Node("b").Bind(mustExec(t, 2), time.Now()); err != nil {
		t.Fatalf("bind b: %v", err)
	}
	if err := de.Node("c").Bind(mustExec(t, 3), time.Now()); err != nil {
		t.Fatalf("bind c: %v", err)
	}
	if got := len(de.ReadyNodes()); got != 0 {
		t.Fatalf("parallel limit 2 with 2 running should admit nobody, got %d", got)
	}
}

func TestFailureAndAlwaysEdges(t *testing.T) {
	nodeA, _ := NewNode("a", 1, "A")
	nodeAlert, _ := NewNode("alert", 2, "告警")
	nodeReport, _ := NewNode("report", 3, "报表")
	failEdge, _ := NewEdge("a", "alert", EdgeStatusFailure)
	alwaysEdge, _ := NewEdge("a", "report", EdgeStatusAlways)
	d, err := New("branchy", "", 0, false, 1, []*DagNode{nodeA, nodeAlert, nodeReport},
		[]Edge{*failEdge, *alwaysEdge})
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	t.Run("a 失败后 Failure 边与 Always 边放行", func(t *testing.T) {
		de, _ := NewExecution(d, nil, TriggerManual, time.Now())
		finishNode(t, de, "a", false)
		got := joinIDs(de.ReadyNodes())
		// ReadyNodes 按定义顺序返回：a、alert、report 的定义序就是 a 在前
		if got != "alert,report" {
			t.Fatalf("alert and report should be ready, got %q", got)
		}
	})

	t.Run("a 成功时 Failure 边不放行", func(t *testing.T) {
		de, _ := NewExecution(d, nil, TriggerManual, time.Now())
		finishNode(t, de, "a", true)
		if got := joinIDs(de.ReadyNodes()); got != "report" {
			t.Fatalf("only report should be ready, got %q", got)
		}
	})

	t.Run("上游跳过时 Always 边放行", func(t *testing.T) {
		de, _ := NewExecution(d, nil, TriggerManual, time.Now())
		if err := de.Node("a").Skip(time.Now()); err != nil {
			t.Fatalf("skip a: %v", err)
		}
		if got := joinIDs(de.ReadyNodes()); got != "report" {
			t.Fatalf("only report should be ready after skip, got %q", got)
		}
	})
}

// ---------- 卡死判定与级联跳过 ----------

func TestStalledNodesCascade(t *testing.T) {
	nodeA, _ := NewNode("a", 1, "A")
	nodeB, _ := NewNode("b", 2, "B")
	ab, _ := NewEdge("a", "b", EdgeStatusSuccess)
	d, err := Rebuild(7, "stall", "", 0, false, DefEnabled, 1, zero(), zero(),
		[]*DagNode{nodeA, nodeB}, []Edge{*ab})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	de, _ := NewExecution(d, nil, TriggerManual, time.Now())

	// a 失败（节点级终态，不再重试）→ b 永远等不到，成为卡死节点
	finishNode(t, de, "a", false)
	if got := joinIDs(de.StalledNodes()); got != "b" {
		t.Fatalf("b should be stalled, got %q", got)
	}
	if err := de.Node("b").Skip(time.Now()); err != nil {
		t.Fatalf("skip b: %v", err)
	}
	if len(de.StalledNodes()) != 0 {
		t.Fatal("no stalled nodes after cascade skip")
	}
}

func TestShouldAbort(t *testing.T) {
	t.Run("fail_fast 开启时失败即中止", func(t *testing.T) {
		de := mustRun(t) // diamond: failFast=true
		if de.ShouldAbort() {
			t.Fatal("should not abort before any failure")
		}
		finishNode(t, de, "a", false)
		if !de.ShouldAbort() {
			t.Fatal("fail_fast dag should abort after a node failed")
		}
	})

	t.Run("fail_fast 关闭时不中止", func(t *testing.T) {
		nodeA, _ := NewNode("a", 1, "A")
		d, _ := Rebuild(8, "patient", "", 0, false, DefEnabled, 1, zero(), zero(),
			[]*DagNode{nodeA}, nil)
		de, _ := NewExecution(d, nil, TriggerManual, time.Now())
		finishNode(t, de, "a", false)
		if de.ShouldAbort() {
			t.Fatal("should not abort when fail_fast is off")
		}
	})
}

// ---------- 节点状态机 ----------

func TestNodeTransitions(t *testing.T) {
	ne, err := NewNodeExec("a", DagNodeStatusWaiting, 0, nil, nil, zero(), zero())
	if err != nil {
		t.Fatalf("new node exec: %v", err)
	}
	if err := ne.Bind(nil, time.Now()); err == nil {
		t.Fatal("bind nil execution should fail")
	}
	e := mustExec(t, 1)
	if err := ne.Bind(e, time.Now()); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if ne.ExecID() != e.ID() {
		t.Fatalf("exec id = %d, want %d", ne.ExecID(), e.ID())
	}
	if err := ne.Skip(time.Now()); err == nil ||
		!strings.Contains(err.Error(), "illegal node status transition") {
		t.Fatalf("running node cannot skip, got %v", err)
	}
	if err := ne.FinishSuccess([]byte(`{"ok":true}`), time.Now()); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if string(ne.Output()) != `{"ok":true}` {
		t.Fatalf("output = %s", ne.Output())
	}
	if err := ne.FinishFailed(time.Now()); err == nil {
		t.Fatal("terminal node cannot transition again")
	}
}

func TestNodeExecValidation(t *testing.T) {
	if _, err := NewNodeExec("a", DagNodeStatus(99), 0, nil, nil, zero(), zero()); err == nil {
		t.Fatal("invalid status should be rejected")
	}
	if _, err := NewNodeExec("a", DagNodeStatusSkipped, 5, nil, nil, zero(), zero()); err == nil {
		t.Fatal("skipped node carrying execution_id should be rejected")
	}
	if _, err := NewNodeExec("a", DagNodeStatusSkipped, 0, nil, nil, zero(), zero()); err != nil {
		t.Fatalf("skipped node without execution should be fine: %v", err)
	}
}

// ---------- 收尾归档 ----------

func TestFinalize(t *testing.T) {
	t.Run("未到终态不收尾", func(t *testing.T) {
		de := mustRun(t)
		done, err := de.Finalize(time.Now())
		if done || err != nil {
			t.Fatalf("running dag should not finalize, got done=%v err=%v", done, err)
		}
	})

	t.Run("全部成功", func(t *testing.T) {
		de := mustRun(t)
		for _, id := range []string{"a", "b", "c", "d"} {
			finishNode(t, de, id, true)
		}
		done, err := de.Finalize(time.Now())
		if !done || err != nil {
			t.Fatalf("want finalize, got done=%v err=%v", done, err)
		}
		if de.Status() != DagExecStatusSuccess {
			t.Fatalf("status = %s, want Success", de.Status())
		}
		if de.EndTime().IsZero() || de.DurationMS() < 0 {
			t.Fatal("end time / duration should be recorded")
		}
	})

	t.Run("存在失败", func(t *testing.T) {
		de := mustRun(t)
		finishNode(t, de, "a", true)
		finishNode(t, de, "b", false)
		finishNode(t, de, "c", true)
		finishNode(t, de, "d", true)
		if done, _ := de.Finalize(time.Now()); !done {
			t.Fatal("want finalize")
		}
		if de.Status() != DagExecStatusFailed {
			t.Fatalf("status = %s, want Failed", de.Status())
		}
	})

	t.Run("无失败但有跳过", func(t *testing.T) {
		de := mustRun(t)
		finishNode(t, de, "a", true)
		finishNode(t, de, "b", true)
		finishNode(t, de, "c", true)
		if err := de.Node("d").Skip(time.Now()); err != nil {
			t.Fatalf("skip d: %v", err)
		}
		if done, _ := de.Finalize(time.Now()); !done {
			t.Fatal("want finalize")
		}
		if de.Status() != DagExecStatusPartial {
			t.Fatalf("status = %s, want Partial", de.Status())
		}
	})

	t.Run("终态不可迁出", func(t *testing.T) {
		de := mustRun(t)
		for _, id := range []string{"a", "b", "c", "d"} {
			finishNode(t, de, id, true)
		}
		if done, _ := de.Finalize(time.Now()); !done {
			t.Fatal("want finalize")
		}
		if _, err := de.Finalize(time.Now()); err != nil {
			t.Fatalf("repeated finalize on terminal run should be a no-op, got %v", err)
		}
		if de.Status() != DagExecStatusSuccess {
			t.Fatalf("status must stay Success, got %s", de.Status())
		}
	})
}

// ---------- 状态迁移表 ----------

func TestStatusTransitionTables(t *testing.T) {
	if DagExecStatusRunning.IsTerminal() {
		t.Fatal("Running is not terminal")
	}
	if !DagExecStatusPartial.IsTerminal() {
		t.Fatal("Partial is terminal")
	}
	if !DagNodeStatusSkipped.IsTerminal() {
		t.Fatal("Skipped is terminal")
	}

	// 整体状态：只有 Running 能迁出
	var s DagExecStatus = DagExecStatusRunning
	if err := s.Transition(DagExecStatusFailed); err != nil {
		t.Fatalf("running -> failed should be legal: %v", err)
	}
	if err := s.Transition(DagExecStatusSuccess); err == nil {
		t.Fatal("terminal is a black hole")
	}

	// 节点状态：Waiting 不能直达终态（必须经 Running）
	var n DagNodeStatus = DagNodeStatusWaiting
	if err := n.Transition(DagNodeStatusSuccess); err == nil {
		t.Fatal("waiting -> success should be illegal")
	}
	if err := n.Transition(DagNodeStatusSkipped); err != nil {
		t.Fatalf("waiting -> skipped should be legal: %v", err)
	}
}

// ---------- 死运行收割 ----------

func TestReap(t *testing.T) {
	t.Run("无 Running 节点的卡死运行被强制收场为 Failed", func(t *testing.T) {
		de := mustRun(t)
		// a 成功、b/c/d 全部还在等（比如任务快照永久取不到）
		finishNode(t, de, "a", true)

		changed, err := de.Reap(time.Now())
		if err != nil || !changed {
			t.Fatalf("want reaped, got changed=%v err=%v", changed, err)
		}
		if de.Status() != DagExecStatusFailed {
			t.Fatalf("status = %s, want Failed（收割不走 Partial 语义）", de.Status())
		}
		for _, id := range []string{"b", "c", "d"} {
			if got := de.Node(id).Status(); got != DagNodeStatusSkipped {
				t.Fatalf("waiting node %s should be skipped, got %s", id, got)
			}
		}
		if de.EndTime().IsZero() {
			t.Fatal("end time should be recorded")
		}
	})

	t.Run("终态运行重复收割是 no-op", func(t *testing.T) {
		de := mustRun(t)
		if changed, _ := de.Reap(time.Now()); !changed {
			t.Fatal("first reap should change")
		}
		if changed, err := de.Reap(time.Now()); changed || err != nil {
			t.Fatalf("second reap should be no-op, got changed=%v err=%v", changed, err)
		}
	})

	t.Run("在飞的 Running 节点也被拉终(防御路径)", func(t *testing.T) {
		// 引擎不会对有 Running 节点的运行收割，但 Reap 自身要保证
		// 调用后整图必达终态，否则落库时 Running 节点与 Failed 主行并存
		de := mustRun(t)
		if err := de.Node("a").Bind(mustExec(t, 1), time.Now()); err != nil {
			t.Fatalf("bind a: %v", err)
		}
		if changed, err := de.Reap(time.Now()); !changed || err != nil {
			t.Fatalf("want reaped, got changed=%v err=%v", changed, err)
		}
		if got := de.Node("a").Status(); got != DagNodeStatusFailed {
			t.Fatalf("running node should be failed, got %s", got)
		}
		if de.Status() != DagExecStatusFailed {
			t.Fatalf("status = %s", de.Status())
		}
	})
}

// ---------- 节点输入（数据流） ----------

func TestNodeInput(t *testing.T) {
	ne, _ := NewNodeExec("b", DagNodeStatusWaiting, 0, nil, nil, zero(), zero())
	if len(ne.Input()) != 0 {
		t.Fatal("fresh node should have no input")
	}
	if err := ne.SetInput([]byte(`{"a":{"ok":true}}`)); err != nil {
		t.Fatalf("set input: %v", err)
	}
	if string(ne.Input()) != `{"a":{"ok":true}}` {
		t.Fatalf("input = %s", ne.Input())
	}
	// 进入 Running 后输入固化：再改是前后不一致，必须被拒
	if err := ne.Bind(mustExec(t, 1), time.Now()); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := ne.SetInput([]byte(`{}`)); err == nil {
		t.Fatal("input is immutable once running")
	}
}

func zero() time.Time { return time.Time{} }
