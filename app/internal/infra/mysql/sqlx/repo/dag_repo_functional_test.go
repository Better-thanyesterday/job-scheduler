package sqlx

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql" // 真库功能测试的驱动注册
	"github.com/jmoiron/sqlx"

	"job-scheduler/app/internal/domain/dag"
	"job-scheduler/app/internal/infra/mysql"
)

// 功能验证（真库）：运行期间改坏 DAG 定义后的隔离语义——
// 一个无法装配的运行只进 broken，不拖垮整批扫描；
// 引擎侧 MarkRunDead 把它收场为 Failed，从此不再出现在扫描里。
// 复现并守护的 bug：装配失败曾让 ListRunning 整批失败，
// 全局在途 DAG 停摆、坏运行永久卡死只能人工改库。
// MySQL 不可达自动 SKIP
func TestDagRepoBrokenRunIsolation(t *testing.T) {
	db, err := sqlx.Connect("mysql", "root:root@tcp(127.0.0.1:3306)/job_dag?parseTime=true&loc=Local")
	if err != nil {
		t.Skipf("MySQL 不可用，跳过真库功能测试: %v", err)
	}
	defer db.Close()
	repo := NewDagExecutionRepo(mysql.DagDB{DB: db})
	ctx := context.Background()

	// 种子：单节点定义 + 两个健康运行 + 一个"幽灵节点"运行
	// （模拟定义在运行期间被改：节点记录指向新定义里不存在的节点）
	const (
		dagID  = int64(990001)
		nodeID = int64(990101)
		runA   = int64(990002) // 健康
		runB   = int64(990003) // 健康
		runC   = int64(990004) // 坏运行
		nodeA  = int64(990201)
		nodeB  = int64(990202)
		nodeC  = int64(990203)
	)
	cleanup := func() {
		db.Exec("DELETE FROM dag_node_executions WHERE dag_execution_id IN (?,?,?)", runA, runB, runC)
		db.Exec("DELETE FROM dag_executions WHERE id IN (?,?,?)", runA, runB, runC)
		db.Exec("DELETE FROM dag_nodes WHERE dag_id = ?", dagID)
		db.Exec("DELETE FROM dags WHERE id = ?", dagID)
	}
	cleanup()
	t.Cleanup(cleanup)

	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("种子 %s: %v", q, err)
		}
	}
	mustExec(`INSERT INTO dags (id, name, description, parallel_limit, fail_fast, status, created_by)
	          VALUES (?,?,?,?,?,?,?)`, dagID, "broken-isolation-test", "临时种子", 3, 0, 1, 1)
	mustExec(`INSERT INTO dag_nodes (id, dag_id, task_id, node_id, node_name, node_type, sort_order)
	          VALUES (?,?,?,?,?,?,?)`, nodeID, dagID, dagID, "n1", "节点1", 1, 0)

	newRun := func(id int64) {
		mustExec(`INSERT INTO dag_executions (id, dag_id, status, triggered_by) VALUES (?,?,?,?)`,
			id, dagID, int32(dag.DagExecStatusRunning), int32(dag.TriggerCron))
	}
	newRun(runA)
	newRun(runB)
	newRun(runC)
	// 健康运行：节点记录与定义对齐；坏运行：指向定义里不存在的 "ghost"
	mustExec(`INSERT INTO dag_node_executions (id, dag_execution_id, node_id, status) VALUES (?,?,?,?)`,
		nodeA, runA, "n1", int32(dag.DagNodeStatusWaiting))
	mustExec(`INSERT INTO dag_node_executions (id, dag_execution_id, node_id, status) VALUES (?,?,?,?)`,
		nodeB, runB, "n1", int32(dag.DagNodeStatusWaiting))
	mustExec(`INSERT INTO dag_node_executions (id, dag_execution_id, node_id, status) VALUES (?,?,?,?)`,
		nodeC, runC, "ghost", int32(dag.DagNodeStatusWaiting))

	// 1. 扫描：健康运行照常返回，坏运行进 broken（整批不失败）
	healthy, broken, err := repo.ListRunning(ctx, 100)
	if err != nil {
		t.Fatalf("ListRunning 不应因坏运行整批失败: %v", err)
	}
	healthyIDs := map[int64]bool{}
	for _, de := range healthy {
		healthyIDs[de.ID()] = true
	}
	if !healthyIDs[runA] || !healthyIDs[runB] {
		t.Fatalf("健康运行 A/B 应照常返回，实际 %v", healthyIDs)
	}
	if healthyIDs[runC] {
		t.Fatal("坏运行不应出现在 healthy 里")
	}
	foundBroken := false
	for _, b := range broken {
		if b.RunID == runC {
			foundBroken = true
			if b.Err == nil {
				t.Fatal("broken 项应携带装配失败原因")
			}
		}
	}
	if !foundBroken {
		t.Fatalf("坏运行应进 broken，实际 %v", broken)
	}

	// 2. 引擎隔离：MarkRunDead 后坏运行收场为 Failed + end_time
	if err := repo.MarkRunDead(ctx, runC); err != nil {
		t.Fatalf("MarkRunDead: %v", err)
	}
	var status int32
	var endTime sql.NullTime
	if err := db.QueryRow("SELECT status, end_time FROM dag_executions WHERE id = ?", runC).
		Scan(&status, &endTime); err != nil {
		t.Fatal(err)
	}
	if status != int32(dag.DagExecStatusFailed) || !endTime.Valid {
		t.Fatalf("坏运行应已收场为 Failed+end_time，实际 status=%d end=%v", status, endTime)
	}

	// 3. 隔离后：扫描再无 broken，健康运行不受影响
	healthy2, broken2, err := repo.ListRunning(ctx, 100)
	if err != nil {
		t.Fatalf("ListRunning: %v", err)
	}
	if len(broken2) != 0 {
		t.Fatalf("隔离后 broken 应为空，实际 %v", broken2)
	}
	for _, de := range healthy2 {
		if de.ID() == runC {
			t.Fatal("已隔离的运行不应再出现在扫描结果里")
		}
	}
	if !healthyIDsContain(healthy2, runA) || !healthyIDsContain(healthy2, runB) {
		t.Fatal("健康运行应持续返回")
	}

	// 4. 幂等：对已收场的行重复 MarkRunDead 必须报错（running 守卫）
	if err := repo.MarkRunDead(ctx, runC); err == nil {
		t.Fatal("对已收场运行重复 MarkRunDead 应报错")
	}
}

func healthyIDsContain(runs []*dag.DagExecution, id int64) bool {
	for _, de := range runs {
		if de.ID() == id {
			return true
		}
	}
	return false
}
