package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"job-scheduler/app/pkg/snowflake"
	"strings"

	"job-scheduler/app/internal/domain/dag"
	"job-scheduler/app/internal/infra/mysql"
	"job-scheduler/app/internal/infra/mysql/sqlx/model"

	"github.com/jmoiron/sqlx"
)

var (
	_ dag.DagRepo          = (*DagRepo)(nil)
	_ dag.DagExecutionRepo = (*DagExecutionRepo)(nil)
)

type DagRepo struct{ db *sqlx.DB }
type DagExecutionRepo struct{ db *sqlx.DB }

func NewDagRepo(dagDB mysql.DagDB) *DagRepo {
	return &DagRepo{db: dagDB.DB}
}
func NewDagExecutionRepo(dagDB mysql.DagDB) *DagExecutionRepo {
	return &DagExecutionRepo{db: dagDB.DB}
}

// dag_nodes/dag_edges/dag_node_executions 的 id 领域不关心，由本层生成（无自增）
func assignID() int64 { return snowflake.GenID() }

// ==================== 定义态：DagRepo ====================

// Create 同事务写入 dags + dag_nodes + dag_edges（节点/边是聚合内部结构，
// 分开写会出现"图只有一半"的可见中间态）。
// created_at/updated_at 交给 DB 默认值，避免零值时间在严格模式下被拒
func (dg *DagRepo) Create(ctx context.Context, d *dag.Dag) error {
	if d.ID() <= 0 {
		return fmt.Errorf("创建 DAG 失败: 实体缺少 id，需由调用方分配")
	}
	tx, err := dg.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("写入 dags 失败: %w", err)
	}
	defer tx.Rollback() // 已提交后调用是 no-op

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO dags (id, name, description, parallel_limit, fail_fast, status, created_by)
		 VALUES (?,?,?,?,?,?,?)`,
		d.ID(), d.Name(), d.Description(), d.ParallelLimit(), d.FailFast(),
		int32(d.Status()), d.CreatedBy()); err != nil {
		return fmt.Errorf("写入 dags 失败: %w", err)
	}
	if err := insertDagChildren(ctx, tx, d); err != nil {
		return err
	}
	return tx.Commit()
}

// Update 改主行 + 整体替换节点集与边集。拓扑只能整体替换（改一条边就可能成环），
// 同事务内先删后建，外界看不到中间态。
// 软删（status=3）的定义不允许再改；dags 的软删除列只由 gen 侧查询过滤，
// 裸表写路径沿用 gorm 版语义（status 条件把关）
func (dg *DagRepo) Update(ctx context.Context, d *dag.Dag) error {
	tx, err := dg.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("更新 dags 失败: %w", err)
	}
	defer tx.Rollback()

	info, err := tx.ExecContext(ctx,
		`UPDATE dags SET name = ?, description = ?, parallel_limit = ?, fail_fast = ?, status = ?,
		        deleted_at = IF(status = ?, NOW(3), NULL)
		 WHERE id = ? AND status <> ?`,
		d.Name(), d.Description(), d.ParallelLimit(), d.FailFast(), int32(d.Status()),
		int32(dag.DefDeleted), d.ID(), int32(dag.DefDeleted))
	if err != nil {
		return fmt.Errorf("更新 dags 失败: %w", err)
	}
	if n, err := info.RowsAffected(); err != nil {
		return fmt.Errorf("更新 dags 失败: %w", err)
	} else if n == 0 {
		return fmt.Errorf("%w: dag id=%d", dag.ErrNotFound, d.ID())
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM dag_nodes WHERE dag_id = ?`, d.ID()); err != nil {
		return fmt.Errorf("清理旧 dag_nodes 失败: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM dag_edges WHERE dag_id = ?`, d.ID()); err != nil {
		return fmt.Errorf("清理旧 dag_edges 失败: %w", err)
	}
	if err := insertDagChildren(ctx, tx, d); err != nil {
		return err
	}
	return tx.Commit()
}

func (dg *DagRepo) Get(ctx context.Context, id int64) (*dag.Dag, error) {
	return loadDagAggregate(ctx, dg.db, id, true)
}

// insertDagChildren 写入节点集与边集。sort_order 取定义顺序，
// 回读时按它排序，保证 Get 出来的图与创建时同序。
// node_type 暂不建模（领域只有任务节点），走列默认值 1
func insertDagChildren(ctx context.Context, tx *sqlx.Tx, d *dag.Dag) error {
	for i, n := range d.Nodes() {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO dag_nodes (id, dag_id, task_id, node_id, node_name, sort_order)
			 VALUES (?,?,?,?,?,?)`,
			assignID(), d.ID(), n.TaskID(), n.NodeID(), n.Name(), int32(i)); err != nil {
			return fmt.Errorf("写入 dag_nodes 失败: %w", err)
		}
	}
	for _, e := range d.Edges() {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO dag_edges (id, dag_id, from_node_id, to_node_id, edge_type)
			 VALUES (?,?,?,?,?)`,
			assignID(), d.ID(), e.FromNodeID(), e.ToNodeID(), int32(e.EdgeType())); err != nil {
			return fmt.Errorf("写入 dag_edges 失败: %w", err)
		}
	}
	return nil
}

// loadDagAggregate 读 dags + dag_nodes + dag_edges 装配完整聚合。
// filterDeleted=true 时过滤软删定义（api 的定义态查询）；
// 运行态装配必须传 false——定义被软删后，其未完成的运行仍要能推进到终态，
// 否则运行会卡死 Running 且引擎每轮报装配失败
func loadDagAggregate(ctx context.Context, db *sqlx.DB, id int64, filterDeleted bool) (*dag.Dag, error) {
	q := `SELECT * FROM dags WHERE id = ?`
	if filterDeleted {
		q += ` AND deleted_at IS NULL`
	}
	var po model.Dag
	err := db.GetContext(ctx, &po, q, id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("%w: dag id=%d", dag.ErrNotFound, id)
	case err != nil:
		return nil, fmt.Errorf("查询 DAG 失败: %w", err)
	}

	var nodePOs []*model.DagNode
	if err := db.SelectContext(ctx, &nodePOs,
		`SELECT * FROM dag_nodes WHERE dag_id = ? ORDER BY sort_order`, id); err != nil {
		return nil, fmt.Errorf("查询 DAG 节点失败: %w", err)
	}
	var edgePOs []*model.DagEdge
	if err := db.SelectContext(ctx, &edgePOs,
		`SELECT * FROM dag_edges WHERE dag_id = ?`, id); err != nil {
		return nil, fmt.Errorf("查询 DAG 边失败: %w", err)
	}
	return dagFromPO(&po, nodePOs, edgePOs)
}

func dagFromPO(po *model.Dag, nodePOs []*model.DagNode, edgePOs []*model.DagEdge) (*dag.Dag, error) {
	nodes := make([]*dag.DagNode, 0, len(nodePOs))
	for _, npo := range nodePOs {
		n, err := dag.NewNode(npo.NodeID, npo.TaskID.Int64, npo.NodeName)
		if err != nil {
			return nil, fmt.Errorf("重建 DAG %d 节点失败: %w", po.ID, err)
		}
		nodes = append(nodes, n)
	}
	edges := make([]dag.Edge, 0, len(edgePOs))
	for _, epo := range edgePOs {
		e, err := dag.NewEdge(epo.FromNodeID, epo.ToNodeID, dag.EdgeStatus(epo.EdgeType))
		if err != nil {
			return nil, fmt.Errorf("重建 DAG %d 边失败: %w", po.ID, err)
		}
		edges = append(edges, *e)
	}
	d, err := dag.Rebuild(po.ID, po.Name, po.Description.String, po.ParallelLimit, po.FailFast,
		dag.DefStatus(po.Status), po.CreatedBy, po.CreatedAt, po.UpdatedAt, nodes, edges)
	if err != nil {
		return nil, fmt.Errorf("重建 DAG %d 失败: %w", po.ID, err)
	}
	return d, nil
}

// ==================== 运行态：DagExecutionRepo ====================

// Create 写入运行行 + 每个节点一条 Waiting 记录（同事务）。
// 节点记录一次性建齐，之后只按 (dag_execution_id, node_id) 推进状态
func (dge *DagExecutionRepo) Create(ctx context.Context, de *dag.DagExecution) error {
	if de.ID() <= 0 {
		return fmt.Errorf("创建 DAG 运行失败: 实体缺少 id，需由调用方分配")
	}
	tx, err := dge.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("写入 dag_executions 失败: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO dag_executions
		 (id, dag_id, status, triggered_by, context, start_time, end_time, duration_ms)
		 VALUES (?,?,?,?,?,?,?,?)`,
		de.ID(), de.DagID(), int32(de.Status()), int32(de.TriggeredBy()),
		resultValue(de.Ctx()), timeValue(de.StartTime()), timeValue(de.EndTime()),
		int32(de.DurationMS())); err != nil {
		return fmt.Errorf("写入 dag_executions 失败: %w", err)
	}
	for _, ne := range de.NodeExecutions() {
		// execution_id/input_context/output_context 尚未产生，留给列默认 NULL
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO dag_node_executions (id, dag_execution_id, node_id, status)
			 VALUES (?,?,?,?)`,
			assignID(), de.ID(), ne.NodeID(), int32(dag.DagNodeStatusWaiting)); err != nil {
			return fmt.Errorf("写入 dag_node_executions 失败: %w", err)
		}
	}
	return tx.Commit()
}

// Update 推进一次运行：主行 + 已离开 Waiting 的节点行（同事务）。
//
// 并发安全（多实例/重投都靠这两道守卫，表上没有 version 列可用）：
//   - 主行落终态时要求 DB 中仍是 Running，两个实例只有先到的能收尾；
//   - 节点行按 (dag_execution_id, node_id) 定位，并带上"合法前驱状态"
//     条件（与领域迁移表一致：Waiting→Running/Skipped，Running→Success/Failed），
//     使迟到的陈旧实体无法把已终结的节点写回 Running。
//
// RowsAffected=0 表示已被其他执行者推进：不报错，视为本实例落后（引擎持
// per-run 锁，正常不会走到这里）
func (dge *DagExecutionRepo) Update(ctx context.Context, de *dag.DagExecution) error {
	tx, err := dge.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("更新 dag_executions 失败: %w", err)
	}
	defer tx.Rollback()

	// 非终态分支同样带 status 守卫（要求 DB 仍是 Running）：
	// 否则一个拿着过期 Running 实体的实例（锁 TTL 过期后仍在推进）会把
	// 其他实例已收尾的运行改回 Running——终态被复活后引擎会永远推进它
	query := `UPDATE dag_executions SET status = ? WHERE id = ? AND status = ?`
	args := []interface{}{int32(de.Status()), de.ID(), int32(dag.DagExecStatusRunning)}
	if de.Status().IsTerminal() {
		// end_time/duration_ms 只在收尾时写：未终结的实体里这两个值是零值，
		// 写进去会把终态时间抹成 NULL
		query = `UPDATE dag_executions SET status = ?, end_time = ?, duration_ms = ? WHERE id = ? AND status = ?`
		args = []interface{}{int32(de.Status()), timeValue(de.EndTime()),
			int32(de.DurationMS()), de.ID(), int32(dag.DagExecStatusRunning)}
	}
	info, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("更新 dag_executions 失败: %w", err)
	}
	if n, err := info.RowsAffected(); err != nil {
		return fmt.Errorf("更新 dag_executions 失败: %w", err)
	} else if n == 0 {
		if err := ensureAlreadyAt(ctx, tx, de); err != nil {
			return err
		}
	}
	for _, ne := range de.NodeExecutions() {
		// Waiting 且无 execution 的节点与 Create 写入的完全一致，无需回写
		if ne.Status() == dag.DagNodeStatusWaiting && ne.ExecID() == 0 {
			continue
		}
		if err := updateNodeExecution(ctx, tx, de.ID(), ne); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ensureAlreadyAt 主行没被本次改动时分清两种情形：
// 目标状态与库中一致 → 幂等成功（引擎崩溃重放）；否则是状态冲突
func ensureAlreadyAt(ctx context.Context, tx *sqlx.Tx, de *dag.DagExecution) error {
	var cur struct {
		Status int32 `db:"status"`
	}
	err := tx.GetContext(ctx, &cur,
		`SELECT status FROM dag_executions WHERE id = ?`, de.ID())
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("%w: dag_execution id=%d", dag.ErrNotFound, de.ID())
	case err != nil:
		return fmt.Errorf("回查 dag_executions 失败: %w", err)
	}
	if dag.DagExecStatus(cur.Status) != de.Status() {
		return fmt.Errorf("%w: dag_execution id=%d db=%d want=%d",
			dag.ErrWrongStatus, de.ID(), cur.Status, de.Status())
	}
	return nil
}

// updateNodeExecution 单节点 CAS 推进，前驱集合取自领域迁移表
func updateNodeExecution(ctx context.Context, tx *sqlx.Tx, dagExecID int64, ne *dag.NodeExecution) error {
	from := allowedPredecessors(ne.Status())
	if len(from) == 0 {
		return fmt.Errorf("%w: node %q cannot be persisted in status %d",
			dag.ErrWrongStatus, ne.NodeID(), ne.Status())
	}
	// status IN (...) 占位符按前驱个数展开
	inPh := strings.Repeat("?,", len(from))
	inPh = inPh[:len(inPh)-1]
	args := []interface{}{int32(ne.Status()), execValue(ne.ExecID()),
		timeValue(ne.StartTime()), timeValue(ne.EndTime()),
		resultValue(ne.Input()), resultValue(ne.Output()),
		dagExecID, ne.NodeID()}
	args = append(args, fromArgs(from)...)
	info, err := tx.ExecContext(ctx,
		`UPDATE dag_node_executions
		 SET status = ?, execution_id = ?, start_time = ?, end_time = ?, input_context = ?, output_context = ?
		 WHERE dag_execution_id = ? AND node_id = ? AND status IN (`+inPh+`)`,
		args...)
	if err != nil {
		return fmt.Errorf("推进节点 %q 失败: %w", ne.NodeID(), err)
	}
	// RowsAffected=0：别的执行者已把这个节点推进过了，保持对方的结果
	_ = info
	return nil
}

// fromArgs 前驱状态切片 → SQL 参数
func fromArgs(from []int32) []interface{} {
	args := make([]interface{}, 0, len(from))
	for _, s := range from {
		args = append(args, s)
	}
	return args
}

// allowedPredecessors 目标状态的合法前驱（对应领域 canNodeTransition 的逆映射）
func allowedPredecessors(to dag.DagNodeStatus) []int32 {
	switch to {
	case dag.DagNodeStatusRunning:
		return []int32{int32(dag.DagNodeStatusWaiting)}
	case dag.DagNodeStatusSuccess, dag.DagNodeStatusFailed:
		return []int32{int32(dag.DagNodeStatusRunning)}
	case dag.DagNodeStatusSkipped:
		return []int32{int32(dag.DagNodeStatusWaiting)}
	default: // Waiting 不可作为迁移目标
		return nil
	}
}

func (dge *DagExecutionRepo) Get(ctx context.Context, id int64) (*dag.DagExecution, error) {
	var po model.DagExecution
	err := dge.db.GetContext(ctx, &po,
		`SELECT * FROM dag_executions WHERE id = ?`, id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("%w: dag_execution id=%d", dag.ErrNotFound, id)
	case err != nil:
		return nil, fmt.Errorf("查询 DAG 运行失败: %w", err)
	}
	defs, err := dge.loadDagAggregates(ctx, []int64{po.DagID})
	if err != nil {
		return nil, fmt.Errorf("加载运行 %d 的 DAG 定义失败: %w", po.ID, err)
	}
	def, ok := defs[po.DagID]
	if !ok {
		return nil, fmt.Errorf("%w: dag id=%d（运行 %d 的定义缺失）", dag.ErrNotFound, po.DagID, po.ID)
	}
	nodeRows, err := loadNodeExecutions(ctx, dge.db, []int64{po.ID})
	if err != nil {
		return nil, err
	}
	return buildDagExecution(def, &po, nodeRows[po.ID])
}

// ListRunning 引擎的扫描入口：所有仍在运行的 DAG 运行（含其定义与节点记录）。
// 批量预取：定义（dags/dag_nodes/dag_edges）与节点运行记录各一次 IN 查询，
// 内存装配——否则每个运行 4 条查询，100 个运行就是每轮 400+ 条。
// 单个运行装配失败（定义被运行期间结构性修改）只进 broken，绝不放弃整批：
// DB 瞬时错误才走 err 让下轮重试
func (dge *DagExecutionRepo) ListRunning(ctx context.Context, limit int) (
	[]*dag.DagExecution, []dag.BrokenRun, error,
) {
	var pos []*model.DagExecution
	err := dge.db.SelectContext(ctx, &pos,
		`SELECT * FROM dag_executions WHERE status = ? ORDER BY created_at LIMIT ?`,
		int32(dag.DagExecStatusRunning), limit)
	if err != nil {
		return nil, nil, fmt.Errorf("扫描运行中的 DAG 失败: %w", err)
	}
	return dge.assembleRuns(ctx, pos)
}

// ListStaleRunning 死运行收割的扫描入口：仍 Running、updated_at 早于阈值、
// 且已无 Running 节点的运行（还有在飞执行的轮不到 DAG 收割，交给 execution 侧）。
// broken 语义同 ListRunning
func (dge *DagExecutionRepo) ListStaleRunning(ctx context.Context, olderThan time.Time,
	limit int) ([]*dag.DagExecution, []dag.BrokenRun, error) {
	var pos []*model.DagExecution
	err := dge.db.SelectContext(ctx, &pos,
		`SELECT * FROM dag_executions de
		 WHERE de.status = ? AND de.updated_at < ?
		   AND NOT EXISTS (SELECT 1 FROM dag_node_executions ne
		                   WHERE ne.dag_execution_id = de.id AND ne.status = ?)
		 ORDER BY de.created_at LIMIT ?`,
		int32(dag.DagExecStatusRunning), olderThan, int32(dag.DagNodeStatusRunning), limit)
	if err != nil {
		return nil, nil, fmt.Errorf("扫描卡死的 DAG 运行失败: %w", err)
	}
	return dge.assembleRuns(ctx, pos)
}

// assembleRuns 批量装配：定义一次 IN、节点记录一次 IN，按外键分组回填。
// 定义加载不做软删过滤（见 loadDagAggregate 注释）。
// 单运行装配失败（定义与节点记录不一致、定义缺失）→ 记入 broken 交引擎隔离；
// 这类失败是确定性的，下轮还会失败，但只影响该运行自身
func (dge *DagExecutionRepo) assembleRuns(ctx context.Context, pos []*model.DagExecution) (
	[]*dag.DagExecution, []dag.BrokenRun, error,
) {
	if len(pos) == 0 {
		return nil, nil, nil
	}
	defs, err := dge.loadDagAggregates(ctx, dagIDsOf(pos))
	if err != nil {
		return nil, nil, err
	}

	runIDs := make([]int64, 0, len(pos))
	for _, po := range pos {
		runIDs = append(runIDs, po.ID)
	}
	nodeRows, err := loadNodeExecutions(ctx, dge.db, runIDs)
	if err != nil {
		return nil, nil, err
	}

	list := make([]*dag.DagExecution, 0, len(pos))
	var broken []dag.BrokenRun
	for _, po := range pos {
		def, ok := defs[po.DagID]
		if !ok {
			broken = append(broken, dag.BrokenRun{
				RunID: po.ID, DagID: po.DagID,
				Err: fmt.Errorf("运行的定义加载失败(dag id=%d)", po.DagID),
			})
			continue
		}
		de, err := buildDagExecution(def, po, nodeRows[po.ID])
		if err != nil {
			broken = append(broken, dag.BrokenRun{RunID: po.ID, DagID: po.DagID, Err: err})
			continue
		}
		list = append(list, de)
	}
	return list, broken, nil
}

// MarkRunDead 把无法装配的运行强制收场为 Failed：绕过装配按主键直写。
// running 态守卫保证只收运行中的行（重复调用/已被并发收场 → 0 行，幂等）
func (dge *DagExecutionRepo) MarkRunDead(ctx context.Context, runID int64) error {
	res, err := dge.db.ExecContext(ctx,
		`UPDATE dag_executions
		    SET status = ?, end_time = NOW(3),
		        duration_ms = TIMESTAMPDIFF(MICROSECOND, start_time, NOW(3)) DIV 1000
		  WHERE id = ? AND status = ?`,
		int32(dag.DagExecStatusFailed), runID, int32(dag.DagExecStatusRunning))
	if err != nil {
		return fmt.Errorf("隔离无法装配的 DAG 运行 %d 失败: %w", runID, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("dag run %d 不在 Running 态(可能已被收场)", runID)
	}
	return nil
}

func dagIDsOf(pos []*model.DagExecution) []int64 {
	ids := make([]int64, 0, len(pos))
	seen := make(map[int64]bool, len(pos))
	for _, po := range pos {
		if !seen[po.DagID] {
			seen[po.DagID] = true
			ids = append(ids, po.DagID)
		}
	}
	return ids
}

// loadNodeExecutions 一次查回多个运行的节点记录，按 dag_execution_id 分组
func loadNodeExecutions(ctx context.Context, db *sqlx.DB, runIDs []int64) (map[int64][]*model.DagNodeExecution, error) {
	if len(runIDs) == 0 {
		return nil, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(runIDs)), ",")
	args := make([]interface{}, 0, len(runIDs))
	for _, id := range runIDs {
		args = append(args, id)
	}
	var pos []*model.DagNodeExecution
	if err := db.SelectContext(ctx, &pos,
		`SELECT * FROM dag_node_executions WHERE dag_execution_id IN (`+ph+`)`, args...); err != nil {
		return nil, fmt.Errorf("批量查询节点记录失败: %w", err)
	}
	grouped := make(map[int64][]*model.DagNodeExecution, len(runIDs))
	for _, po := range pos {
		grouped[po.DagExecutionID] = append(grouped[po.DagExecutionID], po)
	}
	return grouped, nil
}

// loadDagAggregates 批量装配定义（ListRunning/ListStaleRunning 的批量预取），
// 返回 dagID → 聚合。定义加载不做软删过滤（运行态路径，见 loadDagAggregate 注释）
func (dge *DagExecutionRepo) loadDagAggregates(ctx context.Context, dagIDs []int64) (map[int64]*dag.Dag, error) {
	defs := make(map[int64]*dag.Dag, len(dagIDs))
	if len(dagIDs) == 0 {
		return defs, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(dagIDs)), ",")
	args := make([]interface{}, 0, len(dagIDs))
	for _, id := range dagIDs {
		args = append(args, id)
	}
	var dagPOs []*model.Dag
	if err := dge.db.SelectContext(ctx, &dagPOs,
		`SELECT * FROM dags WHERE id IN (`+ph+`)`, args...); err != nil {
		return nil, fmt.Errorf("批量查询 DAG 定义失败: %w", err)
	}
	var nodePOs []*model.DagNode
	if err := dge.db.SelectContext(ctx, &nodePOs,
		`SELECT * FROM dag_nodes WHERE dag_id IN (`+ph+`) ORDER BY sort_order`, args...); err != nil {
		return nil, fmt.Errorf("批量查询 DAG 节点失败: %w", err)
	}
	var edgePOs []*model.DagEdge
	if err := dge.db.SelectContext(ctx, &edgePOs,
		`SELECT * FROM dag_edges WHERE dag_id IN (`+ph+`)`, args...); err != nil {
		return nil, fmt.Errorf("批量查询 DAG 边失败: %w", err)
	}

	// 按外键分组后逐个装配（复用单聚合的行 → 实体逻辑）
	nodesByDag := make(map[int64][]*model.DagNode, len(dagIDs))
	for _, npo := range nodePOs {
		nodesByDag[npo.DagID] = append(nodesByDag[npo.DagID], npo)
	}
	edgesByDag := make(map[int64][]*model.DagEdge, len(dagIDs))
	for _, epo := range edgePOs {
		edgesByDag[epo.DagID] = append(edgesByDag[epo.DagID], epo)
	}
	for _, po := range dagPOs {
		d, err := dagFromPO(po, nodesByDag[po.ID], edgesByDag[po.ID])
		if err != nil {
			return nil, err
		}
		defs[po.ID] = d
	}
	return defs, nil
}

// buildDagExecution 纯装配：运行行 + 所属定义 + 节点记录 → 运行态实体
// （查询已上移到 assembleRuns / Get）
func buildDagExecution(def *dag.Dag, po *model.DagExecution,
	nodeRows []*model.DagNodeExecution) (*dag.DagExecution, error) {
	nodeExecs := make([]*dag.NodeExecution, 0, len(nodeRows))
	for _, npo := range nodeRows {
		ne, err := dag.NewNodeExec(npo.NodeID, dag.DagNodeStatus(npo.Status), npo.ExecutionID.Int64,
			jsonOrNull(npo.InputContext.String), jsonOrNull(npo.OutputContext.String),
			npo.StartTime.Time, npo.EndTime.Time)
		if err != nil {
			return nil, fmt.Errorf("重建运行 %d 节点 %q 失败: %w", po.ID, npo.NodeID, err)
		}
		nodeExecs = append(nodeExecs, ne)
	}
	de, err := dag.RebuildExecution(def, po.ID, dag.DagExecStatus(po.Status),
		dag.TriggerType(po.TriggeredBy), jsonOrNull(po.Context.String), po.StartTime.Time, po.EndTime.Time,
		int64(po.DurationMs.Int32), nodeExecs)
	if err != nil {
		return nil, fmt.Errorf("重建 DAG 运行 %d 失败: %w", po.ID, err)
	}
	return de, nil
}
