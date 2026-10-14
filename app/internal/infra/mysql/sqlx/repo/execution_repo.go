package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/infra/mysql"
	"job-scheduler/app/internal/infra/mysql/sqlx/model"

	"github.com/jmoiron/sqlx"
)

var (
	_ execution.Registry          = (*ExecutionRegistryRepo)(nil)
	_ execution.ExecutorRepo      = (*ExecutionRepo)(nil)
	_ execution.RetryPlannerRepo  = (*RetryPlannerRepo)(nil)
	_ execution.ReaperRepo        = (*ReaperRepo)(nil)
	_ execution.PendingRepushRepo = (*ExecutionRegistryRepo)(nil)
	_ execution.Query             = (*ExecutionQueryRepo)(nil)
)

const executionPageSize = 20

type ExecutionRegistryRepo struct{ db *sqlx.DB }
type ExecutionRepo struct{ db *sqlx.DB }
type RetryPlannerRepo struct{ db *sqlx.DB }
type ReaperRepo struct{ db *sqlx.DB }
type ExecutionQueryRepo struct{ db *sqlx.DB }

func NewExecutionRegistryRepo(executionDB mysql.ExecutionDB) *ExecutionRegistryRepo {
	return &ExecutionRegistryRepo{db: executionDB.DB}
}
func NewExecutionRepo(executionDB mysql.ExecutionDB) *ExecutionRepo {
	return &ExecutionRepo{db: executionDB.DB}
}
func NewRetryPlannerRepo(executionDB mysql.ExecutionDB) *RetryPlannerRepo {
	return &RetryPlannerRepo{db: executionDB.DB}
}
func NewReaperRepo(executionDB mysql.ExecutionDB) *ReaperRepo {
	return &ReaperRepo{db: executionDB.DB}
}
func NewExecutionQueryRepo(executionDB mysql.ExecutionDB) *ExecutionQueryRepo {
	return &ExecutionQueryRepo{db: executionDB.DB}
}

// ==================== 值转换 ====================

// NULL 语义辅助函数见 helpers.go（timeValue / nextRetryValue / resultValue）。
// executions 表无软删除列，查询不加 deleted_at 条件

// executionToEntity 完整回填持久化状态（version 是后续 CAS 的基准）
func executionToEntity(po *model.Execution) (*execution.Execution, error) {
	var nextRetryAt *time.Time
	if po.NextRetryAt.Valid {
		t := po.NextRetryAt.Time
		nextRetryAt = &t
	}
	version := int64(0)
	if po.Version.Valid {
		version = int64(po.Version.Int32)
	}
	return execution.Rebuild(
		po.ID, po.TaskID, po.ScheduleID.Int64, po.DagExecutionID.Int64, po.DagNodeID.String,
		execution.Status(po.Status), version,
		po.StartTime.Time, po.EndTime.Time, int64(po.DurationMs.Int32),
		[]byte(po.Result.String), po.ErrorMsg.String, int(po.RetryCount), int(po.MaxRetry),
		po.WorkerID.String, po.TraceID, nextRetryAt, po.CreatedAt,
	)
}

func executionsToEntities(pos []*model.Execution) ([]*execution.Execution, error) {
	list := make([]*execution.Execution, 0, len(pos))
	for _, po := range pos {
		e, err := executionToEntity(po)
		if err != nil {
			return nil, err
		}
		list = append(list, e)
	}
	return list, nil
}

func getExecutionDB(ctx context.Context, db *sqlx.DB, id int64) (*execution.Execution, error) {
	var po model.Execution
	err := db.GetContext(ctx, &po, `SELECT * FROM executions WHERE id = ?`, id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("%w: id=%d", execution.ErrNotFound, id)
	case err != nil:
		return nil, fmt.Errorf("查询执行记录失败: %w", err)
	}
	e, err := executionToEntity(&po)
	if err != nil {
		return nil, fmt.Errorf("重建执行实体失败: %w", err)
	}
	return e, nil
}

// ==================== Registry：发起方视角 ====================

// Create 登记一次执行。两列必须显式写 NULL：
// result（JSON 列不接受空串）、next_retry_at（列默认值是 CURRENT_TIMESTAMP(3)，
// 留默认会让首次重试跳过退避直接可被 planner 捞走）
func (r *ExecutionRegistryRepo) Create(ctx context.Context, e *execution.Execution) error {
	// executions.id 无自增，ID 由上层（雪花算法）分配：缺失时插 id=0 只会撞主键，
	// 这里直接拦下，错误更直白
	if e.ID() <= 0 {
		return fmt.Errorf("登记执行失败: 实体缺少 id，需由调用方分配")
	}
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO executions
		 (id, task_id, schedule_id, dag_execution_id, dag_node_id, status,
		  start_time, end_time, duration_ms, result, error_msg,
		  retry_count, max_retry, worker_id, trace_id, version, next_retry_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.ID(), e.TaskID(), e.ScheduleID(), e.DagExecID(), e.DagNodeID(),
		int32(e.Status()),
		timeValue(e.StartTime()), timeValue(e.EndTime()), int32(e.DurationMS()),
		resultValue(e.Result()), e.ErrorMsg(),
		int32(e.RetryCount()), int32(e.MaxRetry()), e.WorkerID(), e.TraceID(),
		int32(e.Version()), nextRetryValue(e.NextRetryAt()))
	if err != nil {
		return fmt.Errorf("登记执行失败: %w", err)
	}
	return nil
}

func (r *ExecutionRegistryRepo) Get(ctx context.Context, id int64) (*execution.Execution, error) {
	return getExecutionDB(ctx, r.db, id)
}

// ListByIDs 批量取执行。IN 占位符手工展开（database/sql 不支持切片展开），
// 入参去重防占位符与参数错位；空入参直接返回空结果
func (r *ExecutionRegistryRepo) ListByIDs(ctx context.Context, ids []int64) ([]*execution.Execution, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	uniq := make([]int64, 0, len(ids))
	seen := make(map[int64]bool, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			uniq = append(uniq, id)
		}
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(uniq)), ",")
	args := make([]interface{}, 0, len(uniq))
	for _, id := range uniq {
		args = append(args, id)
	}
	var pos []*model.Execution
	err := r.db.SelectContext(ctx, &pos,
		`SELECT * FROM executions WHERE id IN (`+ph+`)`, args...)
	if err != nil {
		return nil, fmt.Errorf("批量查询执行记录失败: %w", err)
	}
	return executionsToEntities(pos)
}

// ==================== WorkerRepo：worker 视角 ====================

// ClaimPending 原子抢票：单条 UPDATE，靠 status 守卫保证只有一个 worker 能改到行，
// RowsAffected=0 即已被他人认领（重复投递幂等）。
// ★ 不在这里推进 version：version 由实体的流转方法在内存中 +1、随 UpdateTerminal 落库，
// 此处若也改 version，worker 随后的 CAS 就会对不上自己的版本号
func (e *ExecutionRepo) ClaimPending(ctx context.Context, id int64, workerID string, now time.Time) (bool, error) {
	info, err := e.db.ExecContext(ctx,
		`UPDATE executions SET status = ?, worker_id = ?, start_time = ?
		 WHERE id = ? AND status IN (?, ?, ?)`,
		int32(execution.StatusRunning), workerID, now,
		id,
		int32(execution.StatusPending), int32(execution.StatusScheduled), int32(execution.StatusRetrying))
	if err != nil {
		return false, fmt.Errorf("认领执行失败: %w", err)
	}
	n, err := info.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("认领执行失败: %w", err)
	}
	return n == 1, nil
}

// UpdateTerminal 写终态：SQL 中不出现 next_retry_at 列
func (e *ExecutionRepo) UpdateTerminal(ctx context.Context, exec *execution.Execution) error {
	return updateTerminal(ctx, e.db, exec)
}

// ==================== RetryPlannerRepo：重试规划器视角 ====================

// ScanRetryable 扫描可重试执行：
//   - Failed 未超上限且退避到期 → 常规重试规划（CAS → Retrying → 重投）
//   - Retrying 且 next_retry_at 到期 → 僵尸重推（上次已规划但投递失败/进程崩溃，
//     没有任何投递者再来碰它——没有这条分支它们会永久卡死）
func (r *RetryPlannerRepo) ScanRetryable(
	ctx context.Context, now time.Time, limit int,
) ([]*execution.Execution, error) {
	var pos []*model.Execution
	err := r.db.SelectContext(ctx, &pos,
		`SELECT * FROM executions
		 WHERE status IN (?, ?) AND retry_count < max_retry
		   AND (next_retry_at IS NULL OR next_retry_at <= ?)
		 ORDER BY next_retry_at ASC
		 LIMIT ?`,
		int32(execution.StatusFailed), int32(execution.StatusRetrying), now, limit)
	if err != nil {
		return nil, fmt.Errorf("扫描可重试执行失败: %w", err)
	}
	return executionsToEntities(pos)
}

// PlanRetry 持久化重试：CAS 推进，仅仍处于 Failed 且版本未被他人改动的行能进入 Retrying，
// 返回 false 表示被其他 planner 实例抢先。
// ★ 调用前 e 必须已走 e.PlanRetry()：CAS 基准取 e.Version()-1（内存已 +1，库中仍是旧值）
func (r *RetryPlannerRepo) PlanRetry(ctx context.Context, e *execution.Execution) (bool, error) {
	info, err := r.db.ExecContext(ctx,
		`UPDATE executions
		 SET status = ?, retry_count = ?, next_retry_at = ?, version = ?
		 WHERE id = ? AND status = ? AND version = ?`,
		int32(execution.StatusRetrying), int32(e.RetryCount()),
		nextRetryValue(e.NextRetryAt()), int32(e.Version()),
		e.ID(), int32(execution.StatusFailed), int32(e.Version()-1))
	if err != nil {
		return false, fmt.Errorf("持久化重试规划失败: %w", err)
	}
	n, err := info.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("持久化重试规划失败: %w", err)
	}
	return n == 1, nil
}

// MarkFailedFinal 重试超限终局：清空 next_retry_at，不再进入重试扫描
func (r *RetryPlannerRepo) MarkFailedFinal(ctx context.Context, e *execution.Execution) error {
	info, err := r.db.ExecContext(ctx,
		`UPDATE executions
		 SET status = ?, error_msg = ?, next_retry_at = NULL, version = ?
		 WHERE id = ? AND status IN (?, ?) AND version = ?`,
		int32(execution.StatusFailedFinal), e.ErrorMsg(), int32(e.Version()),
		e.ID(), int32(execution.StatusFailed), int32(execution.StatusRetrying), int32(e.Version()-1))
	if err != nil {
		return fmt.Errorf("标记重试终局失败: %w", err)
	}
	n, err := info.RowsAffected()
	if err != nil {
		return fmt.Errorf("标记重试终局失败: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("%w: id=%d", execution.ErrWrongStatus, e.ID())
	}
	return nil
}

// ==================== ReaperRepo：超时收割器视角 ====================

// ScanTimeout 扫描 Running 且 start_time 早于阈值的执行
func (rp *ReaperRepo) ScanTimeout(ctx context.Context, olderThan time.Time, limit int) ([]*execution.Execution, error) {
	var pos []*model.Execution
	err := rp.db.SelectContext(ctx, &pos,
		`SELECT * FROM executions WHERE status = ? AND start_time < ? LIMIT ?`,
		int32(execution.StatusRunning), olderThan, limit)
	if err != nil {
		return nil, fmt.Errorf("扫描超时执行失败: %w", err)
	}
	return executionsToEntities(pos)
}

func (rp *ReaperRepo) UpdateTerminal(ctx context.Context, e *execution.Execution) error {
	return updateTerminal(ctx, rp.db, e)
}

// ==================== PendingRepushRepo：投递补偿器视角 ====================

// ScanStalePending 扫描仍处 Pending 且 created_at 早于阈值的执行。
// Pending 行在投递成功前不会有任何更新，created_at 即"登记时刻"；
// 消费端 ClaimPending 抢票保证重投幂等
func (r *ExecutionRegistryRepo) ScanStalePending(ctx context.Context, olderThan time.Time,
	limit int) ([]*execution.Execution, error) {
	var pos []*model.Execution
	err := r.db.SelectContext(ctx, &pos,
		`SELECT * FROM executions WHERE status = ? AND created_at < ? ORDER BY created_at LIMIT ?`,
		int32(execution.StatusPending), olderThan, limit)
	if err != nil {
		return nil, fmt.Errorf("扫描未投递执行失败: %w", err)
	}
	return executionsToEntities(pos)
}

// updateTerminal worker 与 reaper 共用：status=Running 守卫保证同一行只有一个
// 终态写入者胜出（例如 reaper 已收割、worker 的迟到结果被拒），version 交给数据库自增。
// ★ 不按 e.Version() 做 CAS：WorkerRepo 接口没有加载实体的方法，worker 侧的实体
// 可能来自消息快照而非库中行，拿着它比对版本号会误伤正常写入
func updateTerminal(ctx context.Context, db *sqlx.DB, e *execution.Execution) error {
	info, err := db.ExecContext(ctx,
		`UPDATE executions
		 SET status = ?, start_time = ?, end_time = ?, duration_ms = ?,
		     result = ?, error_msg = ?, worker_id = ?, version = version + 1
		 WHERE id = ? AND status = ?`,
		int32(e.Status()),
		timeValue(e.StartTime()), timeValue(e.EndTime()), int32(e.DurationMS()),
		resultValue(e.Result()), e.ErrorMsg(), e.WorkerID(),
		e.ID(), int32(execution.StatusRunning))
	if err != nil {
		return fmt.Errorf("写执行终态失败: %w", err)
	}
	n, err := info.RowsAffected()
	if err != nil {
		return fmt.Errorf("写执行终态失败: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("%w: id=%d", execution.ErrWrongStatus, e.ID())
	}
	return nil
}

// ==================== Query：只读列表查询 ====================

func (q *ExecutionQueryRepo) ListByTask(ctx context.Context, taskID int64, page int) ([]*execution.Execution, int, error) {
	if page < 1 {
		page = 1
	}
	var count int
	if err := q.db.GetContext(ctx, &count,
		`SELECT COUNT(*) FROM executions WHERE task_id = ?`, taskID); err != nil {
		return nil, 0, fmt.Errorf("分页查询执行记录失败: %w", err)
	}

	var pos []*model.Execution
	err := q.db.SelectContext(ctx, &pos,
		`SELECT * FROM executions WHERE task_id = ? LIMIT ? OFFSET ?`,
		taskID, executionPageSize, (page-1)*executionPageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("分页查询执行记录失败: %w", err)
	}
	entities, err := executionsToEntities(pos)
	if err != nil {
		return nil, 0, fmt.Errorf("重建执行实体失败: %w", err)
	}
	return entities, count, nil
}

func (q *ExecutionQueryRepo) Get(ctx context.Context, id int64) (*execution.Execution, error) {
	return getExecutionDB(ctx, q.db, id)
}
