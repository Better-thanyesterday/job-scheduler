package sqlx

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"job-scheduler/app/internal/domain/task"

	"job-scheduler/app/internal/infra/mysql"
	"job-scheduler/app/internal/infra/mysql/sqlx/model"

	"github.com/jmoiron/sqlx"
)

var _ task.Repository = (*TaskRepo)(nil)

type TaskRepo struct {
	db *sqlx.DB
}

func NewTaskRepo(taskDB mysql.TaskDB) *TaskRepo {
	return &TaskRepo{
		db: taskDB.DB,
	}
}

func (t *TaskRepo) Create(ctx context.Context, tsk *task.Task) error {
	sqlstr := `INSERT INTO tasks (id,name,task_type,config, priority,timeout,
    retry_policy,status,description,created_by)
			VALUES (:id,:name,:task_type,:config,:priority,:timeout,:retry_policy,
			:status,:description,:created_by)`
	po, err := taskToPO(tsk)
	if err != nil {
		return fmt.Errorf("entity to po failed: %w", err)
	}
	if _, err := t.db.NamedExecContext(ctx, sqlstr, po); err != nil {
		return fmt.Errorf("insert task %d: %w", tsk.ID(), err)
	}
	return nil
}

// ExistByCreatedByWithTaskName 语义：存在同名任务 → (true, nil)；
// 不存在 → (false, nil)；数据库故障 → (false, err)
func (t *TaskRepo) ExistByCreatedByWithTaskName(ctx context.Context, createdby int64, name string) (bool, error) {
	var one int
	err := t.db.GetContext(ctx, &one,
		`SELECT 1 FROM tasks WHERE created_by = ? AND name = ? AND deleted_at IS NULL LIMIT 1`,
		createdby, name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("query task by creator and name: %w", err)
	}
	return true, nil
}

func (t *TaskRepo) QueryByID(ctx context.Context, id int64) (*task.Task, error) {
	po, err := t.getPO(ctx, id)
	if err != nil {
		return nil, err
	}
	entity, err := taskToEntity(po)
	if err != nil {
		return nil, fmt.Errorf("decode task %d: %w", po.ID, err)
	}
	return entity, nil
}

func (t *TaskRepo) Get(ctx context.Context, id int64) (*task.Task, error) {
	return t.QueryByID(ctx, id)
}

// getPO 单行装载，QueryByID/Get 共用。gorm 软删除等价条件：deleted_at IS NULL
func (t *TaskRepo) getPO(ctx context.Context, id int64) (*model.Task, error) {
	var po model.Task
	err := t.db.GetContext(ctx, &po,
		`SELECT * FROM tasks WHERE id = ? AND deleted_at IS NULL`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: id=%d", task.ErrNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("query task %d: %w", id, err)
	}
	return &po, nil
}

func (t *TaskRepo) Update(ctx context.Context, id int64, assign task.Assigns) error {
	var sets []string
	var args []interface{}
	if assign.Name != "" {
		sets = append(sets, "name = ?")
		args = append(args, assign.Name)
	}
	if assign.Config != nil {
		sets = append(sets, "config = ?")
		args = append(args, string(assign.Config))
	}
	if assign.Priority != 0 {
		sets = append(sets, "priority = ?")
		args = append(args, int32(assign.Priority))
	}
	if assign.Timeout != 0 {
		sets = append(sets, "timeout = ?")
		args = append(args, int32(assign.Timeout))
	}
	if assign.Description != "" {
		sets = append(sets, "description = ?")
		args = append(args, assign.Description)
	}
	if assign.Status != nil {
		sets = append(sets, "status = ?")
		args = append(args, int32(*assign.Status))
	}
	if assign.RetryPolicy != nil {
		b, _ := json.Marshal(*assign.RetryPolicy)
		sets = append(sets, "retry_policy = ?")
		args = append(args, string(b))
	}
	if len(sets) == 0 {
		return nil
	}
	// gorm 软删除对 UPDATE 同样生效：WHERE 里补 deleted_at IS NULL
	args = append(args, id)
	_, err := t.db.ExecContext(ctx,
		`UPDATE tasks SET `+strings.Join(sets, ", ")+` WHERE id = ? AND deleted_at IS NULL`,
		args...)
	if err != nil {
		return fmt.Errorf("update task %d: %w", id, err)
	}
	return nil
}

// listWhere 组装 List/ListPage 共用的过滤条件
func (t *TaskRepo) listWhere(conds task.Conds) (string, []interface{}) {
	where := "created_by = ?"
	args := []interface{}{conds.CreateBy}
	if conds.Type != nil {
		where += " AND task_type = ?"
		args = append(args, int32(*conds.Type))
	}
	if conds.Status != nil {
		where += " AND status = ?"
		args = append(args, int32(*conds.Status))
	}
	if keyword := strings.TrimSpace(conds.Keyword); keyword != "" {
		// LIKE 通配符转义：用户输入的 %/_ 按字面匹配，不当通配符解释
		keyword = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(keyword)
		where += " AND name LIKE ?"
		args = append(args, "%"+keyword+"%")
	}
	where += " AND deleted_at IS NULL"
	return where, args
}

func (t *TaskRepo) List(ctx context.Context, offset int, pagesize int, conds task.Conds) ([]*task.Task, int64, error) {
	where, args := t.listWhere(conds)

	var count int64
	if err := t.db.GetContext(ctx, &count,
		`SELECT COUNT(*) FROM tasks WHERE `+where, args...); err != nil {
		return nil, 0, fmt.Errorf("count tasks: %w", err)
	}

	query := `SELECT * FROM tasks WHERE ` + where + ` LIMIT ? OFFSET ?`
	countArgs := append(args, pagesize, offset) //nolint:gocritic
	var pos []*model.Task
	if err := t.db.SelectContext(ctx, &pos, query, countArgs...); err != nil {
		return nil, 0, err // 业务前缀由 logic 层统一添加，repo 只透传（含 context.Canceled 等原始错误）
	}
	entities, err := tasksToEntities(pos)
	if err != nil {
		return nil, 0, fmt.Errorf("decode tasks: %w", err)
	}
	return entities, count, nil
}

// ListPage 不做 COUNT：多取一条推断是否有下一页，省掉随数据量线性变贵的 COUNT
func (t *TaskRepo) ListPage(ctx context.Context, offset int, pagesize int, conds task.Conds) ([]*task.Task, bool, error) {
	where, args := t.listWhere(conds)
	query := `SELECT * FROM tasks WHERE ` + where + ` LIMIT ? OFFSET ?`
	queryArgs := append(args, pagesize+1, offset) //nolint:gocritic
	var pos []*model.Task
	if err := t.db.SelectContext(ctx, &pos, query, queryArgs...); err != nil {
		return nil, false, err // 业务前缀由 logic 层统一添加，repo 只透传（含 context.Canceled 等原始错误）
	}
	hasMore := len(pos) > pagesize
	if hasMore {
		pos = pos[:pagesize]
	}
	entities, err := tasksToEntities(pos)
	if err != nil {
		return nil, false, fmt.Errorf("decode tasks: %w", err)
	}
	return entities, hasMore, nil
}

// ==================== 实体 → PO（写库方向）====================

func taskToPO(t *task.Task) (*model.Task, error) {
	// 值对象 → JSON 字符串：只有这里知道"数据库里存的是 JSON"
	cfgJSON, err := json.Marshal(t.Config())
	if err != nil {
		return nil, fmt.Errorf("marshal task %d config: %w", t.ID(), err)
	}
	policyJSON, err := t.RetryPolicy().MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("marshal task %d retry policy: %w", t.ID(), err)
	}

	return &model.Task{
		ID:          t.ID(),
		Name:        t.Name(),
		TaskType:    int32(t.Type()), // 领域枚举 → int32
		Config:      string(cfgJSON), // TaskConfig 值对象 → JSON string
		Priority:    int32(t.Priority()),
		Timeout:     int32(t.Timeout().Seconds()), // Duration → 秒（和表字段单位对齐）
		RetryPolicy: string(policyJSON),           // RetryPolicy 值对象 → JSON string
		Status:      int32(t.Status()),            // Status 枚举 → int32
		Description: t.Description(),
		CreatedBy:   t.CreatedBy(),
		CreatedAt:   t.CreatedAt(),
		UpdatedAt:   t.UpdatedAt(),
		// DeletedAt 不在这设置——删除走 Update 状态字段
	}, nil
}

// ==================== PO → 实体（读库方向）====================

func taskToEntity(po *model.Task) (*task.Task, error) {
	// JSON 字符串 → 值对象：走领域构造入口，脏数据在这里就报错
	cfg, err := task.NewTaskConfig(task.TaskType(po.TaskType), json.RawMessage(po.Config))
	if err != nil {
		return nil, fmt.Errorf("rebuild task %d config: %w", po.ID, err)
	}
	var policy task.RetryPolicy
	if err := policy.UnmarshalJSON([]byte(po.RetryPolicy)); err != nil {
		return nil, fmt.Errorf("rebuild task %d retry policy: %w", po.ID, err)
	}

	// Rebuild：信任已持久化数据，不做业务校验
	return task.Rebuild(
		po.ID,
		po.Name,
		cfg,
		policy,
		int(po.Priority),
		po.Timeout,
		task.TaskStatus(po.Status),
		po.Description,
		po.CreatedBy,
		po.CreatedAt,
		po.UpdatedAt,
		task.TaskType(po.TaskType),
	), nil
}

// 批量版本：List 方法用
func tasksToEntities(pos []*model.Task) ([]*task.Task, error) {
	tasks := make([]*task.Task, 0, len(pos))
	for _, po := range pos {
		t, err := taskToEntity(po)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}
