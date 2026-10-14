package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"job-scheduler/app/internal/domain/schedule"
	"job-scheduler/app/internal/domain/task"
	"job-scheduler/app/internal/infra/mysql"
	"job-scheduler/app/internal/infra/mysql/sqlx/model"
)

var _ schedule.TaskSource = (*TaskSourceRepo)(nil)

type TaskSourceRepo struct {
	db *sqlx.DB
}

func NewTaskSourceRepo(taskDB mysql.TaskDB) *TaskSourceRepo {
	return &TaskSourceRepo{db: taskDB.DB}
}

func (ts *TaskSourceRepo) Get(ctx context.Context, id int64) (*task.Task, error) {
	var po model.Task
	err := ts.db.GetContext(ctx, &po,
		`SELECT * FROM tasks WHERE id = ? AND deleted_at IS NULL`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: id=%d", task.ErrNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("query task %d: %w", id, err)
	}
	entity, err := taskToEntity(&po)
	if err != nil {
		return nil, fmt.Errorf("decode task %d: %w", po.ID, err)
	}
	return entity, nil
}

func (ts *TaskSourceRepo) ListByIDs(ctx context.Context, ids []int64) ([]*task.Task, error) {
	if len(ids) == 0 {
		return []*task.Task{}, nil
	}
	// sqlx.In 把 IN (?) 展开成 IN (?,?,...)；MySQL 占位符是 ?，Rebind 原样通过
	query, args, err := sqlx.In(
		`SELECT * FROM tasks WHERE id IN (?) AND deleted_at IS NULL`, ids)
	if err != nil {
		return nil, fmt.Errorf("build in query: %w", err)
	}
	query = sqlx.Rebind(sqlx.QUESTION, query)

	var pos []*model.Task
	if err := ts.db.SelectContext(ctx, &pos, query, args...); err != nil {
		return nil, fmt.Errorf("list tasks by ids %v: %w", ids, err)
	}
	entities, err := tasksToEntities(pos)
	if err != nil {
		return nil, fmt.Errorf("decode tasks: %w", err)
	}
	return entities, nil
}
