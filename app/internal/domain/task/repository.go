package task

import (
	"context"
	"encoding/json"
)

type Conds struct {
	CreateBy int64
	Status   *int
	Keyword  string
	Type     *int
}

type Assigns struct {
	Name        string
	Config      json.RawMessage
	Priority    int
	Timeout     int
	RetryPolicy *RetryPolicy
	Description string
	Status      *int
}

type Repository interface {
	Create(ctx context.Context, t *Task) error
	ExistByCreatedByWithTaskName(ctx context.Context, id int64, name string) (bool, error)
	QueryByID(ctx context.Context, id int64) (*Task, error)
	Get(ctx context.Context, id int64) (*Task, error)
	Update(ctx context.Context, id int64, assign Assigns) error
	List(ctx context.Context, offset int, pagesize int, conds Conds) ([]*Task, int64, error)
	// ListPage 分页查询但不做 COUNT：多取一条推断 hasMore。
	// COUNT 成本随创建者名下行数线性增长，只有首页需要精确 total，后续页用本方法
	ListPage(ctx context.Context, offset int, pagesize int, conds Conds) ([]*Task, bool, error)
}
