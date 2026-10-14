package logic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"job-scheduler/app/internal/api/types"
	"job-scheduler/app/internal/domain/task"
	repo "job-scheduler/app/internal/infra/mysql/sqlx/repo"
	"job-scheduler/app/pkg/snowflake"
	"time"
)

type TaskLogic struct {
	taskrepo *repo.TaskRepo
}

// ErrInvalidTaskInput 标记"用户输入没过领域校验"（config 非法、重试策略非法等），
// controller 据此把校验详情透传给用户，而不是笼统报"服务繁忙"
var ErrInvalidTaskInput = errors.New("task: invalid input")

func NewTaskLogic(taskrepo *repo.TaskRepo) *TaskLogic {
	return &TaskLogic{
		taskrepo: taskrepo,
	}
}

func (l *TaskLogic) Create(ctx context.Context, in *types.CreateTaskReq, userId int64) (*types.TaskResp, error) {
	// 1. 同创建者下任务名唯一：命中即重名；未命中（repo 包装的 task.ErrNotFound）才继续
	ok, err := l.taskrepo.ExistByCreatedByWithTaskName(ctx, userId, in.Name)
	if ok && err == nil {
		return nil, task.ErrNameDuplicated
	} else if !ok && err != nil {
		return nil, err
	}
	// 2. 构造实体：不传策略时用默认（不重试）
	var policy *task.RetryPolicy
	if in.RetryPolicy != nil {
		p, err := task.NewRetryPolicy(in.RetryPolicy.MaxRetry,
			task.Strategy(in.RetryPolicy.Strategy), in.RetryPolicy.Interval)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidTaskInput, err)
		}
		policy = p
	}
	t, err := task.New(snowflake.GenID(), in.Name, in.Timeout, in.Description,
		in.TaskType, in.Priority, in.Config, policy, userId)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidTaskInput, err)
	}
	// 3. 落库
	if err = l.taskrepo.Create(ctx, t); err != nil {
		return nil, err
	}
	return toTaskResponse(t), nil
}

func (l *TaskLogic) Get(ctx context.Context, id int64) (*types.TaskResp, error) {
	// Unscoped 不加，软删除的记录查不到；未命中时 repo 已包装 task.ErrNotFound
	t, err := l.taskrepo.QueryByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return toTaskResponse(t), nil
}

func (l *TaskLogic) List(ctx context.Context, in *types.ListTasksReq, userID int64) (*types.ListTasksResp, error) {
	conds := task.Conds{
		CreateBy: userID,
		Type:     in.TaskType,
		Status:   in.Status,
		Keyword:  in.Keyword,
	}
	offset := (in.Page - 1) * in.PageSize

	// 第一页才付出 COUNT 成本拿精确 total；后续页多取一条推断 has_more（省掉随数据量线性变贵的 COUNT）
	if in.Page == 1 {
		res, count, err := l.taskrepo.List(ctx, offset, in.PageSize, conds)
		if err != nil {
			return nil, fmt.Errorf("list tasks: %w", err)
		}
		return &types.ListTasksResp{
			Total:   int64(count),
			HasMore: int64(offset+len(res)) < count,
			Items:   toTaskResponses(res),
		}, nil
	}

	res, hasMore, err := l.taskrepo.ListPage(ctx, offset, in.PageSize, conds)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	return &types.ListTasksResp{
		HasMore: hasMore,
		Items:   toTaskResponses(res),
	}, nil
}

func toTaskResponses(res []*task.Task) []*types.TaskResp {
	tasks := make([]*types.TaskResp, 0, len(res))
	for _, r := range res {
		tasks = append(tasks, toTaskResponse(r))
	}
	return tasks
}

// 按传入字段更新（只更新请求里给了值的字段）
func (l *TaskLogic) Update(ctx context.Context, id int64, in *types.UpdateTaskReq) (*types.TaskResp, error) {
	if _, err := l.taskrepo.QueryByID(ctx, id); err != nil {
		return nil, err // 未命中时 repo 已包装 task.ErrNotFound
	}
	// RetryPolicy 是可选字段：给了才校验并覆盖，没给就不动库里的策略
	var retryPolicy *task.RetryPolicy
	if in.RetryPolicy != nil {
		p, err := task.NewRetryPolicy(in.RetryPolicy.MaxRetry,
			task.Strategy(in.RetryPolicy.Strategy), in.RetryPolicy.Interval)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidTaskInput, err)
		}
		retryPolicy = p
	}
	assign := task.Assigns{
		Name:        in.Name,
		Description: in.Description,
		Priority:    in.Priority,
		RetryPolicy: retryPolicy,
		Status:      in.Status,
		Timeout:     in.Timeout,
		Config:      in.Config,
	}
	if err := l.taskrepo.Update(ctx, id, assign); err != nil {
		return nil, err
	}
	return l.Get(ctx, id) // 回查返回最新数据
}

// Delete 取消任务：置为已取消，调度侧按「任务取消 → 对应调度删除」处理。
// 仓储层没有物理/软删除接口，也不做跨库事务；调度失效依赖后续的通知补偿
func (l *TaskLogic) Delete(ctx context.Context, id int64) error {
	t, err := l.taskrepo.Get(ctx, id)
	if err != nil {
		return err
	}
	// 领域把关：已成功的任务不允许取消
	if err := t.Cancel(); err != nil {
		return err
	}
	status := int(task.TaskStatusCanceled)
	return l.taskrepo.Update(ctx, id, task.Assigns{Status: &status})
}

// 备注（原实现留档）：早先的 Delete 打算在 job_task 本地事务里"置删除状态 + 软删除 +
// 写本地消息表"，再由定时任务通知 Scheduler 把调度从堆里摘掉。当前仓储层不具备这些接口，
// 跨库也无法用本地事务兜住，故先按领域语义退化为「取消」；等调度失效的通知通道接好再补。

func toTaskResponse(t *task.Task) *types.TaskResp {
	cfg, _ := task.MarshalConfig(t.Config())
	resp := &types.TaskResp{
		ID:          t.ID(),
		Name:        t.Name(),
		TaskType:    int(t.Type()),
		Config:      json.RawMessage(cfg),
		Priority:    int(t.Priority()),
		Timeout:     int(t.Timeout().Seconds()),
		Status:      int(t.Status()),
		Description: t.Description(),
		CreatedAt:   t.CreatedAt().Format(time.RFC3339),
		UpdatedAt:   t.UpdatedAt().Format(time.RFC3339),
	}
	// 重试策略直接从领域 getter 取值：domain 的 MarshalJSON 是给人看的字符串格式
	// （"fixed"/"1m0s"），与 api 契约的数值格式（1/纳秒）不同，走 JSON 往返会静默丢失
	if p := t.RetryPolicy(); p != (task.RetryPolicy{}) {
		resp.RetryPolicy = &types.RetryPolicy{
			MaxRetry: p.MaxRetry(),
			Strategy: int(p.Strategy()),
			Interval: p.Interval(),
		}
	}
	return resp
}
