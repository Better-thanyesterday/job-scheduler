package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"job-scheduler/app/internal/api/types"
	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/executor/resultstore"
	repo "job-scheduler/app/internal/infra/mysql/sqlx/repo"
	"job-scheduler/app/internal/scheduler"
)

// ExecutionLogic 执行域应用服务：执行历史查询 + 手动触发。
// 触发走与 scheduler 相同的投递通道：登记 Pending 执行 → 经 etcd 成员表发现
// executor → gRPC 直推。推送失败不回滚登记，Pending 执行由 Repusher 周期兜底
type ExecutionLogic struct {
	taskRepo  *repo.TaskRepo
	execRepo  *repo.ExecutionRegistryRepo
	queryRepo *repo.ExecutionQueryRepo
	pusher    coordination.TaskPusher
	// results 溢出结果存储（可选）：信封结果生成预签名下载 URL；nil = 未配置
	results *resultstore.Store
}

func NewExecutionLogic(taskRepo *repo.TaskRepo,
	execRepo *repo.ExecutionRegistryRepo,
	queryRepo *repo.ExecutionQueryRepo,
	pusher coordination.TaskPusher,
	results *resultstore.Store,
) *ExecutionLogic {
	return &ExecutionLogic{
		taskRepo:  taskRepo,
		execRepo:  execRepo,
		queryRepo: queryRepo,
		pusher:    pusher,
		results:   results,
	}
}

func (l *ExecutionLogic) ListByTask(ctx context.Context, in *types.ListExecutionsReq) (*types.ListExecutionsResp, error) {
	list, total, err := l.queryRepo.ListByTask(ctx, in.TaskID, in.Page)
	if err != nil {
		return nil, err
	}
	items := make([]*types.ExecutionResp, 0, len(list))
	for _, e := range list {
		items = append(items, toExecutionResponse(e))
	}
	return &types.ListExecutionsResp{
		Total: total,
		Page:  in.Page,
		Items: items,
	}, nil
}

func (l *ExecutionLogic) Get(ctx context.Context, id int64) (*types.ExecutionResp, error) {
	ee, err := l.queryRepo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return toExecutionResponse(ee), nil
}

// Trigger 手动触发一次执行：任务存在即可触发（不限定任务状态），
// 执行的 maxRetry / 超时 / 配置快照全部取自任务当前版本。
// ScheduleID=0 标记手动来源（对应 coordination.ExecutionMessage 的约定）
func (l *ExecutionLogic) Trigger(ctx context.Context, taskID int64, userID int64) (*types.ExecutionResp, error) {
	t, err := l.taskRepo.QueryByID(ctx, taskID)
	if err != nil {
		return nil, err // 未命中时 repo 已包装 task.ErrNotFound
	}
	exec, err := execution.New(taskID, 0, t.RetryPolicy().MaxRetry(),
		fmt.Sprintf("manual-u%d", userID), time.Now())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidTaskInput, err)
	}
	if err := l.execRepo.Create(ctx, exec); err != nil {
		return nil, err
	}
	// 推送失败向上报错，但保留已登记的 Pending 执行：Repusher 会周期兜底重推
	if err := l.pusher.Push(ctx, scheduler.BuildExecutionMessage(exec, t)); err != nil {
		return nil, err
	}
	return toExecutionResponse(exec), nil
}

func toExecutionResponse(e *execution.Execution) *types.ExecutionResp {
	resp := &types.ExecutionResp{
		ID:          e.ID(),
		TaskID:      e.TaskID(),
		ScheduleID:  e.ScheduleID(),
		DagExecID:   e.DagExecID(),
		DagNodeID:   e.DagNodeID(),
		Status:      int(e.Status()),
		RetryCount:  e.RetryCount(),
		MaxRetry:    e.MaxRetry(),
		NextRetryAt: formatTimePtr(e.NextRetryAt()),
		Version:     e.Version(),
		DurationMS:  e.DurationMS(),
		TraceID:     e.TraceID(),
		WorkerID:    e.WorkerID(),
		StartTime:   formatTime(e.StartTime()),
		EndTime:     formatTime(e.EndTime()),
		ErrorMsg:    e.ErrorMsg(),
		CreatedAt:   formatTime(e.CreatedAt()),
	}
	// result 在库里是 JSON 列，空值不上送 JSON null，交给 omitempty
	if result := e.Result(); len(result) > 0 {
		resp.Result = json.RawMessage(result)
	}
	return resp
}

// ExecutionResultDownload 执行结果的下载形态：
// Inline 非空 = 内联原文（直接作为响应体）；Redirect 非空 = 302 目标（预签名 URL）
type ExecutionResultDownload = struct {
	Inline      []byte
	Redirect    string
	ContentType string
}

// DownloadResult 溢出结果可达性：result 列里是溢出信封时，
// 生成 15 分钟预签名下载 URL 交给调用方 302——大载荷直连对象存储不过 api；
// 内联结果原样返回。未配置结果存储且结果是信封时报错（错误信息指明缺配置）
func (l *ExecutionLogic) DownloadResult(ctx context.Context, id int64) (
	*struct {
		Inline      []byte
		Redirect    string
		ContentType string
	}, error,
) {
	e, err := l.queryRepo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	result := e.Result()
	if env, ok := resultstore.ParseEnvelope(result); ok {
		if l.results == nil {
			return nil, fmt.Errorf("结果已溢出到对象存储，但 api 未配置结果存储（executor.minio 段），无法生成下载链接")
		}
		url, err := l.results.PresignGet(ctx, env, 15*time.Minute)
		if err != nil {
			return nil, err
		}
		return &struct {
			Inline      []byte
			Redirect    string
			ContentType string
		}{Redirect: url}, nil
	}
	if len(result) == 0 {
		return &struct {
			Inline      []byte
			Redirect    string
			ContentType string
		}{Inline: []byte("{}"), ContentType: "application/json"}, nil
	}
	return &struct {
		Inline      []byte
		Redirect    string
		ContentType string
	}{Inline: result, ContentType: "application/json"}, nil
}
