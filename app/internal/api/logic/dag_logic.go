package logic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"job-scheduler/app/internal/api/types"
	"job-scheduler/app/internal/domain/dag"
	repo "job-scheduler/app/internal/infra/mysql/sqlx/repo"
)

type DagLogic struct {
	dagrepo  *repo.DagRepo
	execRepo *repo.DagExecutionRepo
}

// ErrInvalidDagInput 标记"用户输入没过领域校验"（YAML 结构、边类型、环等），
// controller 据此把校验详情透传给用户，而不是笼统报"服务繁忙"
var ErrInvalidDagInput = errors.New("dag: invalid input")

func NewDagLogic(dagrepo *repo.DagRepo, execRepo *repo.DagExecutionRepo) *DagLogic {
	return &DagLogic{dagrepo: dagrepo, execRepo: execRepo}
}

// Import 导入 YAML 定义的 DAG：解析 → 领域构造（出生即校验）→ 落库。
// YAML 里的悬空边/自环/依赖环在这里就被拒绝，不存在"先存了再发现是死图"的路径
func (l *DagLogic) Import(ctx context.Context, in *types.DagYAML, userID int64) (*types.DagResp, error) {
	d, err := buildDagFromYAML(in, userID)
	if err != nil {
		return nil, err
	}
	if err := l.dagrepo.Create(ctx, d); err != nil {
		return nil, err
	}
	return toDagResponse(d), nil
}

func (l *DagLogic) Get(ctx context.Context, id int64) (*types.DagResp, error) {
	// 未命中时 repo 已包装 dag.ErrNotFound
	d, err := l.dagrepo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return toDagResponse(d), nil
}

// maxRunCtxBytes 运行上下文体积上限：runCtx 只随运行行存档展示，
// 是不透传给节点任务的死重数据——不封顶就能被当免费 KV 打爆运行行
const maxRunCtxBytes = 64 << 10 // 64KB

func (l *DagLogic) Run(ctx context.Context, id int64, runCtx json.RawMessage) (*types.DagRunResp, error) {
	if len(runCtx) > maxRunCtxBytes {
		return nil, fmt.Errorf("run_ctx %d 字节超上限 %d", len(runCtx), maxRunCtxBytes)
	}
	d, err := l.dagrepo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !d.Active() {
		return nil, fmt.Errorf("%w: dag %d 未启用（status=%d），不接受新运行", dag.ErrWrongStatus, id, d.Status())
	}
	de, err := dag.NewExecution(d, runCtx, dag.TriggerManual, time.Now())
	if err != nil {
		return nil, err
	}
	if err := l.execRepo.Create(ctx, de); err != nil {
		return nil, err
	}
	return &types.DagRunResp{
		ID:     de.ID(),
		DagID:  id,
		Status: int(de.Status()),
	}, nil
}

// Update 整体替换定义内容（节点/边只能整体换：改一条边就可能成环），
// 保留原 id / 创建者 / 生命周期状态。软删后的定义 repo 层拒绝更新
func (l *DagLogic) Update(ctx context.Context, id int64, in *types.DagYAML) (*types.DagResp, error) {
	old, err := l.dagrepo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	fresh, err := buildDagFromYAML(in, old.CreatedBy())
	if err != nil {
		return nil, err
	}
	updated, err := dag.Rebuild(id, fresh.Name(), fresh.Description(), fresh.ParallelLimit(),
		fresh.FailFast(), old.Status(), old.CreatedBy(), old.CreatedAt(), old.UpdatedAt(),
		fresh.Nodes(), fresh.Edges())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDagInput, err)
	}
	if err := l.dagrepo.Update(ctx, updated); err != nil {
		return nil, err
	}
	return toDagResponse(updated), nil
}

// Enable / Disable / Delete 定义生命周期操作：领域方法把关非法流转，
// repo.Update 持久化（status 列守卫软删后的定义）
func (l *DagLogic) Enable(ctx context.Context, id int64) (*types.DagResp, error) {
	return l.changeStatus(ctx, id, (*dag.Dag).Enable)
}

func (l *DagLogic) Disable(ctx context.Context, id int64) (*types.DagResp, error) {
	return l.changeStatus(ctx, id, (*dag.Dag).Disable)
}

func (l *DagLogic) Delete(ctx context.Context, id int64) (*types.DagResp, error) {
	return l.changeStatus(ctx, id, (*dag.Dag).MarkDeleted)
}

func (l *DagLogic) changeStatus(ctx context.Context, id int64,
	apply func(*dag.Dag, time.Time) error) (*types.DagResp, error) {
	d, err := l.dagrepo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := apply(d, time.Now()); err != nil {
		return nil, fmt.Errorf("%w: %w", dag.ErrWrongStatus, err)
	}
	if err := l.dagrepo.Update(ctx, d); err != nil {
		return nil, err
	}
	return toDagResponse(d), nil
}

// buildDagFromYAML YAML 结构 → 领域聚合。所有领域不变量在 dag.New 一并校验
// （nodeID 唯一、边引用完整、无环含自环），格式问题（未知边类型）在这里拦下。
// 与 repo 无关的纯函数，可独立单测
func buildDagFromYAML(in *types.DagYAML, createdBy int64) (*dag.Dag, error) {
	nodes := make([]*dag.DagNode, 0, len(in.Nodes))
	for _, n := range in.Nodes {
		node, err := dag.NewNode(n.ID, n.TaskID, n.Name)
		if err != nil {
			return nil, fmt.Errorf("%w: 节点 %q: %w", ErrInvalidDagInput, n.ID, err)
		}
		nodes = append(nodes, node)
	}
	edges := make([]dag.Edge, 0, len(in.Edges))
	for _, e := range in.Edges {
		et, err := parseEdgeType(e.Type)
		if err != nil {
			return nil, fmt.Errorf("%w: 边 %s→%s: %w", ErrInvalidDagInput, e.From, e.To, err)
		}
		edge, err := dag.NewEdge(e.From, e.To, et)
		if err != nil {
			return nil, fmt.Errorf("%w: 边 %s→%s: %w", ErrInvalidDagInput, e.From, e.To, err)
		}
		edges = append(edges, *edge)
	}
	failFast := true // YAML 未声明时与 dags 表默认值保持一致
	if in.FailFast != nil {
		failFast = *in.FailFast
	}
	d, err := dag.New(in.Name, in.Description, in.ParallelLimit, failFast, createdBy, nodes, edges)
	if err != nil {
		// 依赖环/重复 nodeID/悬空边等语义错误全在这一步浮出
		return nil, fmt.Errorf("%w: %w", ErrInvalidDagInput, err)
	}
	return d, nil
}

// parseEdgeType YAML 的字符串边类型 → 领域枚举。大小写不敏感，缺省 success
func parseEdgeType(s string) (dag.EdgeStatus, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "success":
		return dag.EdgeStatusSuccess, nil
	case "failure", "fail":
		return dag.EdgeStatusFailure, nil
	case "always":
		return dag.EdgeStatusAlways, nil
	default:
		return 0, fmt.Errorf("%w: 未知边类型 %q（可选 success/failure/always）", ErrInvalidDagInput, s)
	}
}

func toDagResponse(d *dag.Dag) *types.DagResp {
	nodes := make([]types.DagNodeResp, 0, len(d.Nodes()))
	for _, n := range d.Nodes() {
		nodes = append(nodes, types.DagNodeResp{NodeID: n.NodeID(), TaskID: n.TaskID(), Name: n.Name()})
	}
	edges := make([]types.DagEdgeResp, 0, len(d.Edges()))
	for _, e := range d.Edges() {
		edges = append(edges, types.DagEdgeResp{
			FromNodeID: e.FromNodeID(), ToNodeID: e.ToNodeID(), EdgeType: int(e.EdgeType()),
		})
	}
	return &types.DagResp{
		ID:            d.ID(),
		Name:          d.Name(),
		Description:   d.Description(),
		ParallelLimit: d.ParallelLimit(),
		FailFast:      d.FailFast(),
		Status:        int(d.Status()),
		CreatedBy:     d.CreatedBy(),
		CreatedAt:     d.CreatedAt().Format(time.RFC3339),
		UpdatedAt:     d.UpdatedAt().Format(time.RFC3339),
		Nodes:         nodes,
		Edges:         edges,
	}
}
