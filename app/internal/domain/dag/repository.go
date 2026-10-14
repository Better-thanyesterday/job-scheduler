package dag

import (
	"context"
	"time"
)

// DagRepo 定义态仓储(api 创建/编辑 DAG)
type DagRepo interface {
	Create(ctx context.Context, d *Dag) error
	Update(ctx context.Context, d *Dag) error
	Get(ctx context.Context, id int64) (*Dag, error)
}

// BrokenRun 无法按当前定义装配的运行：定义在运行期间被结构性修改
// (节点/边增删)，节点记录与新定义对不上，实体无法重建。
// 这类运行永远推进不了，引擎必须把它隔离为 Failed 终态
// (见 DagExecutionRepo.MarkRunDead)，绝不能让它拖垮整批扫描
type BrokenRun struct {
	RunID int64
	DagID int64
	Err   error
}

// DagExecutionRepo 运行态仓储。引擎每轮推进都要落库:
// 节点状态、整体状态、登记的 execution_id 都在节点记录上
type DagExecutionRepo interface {
	Create(ctx context.Context, de *DagExecution) error
	Update(ctx context.Context, de *DagExecution) error
	Get(ctx context.Context, id int64) (*DagExecution, error)
	// ListRunning 扫描运行中的 DAG 运行(引擎轮询推进)。
	// healthy 可正常推进；broken 是定义被运行期间修改、实体无法重建的运行——
	// DB 层瞬时错误走 err(整批下轮重试)，broken 是确定性的数据形态问题，
	// 引擎必须逐个隔离(MarkRunDead)，绝不能因它们放弃整批
	ListRunning(ctx context.Context, limit int) (healthy []*DagExecution, broken []BrokenRun, err error)
	// ListStaleRunning 扫描疑似卡死的运行(引擎的 reaper 用):
	// 仍处 Running 且 updated_at 早于 olderThan,且已无 Running 节点
	// ——没有在飞的执行、又长时间无任何推进,等待中的节点永远等不到派发。
	// broken 语义同 ListRunning
	ListStaleRunning(ctx context.Context, olderThan time.Time, limit int) (healthy []*DagExecution, broken []BrokenRun, err error)
	// MarkRunDead 把无法装配的运行强制收场为 Failed：绕过装配按主键直写，
	// running 态守卫保证只收运行中的行，重复调用幂等。
	// 仅用于"结构性无法装配"的运行——可装配的运行必须走实体流转 + Update
	MarkRunDead(ctx context.Context, runID int64) error
}
