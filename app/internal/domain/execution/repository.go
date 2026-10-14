package execution

import (
	"context"
	"time"
)

// Registry 发起方视角（api 手动触发 / engine Cron 登记）：
// 只能创建，看不到任何状态迁移方法
type Registry interface {
	Create(ctx context.Context, e *Execution) error
	Get(ctx context.Context, id int64) (*Execution, error)
	// ListByIDs 批量取执行（DAG 引擎同步节点终态用：一轮一个 IN 查询，
	// 替代逐节点 Get 的 N+1）。id 重复时结果去重；查不到的 id 不报错、
	// 不出现在结果里（调用方自行判断）
	ListByIDs(ctx context.Context, ids []int64) ([]*Execution, error)
}

// WorkerRepo worker 视角：只能抢票和写终态。
// ★ 方法面上不存在 PlanRetry / MarkFailedFinal / Reap —— 想越权也调不到
type ExecutorRepo interface {
	// ClaimPending 原子抢票：
	// 返回 false = 已被别人认领（重复投递幂等，调用方直接 ACK 丢弃）
	ClaimPending(ctx context.Context, id int64, workerID string, now time.Time) (bool, error)
	// UpdateTerminal 写终态：实体方法只能 Success/Failed；
	// ★ SQL 中不出现 next_retry_at 列（物理上杜绝 worker 碰它）
	UpdateTerminal(ctx context.Context, e *Execution) error
}

// RetryPlannerRepo 重试规划器视角：扫描 + 重试 + 终局，三者打包给 planner
type RetryPlannerRepo interface {
	// ScanRetryable 扫描可重试执行：
	ScanRetryable(ctx context.Context, now time.Time, limit int) ([]*Execution, error)
	// PlanRetry 持久化重试：同事务 retry_count+1 / next_retry_at / version+1，
	// CAS 失败返回 false（被其他 planner 实例抢先）
	PlanRetry(ctx context.Context, e *Execution) (bool, error)
	MarkFailedFinal(ctx context.Context, e *Execution) error
}

// ReaperRepo 超时收割器视角
type ReaperRepo interface {
	ScanTimeout(ctx context.Context, olderThan time.Time, limit int) ([]*Execution, error)
	UpdateTerminal(ctx context.Context, e *Execution) error
}

// PendingRepushRepo 投递补偿器视角：扫描"登记后长时间仍未投递"的执行。
// cron 与 DAG 共用（同一张 executions 表）——推送失败的执行永远停在 Pending，
// 没有这条扫描它们就只有人工重投一条路
type PendingRepushRepo interface {
	// ScanStalePending 扫描仍处 Pending 且 created_at 早于 olderThan 的执行。
	// 阈值由调用方给定，须大于推送在途的最坏时长，避免把刚推出去的执行重投
	ScanStalePending(ctx context.Context, olderThan time.Time, limit int) ([]*Execution, error)
}

// Query 列表查询（api 执行历史页 / 大盘）——只读，任何人可用
type Query interface {
	ListByTask(ctx context.Context, taskID int64, page int) ([]*Execution, int, error)
	Get(ctx context.Context, id int64) (*Execution, error)
}
