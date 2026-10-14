// Package dag DAG 编排引擎：驱动一次运行内部的节点调度。
//
// 与 scheduler.Engine（时间驱动，cron 到点触发）不同，这里是状态驱动：
// 节点到达终态 → 下游依赖判定 → 派发就绪节点 → 尝试收尾。
// 节点执行完全复用现有链路：execution 登记 → MQ 投递 → worker 认领，
// 重试由 execution 的 planner 托管、超时由 reaper 托管，本引擎只看终态。
//
// 文件划分：engine.go 生命周期主循环；advance.go 推进状态机；
// dispatch.go 派发与数据流装配；sync.go 节点终态同步；reaper.go 死运行收割。
package dag

import (
	"context"
	"time"

	"job-scheduler/app/internal/domain/coordination"
	dagdomain "job-scheduler/app/internal/domain/dag"
	"job-scheduler/app/internal/domain/execution"
	"job-scheduler/app/internal/domain/schedule"

	"go.uber.org/zap"
)

const (
	// defaultInterval 推进扫描周期：节点粒度的调度延迟上界。
	// MQ 唤醒消费把它压到毫秒级，本周期退化为兜底轮询
	defaultInterval = 5 * time.Second
	// defaultBatch 每轮最多推进多少次运行（防止单轮拖太久）
	defaultBatch = 100
	// lockTTL 单次推进动作的锁持有上限（与 scheduler engine 的 trigger 锁一致）
	lockTTL = 30 * time.Second
	// staleAfter 判定运行"卡死"的静默阈值：无在飞执行、又这么久没有任何
	// 状态推进（updated_at 不再变化），基本只有任务快照永久取不到一种成因。
	// 要远大于正常节点的执行时长上限（那些有 execution 的 planner/reaper 兜底）
	staleAfter = time.Hour
	// wakeQueueLen 唤醒队列容量。满了就丢——唤醒只是加速信号，
	// 丢了由下一轮 tick 兜底（DB 终态才是事实源）
	wakeQueueLen = 256
)

type Engine struct {
	execRepo  dagdomain.DagExecutionRepo // ListRunning / Update / Get
	execReg   execution.Registry         // 节点 execution 的登记与终态查询
	tasks     schedule.TaskSource        // 按 task_id 取任务快照（构建消息）
	pusher    coordination.TaskPusher
	locks     coordination.LockManager
	interval  time.Duration
	batchSize int
	wakeCh    chan int64 // 结果消息带来的"待推进运行"唤醒信号
}

func NewEngine(execRepo dagdomain.DagExecutionRepo, execReg execution.Registry,
	tasks schedule.TaskSource, pusher coordination.TaskPusher,
	locks coordination.LockManager, interval time.Duration) *Engine {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Engine{
		execRepo:  execRepo,
		execReg:   execReg,
		tasks:     tasks,
		pusher:    pusher,
		locks:     locks,
		interval:  interval,
		batchSize: defaultBatch,
		wakeCh:    make(chan int64, wakeQueueLen),
	}
}

// Wake 结果消费方（cmd 层的 MQ consumer）收到 dag.* 结果消息后调用：
// 标记该运行待推进。语义是"加速信号"而非"状态驱动"——消息可能重复、
// 乱序、丢失，推进时终态判定仍以 DB 为准。
// 非阻塞：队列满即丢弃
func (e *Engine) Wake(dagExecID int64) {
	select {
	case e.wakeCh <- dagExecID:
	default:
		zap.L().Debug("唤醒队列已满，丢弃唤醒信号（tick 兜底）",
			zap.Int64("dag_execution_id", dagExecID))
	}
}

// Run 周期推进所有运行中的 DAG，直到 ctx 取消。
// 与 planner/reaper 同款循环；接线方在 cmd 里 go engine.Run(ctx) 即可。
// tick 与 MQ 唤醒共用同一推进路径，互不抢锁（per-run 锁串行化）
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	zap.L().Info("dag engine started", zap.Duration("interval", e.interval))
	for {
		select {
		case <-ctx.Done():
			zap.L().Info("dag engine stopped")
			return
		case <-ticker.C:
			e.advanceOnce(ctx)
		case id := <-e.wakeCh:
			if err := e.advanceByID(ctx, id); err != nil {
				zap.L().Error("唤醒推进 DAG 运行失败",
					zap.Int64("dag_execution_id", id), zap.Error(err))
			}
		}
	}
}
