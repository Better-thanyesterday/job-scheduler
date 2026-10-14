// Package metrics 全系统埋点注册表：定义指标 + 打点辅助，集中一处、全局可 import。
//
// 职责边界：本包只负责"把系统状态变成数字"，不判断、不告警、不落库——
// 告警规则、持续窗口、抑制、通知分发全部交给 Prometheus + Alertmanager：
//   - Rule.Sustain   → 告警规则的 for:
//   - Rule.Cooldown  → Alertmanager 的 repeat_interval / group_interval
//   - 抑制表         → Alertmanager 的 silence / inhibition
//   - Notifier 三渠道 → Alertmanager 的 receivers
//   - 内置五条规则    → 告警规则文件（数据源契约见各指标的注释）
package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// TaskTriggered 引擎触发一次调度。
	// 掉零 = 调度停摆的信号之一（配合 engine_up / engine_heartbeat 的 P0 规则）
	TaskTriggered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scheduler_task_triggered_total",
	}, []string{"task_id"})

	// TaskExecuted worker 完成一次执行的终态分布。
	// success_rate(5m) 告警数据源：
	//   rate(scheduler_task_executed_total{status="success"}[5m])
	//     / rate(scheduler_task_executed_total[5m])
	TaskExecuted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scheduler_task_executed_total",
	}, []string{"status"}) // success / failed / final_failed / timeout

	// TaskFinalFailed 按任务维度的终局失败计数（取代原 alert_events 事件表方案）。
	// 单任务失败通知数据源：increase(scheduler_task_final_failed_total[10m]) > 0，
	// task_id 标签供 Alertmanager 路由与 {{ $labels.task_id }} 模板。
	// 只在 planner 判终局时递增，序列基数受"仍在失败的任务数"约束，不是全量任务
	TaskFinalFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scheduler_task_final_failed_total",
	}, []string{"task_id"})

	// TaskDuration 执行耗时分布（task_type 维度，避免 task_id 高基数）
	TaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scheduler_task_duration_seconds",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 13), // 0.1s ~ 409.6s
	}, []string{"task_type"})

	// RetryPending 等待重试的执行堆积数（planner 扫描顺手打点）。
	// 堆积 > 1000 持续 5min → P1，对应告警规则 for: 5m
	RetryPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_retry_pending",
	})

	// ScheduleLag planned vs 实际触发的偏差分布（需求 8.5 调度准确性）
	ScheduleLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "scheduler_trigger_lag_seconds",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
	})

	// EngineUp 本实例 engine 循环是否在跑（= leader 职责已启动）。
	// 进程死亡由 Prometheus 的 up == 0 覆盖；非 leader 实例为 0，
	// 心跳规则用它排除备节点心跳冻结的误报
	EngineUp = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_engine_up",
	})

	// EngineHeartbeat engine 校准循环最后一次成功推进的 unix 时间戳。
	// P0「调度停摆」数据源（进程活着但循环卡死/DB 持续失败时心跳冻结）：
	//   time() - scheduler_engine_heartbeat_timestamp_seconds > 30
	//   and on(instance) scheduler_engine_up == 1
	EngineHeartbeat = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_engine_heartbeat_timestamp_seconds",
	})
)

// ==================== 打点辅助：调用点只表达意图 ====================

// IncTaskTriggered 引擎触发一次调度
func IncTaskTriggered(taskID int64) {
	TaskTriggered.WithLabelValues(strconv.FormatInt(taskID, 10)).Inc()
}

// IncTaskExecuted worker 完成一次执行（status: success / failed / final_failed / timeout）
func IncTaskExecuted(status string) {
	TaskExecuted.WithLabelValues(status).Inc()
}

// IncTaskFinalFailed planner 判定一次终局失败（单任务告警的数据源）
func IncTaskFinalFailed(taskID int64) {
	TaskFinalFailed.WithLabelValues(strconv.FormatInt(taskID, 10)).Inc()
}

// ObserveTaskDuration 记录执行耗时（秒）
func ObserveTaskDuration(taskType int32, secs float64) {
	TaskDuration.WithLabelValues(strconv.FormatInt(int64(taskType), 10)).Observe(secs)
}

// SetRetryPending planner 扫描结果顺手打点（零额外查询）
func SetRetryPending(n float64) {
	RetryPending.Set(n)
}

// ObserveScheduleLag planned vs 实际触发延迟（秒）
func ObserveScheduleLag(secs float64) {
	ScheduleLag.Observe(secs)
}

// SetEngineUp engine 循环启停打点（leader 职责启动/卸任时调用）
func SetEngineUp(up bool) {
	if up {
		EngineUp.Set(1)
		return
	}
	EngineUp.Set(0)
}

// SetEngineHeartbeat engine 校准成功后刷新心跳时间戳
func SetEngineHeartbeat(t time.Time) {
	EngineHeartbeat.Set(float64(t.Unix()))
}

// ==================== 结果溢出（三段式存储）与隔离 ====================

var (
	ResultSpilled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_result_spilled_total", // 全量结果溢出到对象存储的次数
	})
	ResultSpillDegraded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_result_spill_degraded_total", // 溢出失败降级为摘要的次数（>0 说明对象存储异常）
	})
	ResultQueueWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "scheduler_result_queue_wait_seconds", // 消息从入队到被消费的滞留
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
	})
	DagBrokenIsolated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_dag_broken_isolated_total", // 定义被运行期间改坏而隔离的 DAG 运行数（>0 需要人工关注定义编辑规范）
	})
)

func IncResultSpill()                     { ResultSpilled.Inc() }
func IncResultSpillDegraded()             { ResultSpillDegraded.Inc() }
func ObserveResultQueueWait(secs float64) { ResultQueueWait.Observe(secs) }
func IncDagBrokenIsolated()               { DagBrokenIsolated.Inc() }
