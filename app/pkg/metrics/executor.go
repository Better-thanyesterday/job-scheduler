package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// executor 进程专属指标：定义、采集辅助集中在这里，与 scheduler 侧共用 registry.go 的注册表

var (
	// ExecutorRunning 当前执行中的任务数（gRPC 认领成功到终态落库之间）
	ExecutorRunning = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "executor_running_tasks",
	})

	// GrpcExecutions 收到的 Execute 推送计数（code: ok / invalid / unavailable）
	GrpcExecutions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "executor_grpc_requests_total",
	}, []string{"code"})

	// ExecutorUp 进程存活（/metrics 抓到 1 即在线；配合 etcd 成员表互补）
	ExecutorUp = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "executor_up",
	})
)

// SetExecutorRunning 刷新执行中任务数
func SetExecutorRunning(n float64) {
	ExecutorRunning.Set(n)
}

// IncGrpcExecution 统计一次 Execute 推送（code: ok / invalid / unavailable）
func IncGrpcExecution(code string) {
	GrpcExecutions.WithLabelValues(code).Inc()
}

// SetExecutorUp 进程启停打点
func SetExecutorUp(up bool) {
	if up {
		ExecutorUp.Set(1)
		return
	}
	ExecutorUp.Set(0)
}
