package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// 功能验证：终态计数器按 status 标签独立累加（告警 success_rate 的数据源）
func TestTaskExecutedCounter(t *testing.T) {
	label := "functional-test"
	before := testutil.ToFloat64(TaskExecuted.WithLabelValues(label))

	IncTaskExecuted(label)
	IncTaskExecuted(label)

	after := testutil.ToFloat64(TaskExecuted.WithLabelValues(label))
	if after-before != 2 {
		t.Fatalf("计数增量 = %v, 期望 2", after-before)
	}
}
