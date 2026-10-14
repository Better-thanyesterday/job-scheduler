package mq

import "testing"

func TestRoutingKeyForResult(t *testing.T) {
	const (
		timeout   = "timeout"
		execution = "execution"
	)
	cases := []struct {
		fromDag bool
		success bool
		class   string
		want    string
	}{
		{true, true, "", "job.result.dag.success"},
		{true, false, execution, "job.result.dag.failed"},
		{true, false, timeout, "job.result.dag.timeout"},
		// cron 来源无 MQ 回执（结果由 executor 直接落库，planner/reaper 轮询感知），
		// 统一返回空串，投递方据此跳过
		{false, true, "", ""},
		{false, false, execution, ""},
		{false, false, timeout, ""},
		{false, false, "", ""},
		{true, false, "weird", "job.result.dag.failed"},
	}
	for _, tc := range cases {
		if got := RoutingKeyForResult(tc.fromDag, tc.success, tc.class); got != tc.want {
			t.Fatalf("RoutingKeyForResult(dag=%v, success=%v, class=%q) = %q, want %q",
				tc.fromDag, tc.success, tc.class, got, tc.want)
		}
	}
}
