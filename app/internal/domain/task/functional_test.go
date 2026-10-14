package task_test

import (
	"testing"
	"time"

	"job-scheduler/app/internal/domain/task"
)

// 功能验证：cron 解析 + Next 计算（调度触发时刻的来源）
func TestCronParseAndNext(t *testing.T) {
	// 非法表达式必须构造不出来
	bad := []string{
		"",              // 空
		"*/5 * * *",     // 字段不足
		"* * * * * * *", // 字段过多
		"0 0 32 * * *",  // 日越界
		"60 0 * * * *",  // 分越界
	}
	for _, expr := range bad {
		if _, err := task.Parse(expr); err == nil {
			t.Errorf("Parse(%q) 应当报错，却成功了", expr)
		}
	}

	// 合法表达式：工作日 09:30:00（六字段：秒 分 时 日 月 周）
	c, err := task.Parse("0 30 9 * * 1-5")
	if err != nil {
		t.Fatalf("合法表达式解析失败: %v", err)
	}

	// 2026-09-25 是周五：周五 09:30 之后的下一次触发必须跳过周末到下周一
	after := time.Date(2026, 9, 25, 9, 30, 0, 0, time.Local)
	next, err := c.Next(after)
	if err != nil {
		t.Fatalf("Next 失败: %v", err)
	}
	want := time.Date(2026, 9, 28, 9, 30, 0, 0, time.Local)
	if !next.Equal(want) {
		t.Fatalf("Next = %v, 期望 %v（跳过周末）", next, want)
	}
	if !next.After(after) {
		t.Fatal("Next 必须严格晚于给定时刻（不重放同一秒）")
	}

	// 连续计算 N 次：每天一个点，全部落在工作日 09:30
	ns, err := c.NextN(next, 3)
	if err != nil {
		t.Fatalf("NextN 失败: %v", err)
	}
	if len(ns) != 3 {
		t.Fatalf("NextN 返回 %d 个点, 期望 3", len(ns))
	}
	for i, n := range ns {
		if n.Weekday() == time.Saturday || n.Weekday() == time.Sunday {
			t.Fatalf("第 %d 次触发 %v 落在周末", i, n)
		}
		if n.Hour() != 9 || n.Minute() != 30 {
			t.Fatalf("第 %d 次触发 %v 时刻错误", i, n)
		}
	}
}

// 功能验证：重试退避策略（planner 的重投间隔来源）
func TestRetryPolicyBackoff(t *testing.T) {
	// 固定间隔：每次都是同一个值
	fixed, err := task.NewRetryPolicy(3, task.StrategyFixed, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if got := fixed.NextDelay(i); got != time.Minute {
			t.Fatalf("固定策略第 %d 次退避 = %v, 期望 1m", i, got)
		}
	}

	// 指数退避：前期逐次递增，后期封顶
	exp, err := task.NewRetryPolicy(10, task.StrategyExponential, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prev := time.Duration(0)
	for i := 1; i <= 4; i++ { // 1m 2m 4m 8m 16m，前几轮严格递增
		got := exp.NextDelay(i)
		if got <= prev {
			t.Fatalf("指数退避未递增: 第 %d 次 = %v, 上一次 = %v", i, got, prev)
		}
		prev = got
	}
	// 封顶：planner 可达范围（retryCount < MaxRetryLimit=10）内一律 30m
	for i := 5; i < 10; i++ {
		if got := exp.NextDelay(i); got != task.MaxBackoffCeiling {
			t.Fatalf("退避未封顶: 第 %d 次 = %v, 期望 %v", i, got, task.MaxBackoffCeiling)
		}
	}
	// 已知隐患（未修）：i 足够大时 interval<<i 溢出 int64 变负数，封顶检查拦不住。
	// planner 侧 RetryCount>=MaxRetryLimit 时不会调 NextDelay，实际不可达；
	// 若将来放开上限，先在 NextDelay 里做溢出保护

	// 非法参数构造不出来
	if _, err := task.NewRetryPolicy(99, task.StrategyFixed, time.Minute); err == nil {
		t.Fatal("maxRetry 超限应当报错")
	}
	if _, err := task.NewRetryPolicy(1, task.Strategy(9), time.Minute); err == nil {
		t.Fatal("未知策略应当报错")
	}
	if _, err := task.NewRetryPolicy(1, task.StrategyFixed, 0); err == nil {
		t.Fatal("interval<=0 应当报错")
	}
}
