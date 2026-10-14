package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// drive 用合成时钟推进 n 个刻度（每刻度 1s），返回触发的任务——
// 注意：合成路径不执行回调，只验证"何时到期"；回调执行走 Consumer 测试
func drive(m *MultiTimeWheel, n int, start time.Time) []*Item {
	var fired []*Item
	now := start
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < n; i++ {
		now = now.Add(time.Second)
		fired = append(fired, m.stepLocked(now)...)
	}
	return fired
}

// insertSynthetic 以虚拟时钟 now 注册任务（expire 可早于 now）
func insertSynthetic(t *testing.T, m *MultiTimeWheel, now, expire time.Time, cb func()) {
	t.Helper()
	m.mu.Lock()
	m.insertLocked(&Item{ScheduleID: 1, TaskID: 1, ExpireAt: expire, callback: cb}, now)
	m.mu.Unlock()
}

// ---------------------------------------------------------------------------
// 放置与精度：细层 ceil、粗层 floor + 降级
// ---------------------------------------------------------------------------

func TestSubSecond_FiresNextTickNotEarly(t *testing.T) {
	m := NewMultiTimeWheel()
	T0 := time.Now()

	insertSynthetic(t, m, T0, T0.Add(400*time.Millisecond), func() {})

	// 第 1 刻度 now=T0+1s ≥ 到期时刻，触发；不存在更早的刻度
	fired := drive(m, 1, T0)
	if len(fired) != 1 {
		t.Fatalf("sub-second item should fire on first tick, got %d", len(fired))
	}
}

func TestExpiredInsert_FiresImmediately(t *testing.T) {
	m := NewMultiTimeWheel()
	T0 := time.Now()

	insertSynthetic(t, m, T0, T0.Add(-time.Hour), func() {}) // 早已过期

	fired := drive(m, 1, T0)
	if len(fired) != 1 {
		t.Fatal("expired item should fire on first tick")
	}
}

// 90s 在秒轮覆盖内（3600 槽），必须在第 90 刻度精确触发，之前零触发
func TestSecWheel_ExactFireAt90s(t *testing.T) {
	m := NewMultiTimeWheel()
	T0 := time.Now()
	expire := T0.Add(90 * time.Second)

	insertSynthetic(t, m, T0, expire, func() {})
	if m.ws[wheelSec].count != 1 {
		t.Fatalf("item should land in sec wheel, sec.count=%d", m.ws[wheelSec].count)
	}

	if fired := drive(m, 89, T0); len(fired) != 0 {
		t.Fatalf("fired %d items before expiry, want 0", len(fired))
	}
	fired := drive(m, 2, T0.Add(89*time.Second))
	if len(fired) != 1 || !fired[0].ExpireAt.Equal(expire) {
		t.Fatalf("fire at tick 90 missing: %+v", fired)
	}
}

// 2h30m 走 时轮(floor 2)→+2h 降级→秒轮(1800)→+2h30m 精确触发
func TestHourWheel_CascadesDownToFiresExact(t *testing.T) {
	m := NewMultiTimeWheel()
	T0 := time.Now()
	expire := T0.Add(2*time.Hour + 30*time.Minute)

	insertSynthetic(t, m, T0, expire, func() {})
	if m.ws[wheelHour].count != 1 {
		t.Fatalf("item should land in hour wheel, hour.count=%d", m.ws[wheelHour].count)
	}

	// +2h 手进槽，降级到秒轮；此期间不该触发
	if fired := drive(m, 2*3600, T0); len(fired) != 0 {
		t.Fatalf("fired %d items before demotion point, want 0", len(fired))
	}
	if m.ws[wheelSec].count != 1 {
		t.Fatalf("after hour-hand arrival item should demote to sec wheel, sec.count=%d", m.ws[wheelSec].count)
	}
	// 再走 30m：第 9000 刻度精确触发
	fired := drive(m, 30*60, T0.Add(2*time.Hour))
	if len(fired) != 1 || fired[0].ExpireAt.Equal(expire) == false {
		t.Fatalf("exact fire missing: %+v", fired)
	}
}

func TestNeverFiresBeforeExpire_AllLayers(t *testing.T) {
	m := NewMultiTimeWheel()
	T0 := time.Now()
	delays := []time.Duration{
		500 * time.Millisecond, 3 * time.Second, 61 * time.Second,
		90 * time.Second, 61 * time.Minute, 25 * time.Hour, 8 * 24 * time.Hour,
	}
	for _, d := range delays {
		insertSynthetic(t, m, T0, T0.Add(d), func() {})
	}

	// 推进 8 天 + 1 刻度：floor 降级链下所有任务精确到期
	// （61min→时轮降级秒轮；25h→天轮→时轮→秒轮；8d→周轮→天轮→秒轮）
	now := T0
	firedCount, violations := 0, 0
	m.mu.Lock()
	for i := 0; i < 8*24*3600+1; i++ {
		now = now.Add(time.Second)
		for _, it := range m.stepLocked(now) {
			firedCount++
			if now.Before(it.ExpireAt) {
				violations++ // 任何提前触发都是错的
			}
		}
	}
	m.mu.Unlock()

	if violations != 0 {
		t.Fatalf("%d early-fire violations", violations)
	}
	if firedCount != len(delays) {
		t.Fatalf("fired %d, want %d (all should be due within 8d)", firedCount, len(delays))
	}
	if got := m.Pending(); got != 0 {
		t.Fatalf("Pending=%d after all fired, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// far 列表：超覆盖范围暂存 → 进入范围回插
// ---------------------------------------------------------------------------

func TestFarLifecycle_TransfersIntoWeekWheel(t *testing.T) {
	m := NewMultiTimeWheel()
	T0 := time.Now()

	// 36 天：超出 35 天覆盖（floor 5 > 4），应进 far
	insertSynthetic(t, m, T0, T0.Add(36*24*time.Hour), func() {})
	if m.Pending() != 1 || len(m.far) != 1 {
		t.Fatalf("item should be in far list, pending=%d far=%d", m.Pending(), len(m.far))
	}

	// 推进 1 天 + 一个扫描节拍：剩余 35 天应回插周轮
	now := T0
	transferred := false
	m.mu.Lock()
	for i := 0; i < 25*3600 && !transferred; i++ {
		now = now.Add(time.Second)
		m.stepLocked(now)
		if len(m.far) == 0 && m.ws[wheelWeek].count == 1 {
			transferred = true
		}
	}
	m.mu.Unlock()
	if !transferred {
		t.Fatal("far item never transferred into week wheel")
	}
}

// ---------------------------------------------------------------------------
// API 与消费
// ---------------------------------------------------------------------------

func TestSchedule_RejectsNilCallbackAndZeroTime(t *testing.T) {
	m := NewMultiTimeWheel()
	if err := m.Schedule(1, 1, time.Now(), nil); err == nil {
		t.Error("nil callback should be rejected")
	}
	if err := m.Schedule(1, 1, time.Time{}, func() {}); err == nil {
		t.Error("zero ExpireAt should be rejected")
	}
}

func TestConsumer_RunsCallbacksAndRecoversPanic(t *testing.T) {
	m := NewMultiTimeWheel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Advance(ctx) // 推进循环 + 消费者，两个都要起
	wait := m.StartConsumer(ctx, 2)

	var counter int32
	if err := m.After(1100*time.Millisecond, func() { atomic.AddInt32(&counter, 1) }); err != nil {
		t.Fatal(err)
	}
	if err := m.After(1100*time.Millisecond, func() { panic("boom") }); err != nil {
		t.Fatal(err)
	}

	waitFor := func(want int32) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&counter) < want && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitFor(1)
	if atomic.LoadInt32(&counter) != 1 {
		t.Fatal("normal callback never ran")
	}

	// panic 的回调不能拖垮消费者：再投一个，仍能执行
	if err := m.After(1100*time.Millisecond, func() { atomic.AddInt32(&counter, 10) }); err != nil {
		t.Fatal(err)
	}
	waitFor(11)
	if atomic.LoadInt32(&counter) != 11 {
		t.Fatalf("consumer dead after panic, counter=%d", atomic.LoadInt32(&counter))
	}
	cancel()
	wait()
}

func TestIdleWheelCostsNothing(t *testing.T) {
	m := NewMultiTimeWheel()
	if fired := drive(m, 1000, time.Now()); len(fired) != 0 {
		t.Fatalf("empty wheel produced %d items", len(fired))
	}
	if m.Pending() != 0 {
		t.Fatalf("Pending=%d, want 0", m.Pending())
	}
}
