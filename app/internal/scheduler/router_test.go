package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	infraetcd "job-scheduler/app/internal/infra/etcd"
)

func mkInfos(rows ...infraetcd.ExecutorInfo) []infraetcd.ExecutorInfo {
	return rows
}

// ---------------------------------------------------------------------------
// 择优：负载率最低者优先
// ---------------------------------------------------------------------------

func TestCandidates_LeastLoadedFirst(t *testing.T) {
	restore := freezeNow(t)
	defer restore()

	r := newRouter(func(ctx context.Context) ([]infraetcd.ExecutorInfo, error) {
		return mkInfos(
			infraetcd.ExecutorInfo{WorkerID: "busy", Addr: "a:1", MaxConcurr: 10, Running: 8},
			infraetcd.ExecutorInfo{WorkerID: "idle", Addr: "b:1", MaxConcurr: 10, Running: 1},
			infraetcd.ExecutorInfo{WorkerID: "mid", Addr: "c:1", MaxConcurr: 10, Running: 5},
		), nil
	})

	cands, err := r.candidates(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"b:1", "c:1", "a:1"}
	for i, w := range want {
		if cands[i].Addr != w {
			t.Fatalf("cands[%d].Addr = %s, want %s", i, cands[i].Addr, w)
		}
	}
}

// 在途增量参与负载：etcd 快照相同，但本进程已向 a 压了 5 条在途 → 选 b
func TestCandidates_InflightCounts(t *testing.T) {
	restore := freezeNow(t)
	defer restore()

	r := newRouter(func(ctx context.Context) ([]infraetcd.ExecutorInfo, error) {
		return mkInfos(
			infraetcd.ExecutorInfo{WorkerID: "w1", Addr: "a:1", MaxConcurr: 10, Running: 0},
			infraetcd.ExecutorInfo{WorkerID: "w2", Addr: "b:1", MaxConcurr: 10, Running: 0},
		), nil
	})
	r.acquire("a:1")
	r.acquire("a:1")
	r.acquire("a:1")

	cands, _ := r.candidates(context.Background(), 1)
	if cands[0].Addr != "b:1" {
		t.Fatalf("picked %s, want b:1 (a has inflight debt)", cands[0].Addr)
	}
}

// ---------------------------------------------------------------------------
// 熔断：冷却窗口内的失败实例排到最后，但不消失
// ---------------------------------------------------------------------------

func TestCandidates_FailureCooldownDemotes(t *testing.T) {
	base := time.Now()
	restore := setNow(t, base)
	defer restore()

	r := newRouter(func(ctx context.Context) ([]infraetcd.ExecutorInfo, error) {
		return mkInfos(
			infraetcd.ExecutorInfo{WorkerID: "flaky", Addr: "a:1", MaxConcurr: 10, Running: 0},
			infraetcd.ExecutorInfo{WorkerID: "loaded", Addr: "b:1", MaxConcurr: 10, Running: 9},
		), nil
	})
	r.release("a:1", false) // a 刚失败

	// 冷却窗口内：失败的 flaky 即使空闲也排在 90% 负载的 loaded 之后
	cands, _ := r.candidates(context.Background(), 1)
	if cands[0].Addr != "b:1" {
		t.Fatalf("picked %s, want b:1 (failed instance demoted)", cands[0].Addr)
	}
	if len(cands) != 2 {
		t.Fatal("failed instance must remain as fallback candidate")
	}

	// 冷却过后恢复
	restore2 := setNow(t, base.Add(failCooldown+time.Second))
	defer restore2()
	cands, _ = r.candidates(context.Background(), 1)
	if cands[0].Addr != "a:1" {
		t.Fatalf("picked %s, want a:1 (cooldown expired)", cands[0].Addr)
	}
}

// ---------------------------------------------------------------------------
// HRW：并列负载时同一 execID 稳定选同一台，不同 execID 有分散
// ---------------------------------------------------------------------------

func TestCandidates_HRWTieBreakStable(t *testing.T) {
	restore := freezeNow(t)
	defer restore()

	r := newRouter(func(ctx context.Context) ([]infraetcd.ExecutorInfo, error) {
		return mkInfos(
			infraetcd.ExecutorInfo{WorkerID: "w1", Addr: "a:1", MaxConcurr: 10},
			infraetcd.ExecutorInfo{WorkerID: "w2", Addr: "b:1", MaxConcurr: 10},
			infraetcd.ExecutorInfo{WorkerID: "w3", Addr: "c:1", MaxConcurr: 10},
		), nil
	})

	// 同一 execID 多次取候选，首选必须一致
	first, _ := r.candidates(context.Background(), 42)
	for i := 0; i < 10; i++ {
		cands, _ := r.candidates(context.Background(), 42)
		if cands[0].Addr != first[0].Addr {
			t.Fatalf("unstable pick for exec 42: %s then %s", first[0].Addr, cands[0].Addr)
		}
	}

	// 大样本下三个节点都应被命中（全空负载靠 HRW 打散）
	hit := map[string]int{}
	for id := int64(1); id <= 300; id++ {
		cands, _ := r.candidates(context.Background(), id)
		hit[cands[0].Addr]++
	}
	if len(hit) != 3 {
		t.Fatalf("only %d nodes hit over 300 ids: %v", len(hit), hit)
	}
}

// ---------------------------------------------------------------------------
// 快照缓存与降级
// ---------------------------------------------------------------------------

func TestSnapshot_CacheAndStaleFallback(t *testing.T) {
	base := time.Now()
	restore := setNow(t, base)
	defer restore()

	var calls int32
	r := newRouter(func(ctx context.Context) ([]infraetcd.ExecutorInfo, error) {
		atomic.AddInt32(&calls, 1)
		return mkInfos(infraetcd.ExecutorInfo{WorkerID: "w", Addr: "a:1", MaxConcurr: 1}), nil
	})

	for i := 0; i < 5; i++ { // 缓存窗口内只拉一次
		if _, err := r.snapshot(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("list called %d times within cache window, want 1", got)
	}

	// 过期后再拉：这次 list 报错 → 降级用旧快照，不返回错误
	restore2 := setNow(t, base.Add(routeRefresh+time.Second))
	defer restore2()
	r.list = func(ctx context.Context) ([]infraetcd.ExecutorInfo, error) {
		return nil, errors.New("etcd down")
	}
	infos, err := r.snapshot(context.Background())
	if err != nil || len(infos) != 1 {
		t.Fatalf("stale fallback failed: err=%v infos=%v", err, infos)
	}

	// 旧快照也没有 → 如实报错
	r2 := newRouter(func(ctx context.Context) ([]infraetcd.ExecutorInfo, error) {
		return nil, errors.New("etcd down")
	})
	if _, err := r2.snapshot(context.Background()); err == nil {
		t.Fatal("expected error when no cache and list fails")
	}
}

func TestCandidates_NoExecutor(t *testing.T) {
	restore := freezeNow(t)
	defer restore()

	r := newRouter(func(ctx context.Context) ([]infraetcd.ExecutorInfo, error) {
		return nil, nil
	})
	if _, err := r.candidates(context.Background(), 1); err == nil {
		t.Fatal("empty member table should error")
	}
}

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

func setNow(t *testing.T, tm time.Time) (restore func()) {
	t.Helper()
	old := now
	now = func() time.Time { return tm }
	return func() { now = old }
}

func freezeNow(t *testing.T) (restore func()) {
	return setNow(t, time.Now())
}
