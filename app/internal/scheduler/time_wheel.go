// Package timewheel 多级时间轮：按绝对到期时间调度回调。
//
// 层级（细 → 粗）：秒(3600×1s) 时(24×1h) 天(7×1d) 周(4×7d)，
// 覆盖 35 天；更远的任务暂存 far 列表，进入覆盖范围后回插。
//
// 放置与降级：细层 ceil 落槽（保证不早触，误差 ≤1 tick）；粗层 floor 落槽，
// 指针进入槽位时剩余必然不足一层 → drain 降级到细层重排。
// 因此任意 delay ≤ 24h 的触发都精确到秒级 tick。
//
// 推进模型：Advance 每秒醒来一次，按真实流逝的秒数推进若干刻度
// （ticker 丢刻度/GC 停顿不会让轮子永久滞后）。
//
// 触发纪律：只触发 ExpireAt <= now 的任务——未到期的重新落槽
// （提前触发对调度器比延迟更有害），粒度误差最多一个槽位。
package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Item 一次调度。ScheduleID/TaskID 供上游对账与日志，轮子本身只用 ExpireAt
type Item struct {
	ScheduleID int64
	TaskID     int64
	ExpireAt   time.Time
	callback   func() // 私有：回调只能经 Schedule/After 注册
}


// wheel 单层时间轮：unit 每格时长，pos 指向"本刻度待处理"的槽
type wheel struct {
	unit  time.Duration
	slots [][]*Item
	pos   int
	count int  // 所有槽数目合计，O(1) 空判断
	fine  bool // 细层：ceil 落槽，drain 直接触发；粗层：floor 落槽，drain 降级
}

func newWheel(size int, unit time.Duration, fine bool) *wheel {
	return &wheel{unit: unit, slots: make([][]*Item, size), fine: fine}
}

func (w *wheel) size() int { return len(w.slots) }

// 层索引（细 → 粗）
const (
	wheelSec = iota
	wheelHour
	wheelDay
	wheelWeek
	wheelN
)

const (
	// farScanEvery 每推进多少秒回插一次 far 列表
	farScanEvery = 60
	// maxCatchUp 一次醒来最多补推多少刻度：长期停顿（如进程挂起）后
	// 限流补推，避免刻度风暴
	maxCatchUp = 300
)

// MultiTimeWheel 四级时间轮
type MultiTimeWheel struct {
	mu        sync.Mutex // 唯一的锁：轮结构、指针、槽内容都归它， Advance 与 Insert 竞争极小
	ws        [wheelN]*wheel
	far       []*Item // 超出覆盖范围的任务
	steps     int     // 总推进刻度数（far 回插节拍用）
	triggerCh chan *Item
}

func NewMultiTimeWheel() *MultiTimeWheel {
	m := &MultiTimeWheel{triggerCh: make(chan *Item, 4096)}
	m.ws[wheelSec] = newWheel(3600, time.Second, true) // 1h 全程秒级精度
	m.ws[wheelHour] = newWheel(24, time.Hour, false)
	m.ws[wheelDay] = newWheel(7, 24*time.Hour, false)
	m.ws[wheelWeek] = newWheel(4, 7*24*time.Hour, false)
	return m
}


// Schedule 在绝对时刻 at 触发 cb。at 已过期也能插：下个刻度立即触发
func (m *MultiTimeWheel) Schedule(scheduleID, taskID int64, at time.Time, cb func()) error {
	if cb == nil {
		return fmt.Errorf("timewheel: nil callback (schedule=%d task=%d)", scheduleID, taskID)
	}
	if at.IsZero() {
		return fmt.Errorf("timewheel: zero ExpireAt (schedule=%d task=%d)", scheduleID, taskID)
	}
	m.mu.Lock()
	m.insertLocked(&Item{ScheduleID: scheduleID, TaskID: taskID, ExpireAt: at, callback: cb}, time.Now())
	m.mu.Unlock()
	return nil
}

// After 延迟 delay 后触发 cb
func (m *MultiTimeWheel) After(delay time.Duration, cb func()) error {
	return m.Schedule(0, 0, time.Now().Add(delay), cb)
}

// Pending 当前未触发的任务总数（监控用）
func (m *MultiTimeWheel) Pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.far)
	for _, w := range m.ws {
		n += w.count
	}
	return n
}

// insertLocked 从细到粗找第一层能容纳的层插入；超过覆盖范围进 far。
// now 由调用方给：测试可注入虚拟时钟
func (m *MultiTimeWheel) insertLocked(it *Item, now time.Time) {
	delay := it.ExpireAt.Sub(now)
	for i := 0; i < wheelN; i++ {
		if offset, ok := m.ws[i].fit(delay); ok {
			m.placeAt(i, it, offset)
			return
		}
	}
	m.far = append(m.far, it)
}

// fit 本层能否容纳 delay，返回落槽偏移：
//   - 细层 ceil：槽位到达时间 ∈ [expire, expire+1tick)，配合 drain 的
//     now>=ExpireAt 判定绝不早触
//   - 粗层 floor：槽位到达时剩余必然不足一层（∈ (0,unit)），drain 降级到
//     细层重排——粗层只当"缓冲区"，精度由细层兜底
func (w *wheel) fit(delay time.Duration) (int, bool) {
	var off int
	if w.fine {
		off = ceilDiv(delay, w.unit) // ≥1，含 delay≤0 的立即触发
	} else {
		off = int(delay / w.unit) // floor
		if off < 1 {
			return 0, false // 不足一层：该进更细的层
		}
	}
	if off > w.size() {
		return 0, false // 超出本层覆盖
	}
	return off, true
}

func (m *MultiTimeWheel) placeAt(i int, it *Item, offset int) {
	w := m.ws[i]
	slot := (w.pos + offset) % w.size()
	w.slots[slot] = append(w.slots[slot], it)
	w.count++
}

func ceilDiv(d, unit time.Duration) int {
	if d <= 0 {
		return 1
	}
	return int((d + unit - 1) / unit)
}

// ---------- 推进 ----------

// Advance 阻塞推进循环（独立 goroutine 运行）：
// 按 wall clock 实际流逝的秒数补推刻度，到期的任务发进 triggerCh
// （在锁外发送：channel 满时形成背压，但不会卡住 Insert）
func (m *MultiTimeWheel) Advance(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	anchor := time.Now() // 已推进到的时刻，刻度从它的整秒差累计
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			ticks := int(now.Sub(anchor) / time.Second)
			if ticks <= 0 { // ticker 抖动早醒
				continue
			}
			if ticks > maxCatchUp {
				ticks = maxCatchUp
				anchor = now.Add(-maxCatchUp * time.Second)
			} else {
				anchor = anchor.Add(time.Duration(ticks) * time.Second)
			}

			m.mu.Lock()
			if m.idleLocked() {
				m.mu.Unlock()
				continue
			}
			due := make([]*Item, 0, 8)
			for i := 0; i < ticks; i++ {
				due = append(due, m.stepLocked(now)...)
			}
			m.mu.Unlock()

			for _, it := range due {
				m.triggerCh <- it
			}
		}
	}
}

func (m *MultiTimeWheel) idleLocked() bool {
	if len(m.far) > 0 {
		return false
	}
	for _, w := range m.ws {
		if w.count > 0 {
			return false
		}
	}
	return true
}

// stepLocked 推进一个刻度：细层走一格并处理该槽；走满一圈粗层走一格，
// 逐层级联。返回本刻度到期的任务
func (m *MultiTimeWheel) stepLocked(now time.Time) []*Item {
	m.steps++
	var due []*Item
	for i := 0; i < wheelN; i++ {
		w := m.ws[i]
		w.pos = (w.pos + 1) % w.size()
		due = append(due, m.drainLocked(i, now)...)
		if w.pos != 0 {
			break // 本层没走满一圈，更粗的层不动
		}
	}
	// far 回插：低频任务，节拍扫描即可
	if len(m.far) > 0 && m.steps%farScanEvery == 0 {
		m.reinsertFarLocked(now)
	}
	return due
}

// drainLocked 取走第 i 层当前槽：到期的进 due；未到期的（插放抖动/
// 补推导致提前到槽）按剩余延迟重新落层——绝不提前触发
func (m *MultiTimeWheel) drainLocked(i int, now time.Time) []*Item {
	w := m.ws[i]
	slot := w.slots[w.pos]
	w.slots[w.pos] = nil
	w.count -= len(slot)

	var due []*Item
	for _, it := range slot {
		if !now.Before(it.ExpireAt) {
			due = append(due, it)
		} else {
			m.insertLocked(it, now)
		}
	}
	return due
}

func (m *MultiTimeWheel) reinsertFarLocked(now time.Time) {
	// floor 放置下周轮的实际覆盖是 [0, (size+1)*unit) = 35 天
	wk := m.ws[wheelWeek]
	horizon := wk.unit * time.Duration(wk.size()+1)
	remaining := m.far[:0]
	for _, it := range m.far {
		if it.ExpireAt.Sub(now) < horizon {
			m.insertLocked(it, now) // 进入覆盖范围，回插
		} else {
			remaining = append(remaining, it)
		}
	}
	m.far = remaining
}

// ---------- 消费 ----------

// StartConsumer 起 n 个消费 goroutine 执行回调（并发上限 n），
// 返回等待函数供优雅退出时 join。回调 panic 被捕获记日志，不拖垮进程
func (m *MultiTimeWheel) StartConsumer(ctx context.Context, n int) (wait func()) {
	if n <= 0 {
		n = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case it := <-m.triggerCh:
					runCallback(it)
				}
			}
		}()
	}
	return wg.Wait
}

func runCallback(it *Item) {
	defer func() {
		if r := recover(); r != nil {
			zap.L().Error("timewheel: 回调 panic",
				zap.Int64("schedule_id", it.ScheduleID),
				zap.Int64("task_id", it.TaskID),
				zap.Any("panic", r))
		}
	}()
	if lag := time.Since(it.ExpireAt); lag > 2*time.Second {
		zap.L().Warn("timewheel: 触发滞后",
			zap.Int64("schedule_id", it.ScheduleID),
			zap.Duration("lag", lag))
	}
	it.callback()
}
