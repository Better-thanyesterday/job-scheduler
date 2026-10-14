package task

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Cron 不可变值对象：解析后的 Cron 表达式。
// 支持标准 5 字段（分 时 日 月 周）与秒级 6 字段（秒 分 时 日 月 周）。
// 字段语法：* , - / 数字（L/W/# 属 P2 扩展，MVP 不支持，显式报错）

type Cron struct {
	expr     string
	sec      map[int]struct{} // 秒级字段；5 字段表达式时为 nil
	min      map[int]struct{}
	hour     map[int]struct{}
	dom      map[int]struct{} // day of month
	month    map[int]struct{}
	dow      map[int]struct{} // day of week（0-6，0=周日）
	sixField bool
}

// Parse 唯一构造入口：非法表达式构造不出来
func Parse(expr string) (Cron, error) {
	expr = strings.TrimSpace(expr)
	fields := strings.Fields(expr)
	if len(fields) != 5 && len(fields) != 6 {
		return Cron{}, fmt.Errorf("cron: %q must have 5 or 6 fields, got %d", expr, len(fields))
	}
	c := Cron{expr: expr, sixField: len(fields) == 6}
	var err error
	i := 0
	if c.sixField {
		if c.sec, err = parseField(fields[0], 0, 59); err != nil {
			return Cron{}, fmt.Errorf("cron: %q sec: %w", expr, err)
		}
		i = 1
	} else {
		c.sec = map[int]struct{}{0: {}} // 5 字段表达式固定第 0 秒触发
	}
	if c.min, err = parseField(fields[i], 0, 59); err != nil {
		return Cron{}, fmt.Errorf("cron: %q min: %w", expr, err)
	}
	if c.hour, err = parseField(fields[i+1], 0, 23); err != nil {
		return Cron{}, fmt.Errorf("cron: %q hour: %w", expr, err)
	}
	if c.dom, err = parseField(fields[i+2], 1, 31); err != nil {
		return Cron{}, fmt.Errorf("cron: %q dom: %w", expr, err)
	}
	if c.month, err = parseField(fields[i+3], 1, 12); err != nil {
		return Cron{}, fmt.Errorf("cron: %q month: %w", expr, err)
	}
	if c.dow, err = parseField(fields[i+4], 0, 6); err != nil {
		return Cron{}, fmt.Errorf("cron: %q dow: %w", expr, err)
	}
	return c, nil
}

// parseField 解析单字段：支持 * 、逗号列表、范围、步长
func parseField(f string, lo, hi int) (map[int]struct{}, error) {
	out := map[int]struct{}{}
	for _, part := range strings.Split(f, ",") {
		step := 1
		if idx := strings.Index(part, "/"); idx >= 0 {
			n, err := strconv.Atoi(part[idx+1:])
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("invalid step in %q", part)
			}
			step, part = n, part[:idx]
		}
		start, end := lo, hi
		switch part {
		case "*", "":
			// 全范围
		default:
			if idx := strings.Index(part, "-"); idx >= 0 {
				a, err1 := strconv.Atoi(part[:idx])
				b, err2 := strconv.Atoi(part[idx+1:])
				if err1 != nil || err2 != nil || a > b {
					return nil, fmt.Errorf("invalid range in %q", part)
				}
				start, end = a, b
			} else {
				n, err := strconv.Atoi(part)
				if err != nil {
					return nil, fmt.Errorf("invalid value %q", part)
				}
				if strings.Contains(f, "/") {
					end = hi // "5/10" 语义：从 5 开始每 10
				} else {
					start, end = n, n // 单值
				}
			}
		}
		if start < lo || end > hi {
			return nil, fmt.Errorf("value out of range [%d,%d] in %q", lo, hi, part)
		}
		for v := start; v <= end; v += step {
			out[v] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty field")
	}
	return out, nil
}

func (c Cron) match(t time.Time) bool {
	_, okSec := c.sec[t.Second()]
	_, okMin := c.min[t.Minute()]
	_, okHour := c.hour[t.Hour()]
	_, okDom := c.dom[t.Day()]
	_, okMonth := c.month[int(t.Month())]
	_, okDow := c.dow[int(t.Weekday())]
	return okSec && okMin && okHour && okMonth && okDom && okDow
}

// String 返回原始表达式——保证 Parse(c.String()) == c（往返恒等律的值对象版）
func (c Cron) String() string { return c.expr }

// 预览：需求 2.2"连续计算未来 5 次"
func (c Cron) NextN(after time.Time, n int) ([]time.Time, error) {
	out := make([]time.Time, 0, n)
	t := after
	for i := 0; i < n; i++ {
		next, err := c.Next(t)
		if err != nil {
			return nil, err
		}
		out = append(out, next)
		t = next
	}
	return out, nil
}

// internal/domain/task/cron.go —— 替换原 Next 的实现
// Next 计算严格晚于 after 的下一次触发时间。
// 字段跳跃算法：最坏 O(字段值空间 × 字段数) ≈ 几百次操作，与时间跨度无关。
func (c Cron) Next(after time.Time) (time.Time, error) {
	// 转入调度配置的时区语义由调用方保证：传入的 after 已是目标时区时间
	t := after.Truncate(time.Second).Add(time.Second)
	limit := t.AddDate(4, 0, 0) // 兜底：4 年无触发视为永不触发

	for t.Before(limit) {
		if c.match(t) {
			return t, nil
		}
		var jumped bool
		t, jumped = c.advance(t)
		if !jumped { // 未发生跳转却仍不匹配（理论不发生，防御）
			t = t.Add(time.Second)
		}
	}
	return time.Time{}, fmt.Errorf("cron: %q never fires within 4 years", c.expr)
}

// advance 把 t 调整到"至少跳过一个非法值"的下一个候选时刻
func (c Cron) advance(t time.Time) (time.Time, bool) {
	// 1. 月：不在合法月集合 → 跳到下一个合法月，日/时/分/秒重置为最小值
	if !c.contains(c.month, int(t.Month())) {
		next := c.nextInSet(c.month, int(t.Month()), 1, 12)
		if next < 0 { // 本年已无合法月 → 明年 1 月
			return time.Date(t.Year()+1, time.January,
				c.minOf(c.dom), c.minOf(c.hour), 0, 0, 0, t.Location()), true
		}
		return time.Date(t.Year(), time.Month(next),
			c.minOf(c.dom), c.minOf(c.hour), c.minOf(c.min), c.minOf(c.sec), 0, t.Location()), true
	}

	// 2. 日：日/周 都不匹配 → 跳到下一个合法日
	//    （注意标准 cron 语义：dom 与 dow 均受限时为 OR 关系，此处按 AND 简化，
	//     若需严格 OR 语义，将 dom/dow 匹配改为 c.matchDay(t) 单独实现）
	if !c.contains(c.dom, t.Day()) || !c.contains(c.dow, int(t.Weekday())) {
		next := t.AddDate(0, 0, 1)
		return time.Date(next.Year(), next.Month(), next.Day(),
			c.minOf(c.hour), c.minOf(c.min), c.minOf(c.sec), 0, t.Location()), true
	}

	// 3. 时
	if !c.contains(c.hour, t.Hour()) {
		next := c.nextInSet(c.hour, t.Hour(), 0, 23)
		if next < 0 { // 今日已无合法时 → 明天零点起
			n := t.AddDate(0, 0, 1)
			return time.Date(n.Year(), n.Month(), n.Day(),
				c.minOf(c.hour), c.minOf(c.min), c.minOf(c.sec), 0, t.Location()), true
		}
		return time.Date(t.Year(), t.Month(), t.Day(),
			next, c.minOf(c.min), c.minOf(c.sec), 0, t.Location()), true
	}

	// 4. 分
	if !c.contains(c.min, t.Minute()) {
		next := c.nextInSet(c.min, t.Minute(), 0, 59)
		if next < 0 { // 本小时已无合法分 → 下一小时零分
			return t.Truncate(time.Hour).Add(time.Hour).
				Add(time.Duration(c.minOf(c.min))*time.Minute +
					time.Duration(c.minOf(c.sec))*time.Second), true
		}
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(),
			next, c.minOf(c.sec), 0, t.Location()), true
	}

	// 5. 秒
	if !c.contains(c.sec, t.Second()) {
		next := c.nextInSet(c.sec, t.Second(), 0, 59)
		if next < 0 { // 本分已无合法秒 → 下一分零秒
			return t.Truncate(time.Minute).Add(time.Minute).
				Add(time.Duration(c.minOf(c.sec)) * time.Second), true
		}
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(),
			next, 0, t.Location()), true
	}
	return t, false // 全匹配（外层已处理，理论到不了）
}

// ---------- 集合辅助 ----------

func (c Cron) contains(set map[int]struct{}, v int) bool {
	_, ok := set[v]
	return ok
}

// nextInSet 在 [from, hi] 范围内找集合中 ≥ from 的最小值；无则 -1。
// 为 O(60) 线性扫，字段空间极小，不值得上有序结构
func (c Cron) nextInSet(set map[int]struct{}, from, lo, hi int) int {
	for v := from; v <= hi; v++ {
		if _, ok := set[v]; ok {
			return v
		}
	}
	_ = lo
	return -1
}

// minOf 返回集合最小值（构造时保证非空）
func (c Cron) minOf(set map[int]struct{}) int {
	m := 1 << 30
	for v := range set {
		if v < m {
			m = v
		}
	}
	return m
}
