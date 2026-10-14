package task_test

import (
	"testing"
	"time"

	"job-scheduler/app/internal/domain/task"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{"五字段-每分钟", "* * * * *", false},
		{"五字段-步长", "*/15 9-18 * * 1-5", false},
		{"六字段-秒级", "0 */5 * * * *", false},
		{"字段数不足", "*/5 * * *", true},
		{"字段数过多", "0 * * * * * *", true},
		{"dom 越界", "0 0 0 32 * *", true},
		{"dow 越界", "0 0 * * * 7", true},
		{"L 不支持(P2 扩展显式拒绝)", "0 0 * * * L", true},
		{"空表达式", "", true},
		{"范围倒挂", "0 50-10 * * * *", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := task.Parse(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse(%q) err = %v, wantErr = %v", tt.expr, err, tt.wantErr)
			}
		})
	}
}

func TestParse_StringRoundTrip(t *testing.T) {
	expr := "*/7 3-18 * * 1-5"
	c, err := task.Parse(expr)
	if err != nil {
		t.Fatal(err)
	}
	if c.String() != expr {
		t.Fatalf("round trip broken: got %q", c.String())
	}
}

func TestCron_Next(t *testing.T) {
	loc := time.Local
	c, err := task.Parse("0 30 9 * * 1-5") // 工作日 09:30
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		after time.Time
		want  time.Time
	}{
		{
			// 2026-09-23 是周三；周三 10:00 的下一次触发是周四 09:30
			name:  "周三上午之后→次日工作日",
			after: time.Date(2026, 9, 23, 10, 0, 0, 0, loc),
			want:  time.Date(2026, 9, 24, 9, 30, 0, 0, loc),
		},
		{
			// 周五 09:30 之后→跳过周末，下周一
			name:  "周五触发点之后→跳过周末",
			after: time.Date(2026, 9, 25, 9, 30, 0, 0, loc),
			want:  time.Date(2026, 9, 28, 9, 30, 0, 0, loc),
		},
		{
			name:  "严格晚于 after（不重放同一秒）",
			after: time.Date(2026, 9, 23, 9, 30, 0, 0, loc),
			want:  time.Date(2026, 9, 24, 9, 30, 0, 0, loc),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Next(tt.after)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("Next = %v, want %v", got, tt.want)
			}
		})
	}
}
