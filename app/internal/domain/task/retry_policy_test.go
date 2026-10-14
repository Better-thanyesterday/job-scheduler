package task

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestNewRetryPolicy(t *testing.T) {
	tests := []struct {
		name     string
		maxRetry int
		strategy Strategy
		interval time.Duration
		wantErr  error
		want     *RetryPolicy
	}{
		{
			name:     "valid fixed",
			maxRetry: 3,
			strategy: StrategyFixed,
			interval: 2 * time.Minute,
			wantErr:  nil,
			want:     &RetryPolicy{maxRetry: 3, strategy: StrategyFixed, interval: 2 * time.Minute},
		},
		{
			name:     "valid exponential",
			maxRetry: 5,
			strategy: StrategyExponential,
			interval: 1 * time.Minute,
			wantErr:  nil,
			want:     &RetryPolicy{maxRetry: 5, strategy: StrategyExponential, interval: 1 * time.Minute},
		},
		{
			name:     "max retry zero",
			maxRetry: 0,
			strategy: StrategyFixed,
			interval: time.Second,
			wantErr:  nil,
			want:     &RetryPolicy{maxRetry: 0, strategy: StrategyFixed, interval: time.Second},
		},
		{
			name:     "max retry exceeds limit",
			maxRetry: MaxRetryLimit + 1,
			strategy: StrategyFixed,
			interval: time.Second,
			wantErr:  ErrInvalidMaxRetry,
		},
		{
			name:     "negative max retry",
			maxRetry: -1,
			strategy: StrategyFixed,
			interval: time.Second,
			wantErr:  ErrInvalidMaxRetry,
		},
		{
			name:     "invalid strategy",
			maxRetry: 1,
			strategy: Strategy(99),
			interval: time.Second,
			wantErr:  ErrInvalidStrategy,
		},
		{
			name:     "zero interval",
			maxRetry: 1,
			strategy: StrategyFixed,
			interval: 0,
			wantErr:  ErrInvalidInterval,
		},
		{
			name:     "negative interval",
			maxRetry: 1,
			strategy: StrategyFixed,
			interval: -time.Second,
			wantErr:  ErrInvalidInterval,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewRetryPolicy(tt.maxRetry, tt.strategy, tt.interval)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewRetryPolicy() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewRetryPolicy() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	if p.maxRetry != 0 {
		t.Errorf("DefaultRetryPolicy().maxRetry = %d, want 0", p.maxRetry)
	}
	if p.strategy != StrategyFixed {
		t.Errorf("DefaultRetryPolicy().strategy = %v, want %v", p.strategy, StrategyFixed)
	}
	if p.interval != DefaultRetryInterval {
		t.Errorf("DefaultRetryPolicy().interval = %v, want %v", p.interval, DefaultRetryInterval)
	}
	if p.CanRetry(0) {
		t.Errorf("DefaultRetryPolicy().CanRetry(0) = true, want false")
	}
}

func TestRetryPolicy_CanRetry(t *testing.T) {
	p, err := NewRetryPolicy(3, StrategyFixed, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		retryCount int
		want       bool
	}{
		{0, true},
		{1, true},
		{2, true},
		{3, false},
		{4, false},
	}
	for _, tt := range tests {
		if got := p.CanRetry(tt.retryCount); got != tt.want {
			t.Errorf("CanRetry(%d) = %v, want %v", tt.retryCount, got, tt.want)
		}
	}
}

func TestRetryPolicy_NextDelay(t *testing.T) {
	// 固定间隔
	fixed, _ := NewRetryPolicy(5, StrategyFixed, 2*time.Minute)
	for i := 0; i <= 5; i++ {
		if d := fixed.NextDelay(i); d != 2*time.Minute {
			t.Errorf("fixed NextDelay(%d) = %v, want %v", i, d, 2*time.Minute)
		}
	}
	if d := fixed.NextDelay(-1); d != 2*time.Minute {
		t.Errorf("fixed NextDelay(-1) = %v, want %v", d, 2*time.Minute)
	}

	// 指数退避
	exp, _ := NewRetryPolicy(10, StrategyExponential, 1*time.Minute)
	expected := []time.Duration{
		1 * time.Minute,  // 0
		2 * time.Minute,  // 1
		4 * time.Minute,  // 2
		8 * time.Minute,  // 3
		16 * time.Minute, // 4
		30 * time.Minute, // 5 -> 32 封顶 30
		30 * time.Minute, // 6
	}
	for i, want := range expected {
		if d := exp.NextDelay(i); d != want {
			t.Errorf("exp NextDelay(%d) = %v, want %v", i, d, want)
		}
	}
	// 负数
	if d := exp.NextDelay(-5); d != 1*time.Minute {
		t.Errorf("exp NextDelay(-5) = %v, want %v", d, 1*time.Minute)
	}
}

func TestStrategy_String(t *testing.T) {
	tests := []struct {
		s    Strategy
		want string
	}{
		{StrategyFixed, "fixed"},
		{StrategyExponential, "exponential"},
		{Strategy(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("Strategy(%d).String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}

func TestRetryPolicy_MarshalJSON(t *testing.T) {
	p, _ := NewRetryPolicy(3, StrategyExponential, 2*time.Minute)
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"max_retry":3,"strategy":"exponential","interval":"2m0s"}`
	if string(data) != want {
		t.Errorf("MarshalJSON() = %s, want %s", string(data), want)
	}
}

func TestRetryPolicy_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		want    RetryPolicy
		wantErr error
	}{
		{
			name: "valid fixed",
			json: `{"max_retry":2,"strategy":"fixed","interval":"30s"}`,
			want: RetryPolicy{maxRetry: 2, strategy: StrategyFixed, interval: 30 * time.Second},
		},
		{
			name: "valid exponential",
			json: `{"max_retry":5,"strategy":"exponential","interval":"1m"}`,
			want: RetryPolicy{maxRetry: 5, strategy: StrategyExponential, interval: 1 * time.Minute},
		},
		{
			name:    "invalid strategy",
			json:    `{"max_retry":1,"strategy":"unknown","interval":"1s"}`,
			wantErr: ErrInvalidStrategy,
		},
		{
			name:    "invalid interval format",
			json:    `{"max_retry":1,"strategy":"fixed","interval":"bad"}`,
			wantErr: errors.New("bad interval"), // 仅检查非 nil
		},
		{
			name:    "max retry exceeds limit",
			json:    `{"max_retry":100,"strategy":"fixed","interval":"1s"}`,
			wantErr: ErrInvalidMaxRetry,
		},
		{
			name:    "negative max retry",
			json:    `{"max_retry":-1,"strategy":"fixed","interval":"1s"}`,
			wantErr: ErrInvalidMaxRetry,
		},
		{
			name:    "zero interval",
			json:    `{"max_retry":1,"strategy":"fixed","interval":"0s"}`,
			wantErr: ErrInvalidInterval,
		},
		{
			name:    "invalid json",
			json:    `{bad json}`,
			wantErr: errors.New("invalid character"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p RetryPolicy
			err := json.Unmarshal([]byte(tt.json), &p)
			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("UnmarshalJSON() error = nil, wantErr %v", tt.wantErr)
				}
				// 对已定义的错误变量做精确匹配，其他只检查非 nil
				if errors.Is(tt.wantErr, ErrInvalidStrategy) ||
					errors.Is(tt.wantErr, ErrInvalidMaxRetry) ||
					errors.Is(tt.wantErr, ErrInvalidInterval) {
					if !errors.Is(err, tt.wantErr) {
						t.Errorf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("UnmarshalJSON() unexpected error = %v", err)
			}
			if !reflect.DeepEqual(p, tt.want) {
				t.Errorf("UnmarshalJSON() = %+v, want %+v", p, tt.want)
			}
		})
	}
}

// 专门测试 UnmarshalJSON 是否正确更新了接收者（揭示 p = policy 的 bug）
func TestRetryPolicy_UnmarshalJSON_UpdatesReceiver(t *testing.T) {
	var p RetryPolicy
	err := json.Unmarshal([]byte(`{"max_retry":2,"strategy":"fixed","interval":"1m"}`), &p)
	if err != nil {
		t.Fatal(err)
	}
	want := RetryPolicy{maxRetry: 2, strategy: StrategyFixed, interval: time.Minute}
	if !reflect.DeepEqual(p, want) {
		t.Errorf("UnmarshalJSON did not update receiver: got %+v, want %+v", p, want)
	}
}

func TestRetryPolicy_JSONRoundTrip(t *testing.T) {
	original, _ := NewRetryPolicy(4, StrategyExponential, 3*time.Minute)
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored RetryPolicy
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, &restored) {
		t.Errorf("JSON round trip failed: original=%+v, restored=%+v", original, &restored)
	}
}
