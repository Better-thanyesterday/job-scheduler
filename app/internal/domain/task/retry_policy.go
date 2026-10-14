package task

import (
	"encoding/json"
	"fmt"
	"time"
)

// Strategy 重试策略类型
type Strategy int

const (
	StrategyFixed       Strategy = iota + 1 // 固定间隔
	StrategyExponential                     // 指数退避
)

func (s Strategy) String() string {
	switch s {
	case StrategyFixed:
		return "fixed"
	case StrategyExponential:
		return "exponential"
	default:
		return "unknown"
	}
}

// 业务边界常量——规则收口在这里，别处不许出现魔法数
const (
	MaxRetryLimit        = 10               // 最多重试 10 次
	MaxBackoffCeiling    = 30 * time.Minute // 退避封顶
	DefaultRetryInterval = 1 * time.Minute
)

// RetryPolicy 不可变值对象。注意：字段全部私有，没有任何 setter
type RetryPolicy struct {
	maxRetry int
	strategy Strategy
	interval time.Duration
}

func (r RetryPolicy) MaxRetry() int           { return r.maxRetry }
func (r RetryPolicy) Strategy() Strategy      { return r.strategy }
func (r RetryPolicy) Interval() time.Duration { return r.interval }

var (
	ErrInvalidMaxRetry = fmt.Errorf("retry policy: max retry must be in [0, %d]", MaxRetryLimit)
	ErrInvalidInterval = fmt.Errorf("retry policy: interval must be positive")
	ErrInvalidStrategy = fmt.Errorf("retry policy: unknown strategy %d", StrategyExponential+1)
)

// NewRetryPolicy 构造即校验。校验失败就返回错误，非法的 RetryPolicy 不可能存在于内存中
func NewRetryPolicy(maxRetry int, strategy Strategy, interval time.Duration) (*RetryPolicy, error) {
	if maxRetry < 0 || maxRetry > MaxRetryLimit {
		return &RetryPolicy{}, ErrInvalidMaxRetry
	}
	if strategy != StrategyFixed && strategy != StrategyExponential {
		return &RetryPolicy{}, ErrInvalidStrategy
	}
	if interval <= 0 {
		return &RetryPolicy{}, ErrInvalidInterval
	}
	return &RetryPolicy{maxRetry: maxRetry, strategy: strategy, interval: interval}, nil
}

// DefaultRetryPolicy 不配置重试时的默认值：不重试
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{maxRetry: 0, strategy: StrategyFixed, interval: DefaultRetryInterval}
}

// CanRetry 还能重试吗（当前已重试次数 vs 上限）
func (p RetryPolicy) CanRetry(retryCount int) bool {
	return retryCount < p.maxRetry
}

// NextDelay 第 retryCount 次重试前应等待多久
// 固定间隔 / 指数退避(1→2→4→8min 封顶 30min)。
// 指数分支用迭代翻倍而不是移位：retryCount 不受信时 interval<<n 会
// int64 溢出成负数/极小值，绕过天花板校验；迭代版每步检查封顶，天然防溢出
func (p RetryPolicy) NextDelay(retryCount int) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}
	d := p.interval
	if p.strategy == StrategyExponential {
		for i := 0; i < retryCount && d < MaxBackoffCeiling; i++ {
			d *= 2
		}
	}
	if d > MaxBackoffCeiling || d <= 0 {
		d = MaxBackoffCeiling
	}
	return d
}

// ---- JSON 收口：值对象自己负责序列化，infra 不用理解内部结构 ----

func (p RetryPolicy) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		MaxRetry int    `json:"max_retry"`
		Strategy string `json:"strategy"`
		Interval string `json:"interval"`
	}{
		MaxRetry: p.maxRetry,
		Strategy: p.strategy.String(),
		Interval: p.interval.String(),
	})
}

func (p *RetryPolicy) UnmarshalJSON(b []byte) error {
	var raw struct {
		MaxRetry int    `json:"max_retry"`
		Strategy string `json:"strategy"`
		Interval string `json:"interval"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	var s Strategy
	switch raw.Strategy {
	case "fixed":
		s = StrategyFixed
	case "exponential":
		s = StrategyExponential
	default:
		return ErrInvalidStrategy
	}
	interval, err := time.ParseDuration(raw.Interval)
	if err != nil {
		return fmt.Errorf("retry policy: bad interval %q: %w", raw.Interval, err)
	}
	// 关键：反序列化也走构造函数——从数据库读出来的数据同样过校验
	policy, err := NewRetryPolicy(raw.MaxRetry, s, interval)
	if err != nil {
		return err
	}
	*p = *policy
	return nil
}
