package task

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// ---------- 密封接口：所有任务配置的公共能力 ----------
// TaskConfig 任务配置的公共接口。
// seal() 是未导出方法 —— 只有本包内的类型能实现这个接口，
// 外部包无法伪造新配置类型，switch 分支因此是完备的。
type TaskConfig interface {
	Type() TaskType
	Validate() error
	seal()
}

// internal/domain/task/task_config.go 里追加

// NewTaskConfig 按任务类型反序列化并校验配置。
// 这是所有 "JSON → TaskConfig" 的唯一入口：
//   - infra 层从数据库加载（taskToEntity）
//   - api 层从请求体创建（logic.CreateTask）
//
// 两条路都走这里，不存在绕过校验的路径。
func NewTaskConfig(t TaskType, raw json.RawMessage) (TaskConfig, error) {
	var cfg TaskConfig
	switch t {
	case TaskTypeHTTP:
		cfg = &HTTPConfig{}
	case TaskTypeScript:
		cfg = &ScriptConfig{}
	case TaskTypeMessage:
		cfg = &MessageConfig{}
	case TaskTypeSQL:
		cfg = &SQLConfig{}
	case TaskTypeSubDAG:
		cfg = &SubDAGConfig{}
	default:
		return nil, fmt.Errorf("task config: unknown task type %d", t)
	}

	if len(raw) == 0 {
		return nil, fmt.Errorf("task config: config body is empty for type %d", t)
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("task config: unmarshal type %d: %w", t, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// MarshalConfig 序列化的对称出口。
// 之所以不直接 json.Marshal(cfg)：cfg 是接口，
// 显式包装一层让 infra 代码意图更清晰，将来若需在序列化时
// 附加版本号/脱敏字段，只改这一处。
func MarshalConfig(cfg TaskConfig) (json.RawMessage, error) {
	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("task config: marshal type %d: %w", cfg.Type(), err)
	}
	return b, nil
}

// ---------- 类型一：HTTP ----------

type HTTPConfig struct {
	URL             string            `json:"url"`
	Method          string            `json:"method"`
	Headers         map[string]string `json:"headers,omitempty"`
	Body            string            `json:"body,omitempty"`
	ExpectedCodes   []int             `json:"expected_codes,omitempty"` // 空则默认 2xx
	TimeoutOverride time.Duration     `json:"timeout,omitempty"`
}

func (c *HTTPConfig) Type() TaskType { return TaskTypeHTTP }
func (c *HTTPConfig) seal()          {}

func (c *HTTPConfig) Validate() error {
	u, err := url.Parse(c.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("http config: invalid url %q", c.URL)
	}
	switch strings.ToUpper(c.Method) {
	case "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD":
	default:
		return fmt.Errorf("http config: unsupported method %q", c.Method)
	}
	return nil
}

// 行为示例：判断响应码是否算成功——规则属于配置，不属于 executor
func (c *HTTPConfig) IsSuccess(code int) bool {
	if len(c.ExpectedCodes) == 0 {
		return code >= 200 && code < 300
	}
	for _, want := range c.ExpectedCodes {
		if code == want {
			return true
		}
	}
	return false
}

// ---------- 类型二：Script ----------

type ScriptConfig struct {
	Content     string `json:"content"`     // 脚本内容（或路径，二选一策略在构造时定死）
	Interpreter string `json:"interpreter"` // sh / bash / python3
}

func (c *ScriptConfig) Type() TaskType { return TaskTypeScript }
func (c *ScriptConfig) seal()          {}

func (c *ScriptConfig) Validate() error {
	if strings.TrimSpace(c.Content) == "" {
		return fmt.Errorf("script config: content is empty")
	}
	switch c.Interpreter {
	case "sh", "bash", "python3":
	default:
		return fmt.Errorf("script config: unsupported interpreter %q", c.Interpreter)
	}
	return nil
}

// ---------- 类型三：SQL ----------

type SQLConfig struct {
	DSN      string `json:"dsn"`
	Query    string `json:"query"`
	ReadOnly bool   `json:"read_only"` // 只读保护：UPDATE/DELETE 直接拒绝
}

func (c *SQLConfig) Type() TaskType { return TaskTypeSQL }
func (c *SQLConfig) seal()          {}

func (c *SQLConfig) Validate() error {
	if strings.TrimSpace(c.DSN) == "" {
		return fmt.Errorf("sql config: dsn is empty")
	}
	if strings.TrimSpace(c.Query) == "" {
		return fmt.Errorf("sql config: query is empty")
	}
	if c.ReadOnly && isWriteQuery(c.Query) {
		return fmt.Errorf("sql config: write query not allowed in read-only mode")
	}
	return nil
}

func isWriteQuery(q string) bool {
	q = strings.ToUpper(strings.TrimSpace(q))
	return strings.HasPrefix(q, "UPDATE") || strings.HasPrefix(q, "DELETE") ||
		strings.HasPrefix(q, "DROP") || strings.HasPrefix(q, "TRUNCATE")
}

// ---------- Message / SubDAG 结构类似，略 ----------

type MessageConfig struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

func (c *MessageConfig) Type() TaskType { return TaskTypeMessage }
func (c *MessageConfig) seal()          {}
func (c *MessageConfig) Validate() error {
	if c.Topic == "" {
		return fmt.Errorf("message config: topic is empty")
	}
	return nil
}

type SubDAGConfig struct {
	DAGID   int64                  `json:"dag_id"`
	Context map[string]interface{} `json:"context,omitempty"`
}

func (c *SubDAGConfig) Type() TaskType { return TaskTypeSubDAG }
func (c *SubDAGConfig) seal()          {}
func (c *SubDAGConfig) Validate() error {
	if c.DAGID <= 0 {
		return fmt.Errorf("subdag config: invalid dag id")
	}
	return nil
}
