package execution

import "fmt"

// Status 执行状态（对应表 executions.status，编号与 DB 保持一致）
type Status int32

const (
	StatusPending     Status = 1 // 已登记，待投递
	StatusScheduled   Status = 2 // 已入队（保留给 engine 堆登记场景）
	StatusRunning     Status = 3 // worker ClaimPending 成功后的执行中
	StatusSuccess     Status = 4 // 终态
	StatusFailed      Status = 5 // 执行失败（worker 写），等待 planner 判定
	StatusRetrying    Status = 6 // planner 已重投，等待再次消费
	StatusFailedFinal Status = 7 // 终态：重试超限，进告警流程
)

func (s Status) Valid() bool { return s >= StatusPending && s <= StatusFailedFinal }
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "Pending"
	case StatusScheduled:
		return "Scheduled"
	case StatusRunning:
		return "Running"
	case StatusSuccess:
		return "Success"
	case StatusFailed:
		return "Failed"
	case StatusRetrying:
		return "Retrying"
	case StatusFailedFinal:
		return "FailedFinal"
	default:
		return "unknown"
	}
}

// IsTerminal 终态判定：planner 扫描、大盘统计都靠它
func (s Status) IsTerminal() bool { return s == StatusSuccess || s == StatusFailedFinal }

// Failed 可重试判定：planner 扫描条件的领域表达
func (s Status) Failed() bool { return s == StatusFailed }

// canTransition 合法迁移表——唯一的事实来源（Single Source of Truth）
var canTransition = map[Status]map[Status]bool{
	StatusPending:   {StatusRunning: true},
	StatusScheduled: {StatusRunning: true},
	StatusRunning:   {StatusSuccess: true, StatusFailed: true},
	StatusFailed:    {StatusRetrying: true, StatusFailedFinal: true}, // planner 专属
	StatusRetrying:  {StatusRunning: true},
	// 终态是黑洞：不可迁出
}

func CanTransition(from, to Status) bool { return canTransition[from][to] }

// Transition 实体内部用的迁移执行：非法迁移直接报错
func (s *Status) Transition(to Status) error {
	if !CanTransition(*s, to) {
		return fmt.Errorf("execution: illegal transition %d -> %d", *s, to)
	}
	*s = to
	return nil
}
