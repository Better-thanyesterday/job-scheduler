package schedule

type Status int32

const (
	StatusRunning   Status = iota + 1 // 运行中 -->对应task的取消
	StatusPaused                      // 暂停
	StatusExpired                     // 过期 -->一般为调度器重启导致
	StatusDeleted                     // 删除 -->对应task的取消
	StatusCompleted                   // 完成 -->对应task的成功
)

func (s Status) Valid() bool {
	return s >= StatusRunning && s <= StatusDeleted
}

func (s Status) String() string {
	switch s {
	case StatusRunning:
		return "Running"
	case StatusExpired:
		return "Expired"
	case StatusDeleted:
		return "Deleted"
	case StatusPaused:
		return "Paused"
	default:
		return "unknown"
	}
}
func (s Status) Active() bool { return s == StatusRunning }

// ScheduleType 触发方式（1-Cron 2-手动 3-API 触发）
type ScheduleType int32

const (
	TypeCron ScheduleType = iota + 1
	TypeManual
	TypeAPI
)

type TriggerSource int32

const (
	Cron TriggerSource = iota + 1
	Manual
	Misfire
)

func (m ScheduleType) Valid() bool { return m >= TypeCron && m <= TypeAPI }

type MisfirePolicy int32

const (
	MisfireFireAll MisfirePolicy = iota + 1 // 立即补发：逐个入队所有错过的时间点
	MisfireSkip                             // 跳过：忽略错过的执行
	MisfireMerge                            // 合并：所有错过点合并为一次执行
)

func (m MisfirePolicy) Valid() bool { return m >= MisfireFireAll && m <= MisfireMerge }
