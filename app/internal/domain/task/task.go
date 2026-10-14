package task

import (
	"encoding/json"
	"errors"
	"time"
)

type TaskType int

const (
	TaskTypeHTTP TaskType = iota + 1
	TaskTypeScript
	TaskTypeMessage
	TaskTypeSQL
	TaskTypeSubDAG
)

type TaskStatus int

// 任务状态
const (
	TaskStatusPending TaskStatus = iota + 1
	TaskStatusScheduled
	TaskStatusRunning
	TaskStatusSucceeded
	TaskStatusFailed //执行失败可以重试
	TaskStatusCanceled
)

type Task struct {
	id          int64       // 任务ID（雪花算法）
	name        string      // 任务名称
	taskType    TaskType    // 任务类型：1-HTTP 2-脚本 3-消息 4-SQL 5-子工作流
	config      TaskConfig  // 任务配置体
	priority    int32       // 优先级 1-10
	timeout     int32       // 超时时间（秒）
	retryPolicy RetryPolicy // 重试策略
	status      TaskStatus  // 状态：1-启用 2-禁用 3-删除
	description string      // 任务描述
	createdBy   int64       // 创建者用户ID
	createdAt   time.Time
	updatedAt   time.Time
	deletedAt   time.Time // 软删除时间
}

// New 创建一个待调度的执行任务，保证"出生时"状态合法
func New(id int64, name string, timeout int32,
	description string, tasktype int32, priority int32,
	config json.RawMessage, retrypolicy *RetryPolicy, createBy int64) (*Task, error) {
	if id <= 0 {
		return nil, errors.New("task: invalid task id")
	}
	// 0 = 未指定，取默认值；越界值显式拒绝（别让非法优先级落库）
	if priority < 0 || priority > 10 {
		return nil, ErrPriorityOutOfRange
	}
	task := &Task{
		id:          id,
		status:      TaskStatusPending,
		createdAt:   time.Now(),
		name:        name,
		timeout:     timeout,
		description: description,
		taskType:    TaskType(tasktype),
		priority:    priority,
		createdBy:   createBy,
	}
	cfg, err := NewTaskConfig(TaskType(tasktype), config)
	if err != nil {
		return nil, err
	}
	task.config = cfg
	if priority == 0 {
		task.priority = 5 // 文档默认值
	}
	if timeout == 0 {
		task.timeout = 300
	}
	if retrypolicy != nil {
		task.retryPolicy = *retrypolicy
	} else {
		// 未配置策略时用默认值：零值 RetryPolicy 序列化出来 strategy 非法，
		// 落库后回读会失败（见 RetryPolicy.UnmarshalJSON），出生即补齐
		task.retryPolicy = DefaultRetryPolicy()
	}
	return task, nil
}

// Rebuild 供 infra 层从持久化数据重建实体。不校验业务不变量（数据已在创建时校验过），
// 但要防 null/越界导致的 panic
func Rebuild(id int64, name string, cfg TaskConfig, policy RetryPolicy, priority int,
	timeout int32,
	status TaskStatus,
	description string,
	createdBy int64,
	createdAt time.Time,
	updatedAt time.Time,
	tasktype TaskType) *Task {
	return &Task{
		id:          id,
		name:        name,
		config:      cfg,
		retryPolicy: policy,
		taskType:    tasktype,
		priority:    int32(priority),
		timeout:     timeout,
		status:      status,
		description: description,
		createdAt:   createdAt,
		updatedAt:   updatedAt,
		createdBy:   createdBy,
	}
}

func (t *Task) Schedule() error {
	if t.status == TaskStatusPending || t.status == TaskStatusFailed {
		t.status = TaskStatusScheduled
		return nil
	}
	return ErrInvalidTransition
}

func (t *Task) Cancel() error {
	if t.status == TaskStatusSucceeded {
		return ErrInvalidTransition
	}
	t.status = TaskStatusCanceled
	return nil
}

func (t *Task) ID() int64                { return t.id }
func (t *Task) Name() string             { return t.name }
func (t *Task) Config() TaskConfig       { return t.config }
func (t *Task) RetryPolicy() RetryPolicy { return t.retryPolicy }
func (t *Task) Status() TaskStatus       { return t.status }
func (t *Task) Priority() int32          { return t.priority }
func (t *Task) Timeout() time.Duration   { return time.Duration(t.timeout) * time.Second }
func (t *Task) Type() TaskType           { return t.taskType }
func (t *Task) Description() string      { return t.description }
func (t *Task) CreatedBy() int64         { return t.createdBy }
func (t *Task) CreatedAt() time.Time     { return t.createdAt }
func (t *Task) UpdatedAt() time.Time     { return t.updatedAt }
