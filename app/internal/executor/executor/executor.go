package executor

import (
	"context"

	"job-scheduler/app/internal/domain/coordination"
	"job-scheduler/app/internal/domain/task"
)

// Executor 单个任务类型的执行器。输入是消息快照，输出是结果——无状态
type Executor interface {
	Execute(ctx context.Context, msg *coordination.ExecutionMessage) (result []byte, err error)
}

// Registry 按 task_type 分发。
type Registry map[int32]Executor

// NewRegistry 组装默认注册表：目前覆盖 HTTP/脚本/消息/SQL 四类，
// SubDAG 待 DAG 域装配好后再挂进来
func NewRegistry(publisher MessagePublisher) Registry {
	return Registry{
		int32(task.TaskTypeHTTP):    NewHTTPExecutor(),
		int32(task.TaskTypeScript):  NewScriptExecutor(),
		int32(task.TaskTypeMessage): NewMessageExecutor(publisher),
		int32(task.TaskTypeSQL):     NewSQLExecutor(),
	}
}

// WithScriptOutputCap 设置脚本执行器的输出上限（字节）。
// 0 = 不设限：脚本返回全量输出，由 handler 做三段式溢出（>阈值直传对象存储）。
// 配置了结果溢出存储时必须调本方法放开上限，否则脚本输出在执行器内
// 就被截成 4KB，溢出路径永远不触发
func (r Registry) WithScriptOutputCap(n int) Registry {
	if se, ok := r[int32(task.TaskTypeScript)].(*ScriptExecutor); ok {
		se.maxOutput = n
	}
	return r
}
