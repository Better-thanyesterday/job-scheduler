package task

import "errors"

var (
	// 实体相关
	ErrNotFound           = errors.New("task: not found")
	ErrNameRequired       = errors.New("task: name is required")
	ErrNameDuplicated     = errors.New("task: name already exists")
	ErrPriorityOutOfRange = errors.New("task: priority must be in [1,10]")
	ErrInvalidTimeout     = errors.New("task: timeout must be positive")

	// 状态机相关
	ErrInvalidTransition = errors.New("task: illegal status transition")
	ErrAlreadyDeleted    = errors.New("task: already deleted")
)
