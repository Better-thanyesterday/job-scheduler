package execution

import "errors"

var (
	ErrNotFound       = errors.New("execution: not found")
	ErrAlreadyClaimed = errors.New("execution: already claimed")
	ErrWrongStatus    = errors.New("execution: illegal status transition")
	ErrRetryExhausted = errors.New("execution: retry exhausted")
)
