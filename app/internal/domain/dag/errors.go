package dag

import "errors"

var (
	ErrNotFound          = errors.New("dag: not found")
	ErrWrongStatus       = errors.New("dag: illegal status transition")
	ErrInvalidDefinition = errors.New("dag: invalid definition")
	ErrCycle             = errors.New("dag: dependency cycle")
)
