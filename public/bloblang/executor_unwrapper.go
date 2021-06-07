package bloblang

import "github.com/Jeffail/benthos/v3/internal/bloblang/mapping"

type executorUnwrapper struct {
	child *mapping.Executor
}

func (e executorUnwrapper) Unwrap() *mapping.Executor {
	return e.child
}

// XExecutorUnwrapper is for internal use only, do not use this.
func XExecutorUnwrapper(e *Executor) interface{} {
	return executorUnwrapper{child: e.exec}
}
