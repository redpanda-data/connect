package bloblang

import "github.com/Jeffail/benthos/v3/internal/bloblang/mapping"

type executorUnwrapper struct {
	child *mapping.Executor
}

func (e executorUnwrapper) Unwrap() *mapping.Executor {
	return e.child
}

// XUnwrapper is for internal use only, do not use this.
func (e *Executor) XUnwrapper() interface{} {
	return executorUnwrapper{child: e.exec}
}
