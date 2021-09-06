package bloblang

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang"
)

type environmentUnwrapper struct {
	child *bloblang.Environment
}

func (e environmentUnwrapper) Unwrap() *bloblang.Environment {
	return e.child
}

// XUnwrapper is for internal use only, do not use this.
func (e *Environment) XUnwrapper() interface{} {
	return environmentUnwrapper{child: e.env}
}

// XWrapEnvironment is for internal use only, do not use this.
func XWrapEnvironment(v interface{}) *Environment {
	if bEnv, ok := v.(*bloblang.Environment); ok {
		return &Environment{env: bEnv}
	}
	return NewEnvironment()
}
