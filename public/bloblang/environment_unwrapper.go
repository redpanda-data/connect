package bloblang

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang"
)

type environmentUnwrapper struct {
	child *bloblang.Environment
}

func (e environmentUnwrapper) Unwrap() *bloblang.Environment {
	return e.child
}

// XUnwrapper is for internal use only, do not use this.
func (e *Environment) XUnwrapper() any {
	return environmentUnwrapper{child: e.env}
}

// XWrapEnvironment is for internal use only, do not use this.
func XWrapEnvironment(v *bloblang.Environment) *Environment {
	return &Environment{env: v}
}
