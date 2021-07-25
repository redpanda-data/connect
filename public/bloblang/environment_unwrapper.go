package bloblang

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
)

type environmentUnwrapper struct {
	child parser.Context
}

func (e environmentUnwrapper) Unwrap() parser.Context {
	return e.child
}

// XUnwrapper is for internal use only, do not use this.
func (e *Environment) XUnwrapper() interface{} {
	return environmentUnwrapper{child: parser.Context{
		Functions: e.functions,
		Methods:   e.methods,
	}}
}
