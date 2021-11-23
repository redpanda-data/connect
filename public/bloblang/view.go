package bloblang

import "github.com/Jeffail/benthos/v3/internal/bloblang/query"

// FunctionView describes a particular function belonging to a Bloblang
// environment.
type FunctionView struct {
	spec query.FunctionSpec
}

// Description provides an overview of the function.
func (v *FunctionView) Description() string {
	return v.spec.Description
}

// MethodView describes a particular method belonging to a Bloblang environment.
type MethodView struct {
	spec query.MethodSpec
}

// Description provides an overview of the method.
func (v *MethodView) Description() string {
	return v.spec.Description
}
