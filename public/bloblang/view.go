package bloblang

import (
	"encoding/json"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

// FunctionView describes a particular function belonging to a Bloblang
// environment.
type FunctionView struct {
	spec query.FunctionSpec
}

// Description provides an overview of the function.
func (v *FunctionView) Description() string {
	return v.spec.Description
}

// FormatJSON returns a byte slice of the function configuration formatted as a
// JSON object. The schema of this method is undocumented and is not intended
// for general use.
//
// Experimental: This method is not intended for general use and could have its
// signature and/or behaviour changed outside of major version bumps.
func (v *FunctionView) FormatJSON() ([]byte, error) {
	return json.Marshal(v.spec)
}

// MethodView describes a particular method belonging to a Bloblang environment.
type MethodView struct {
	spec query.MethodSpec
}

// Description provides an overview of the method.
func (v *MethodView) Description() string {
	return v.spec.Description
}

// FormatJSON returns a byte slice of the method configuration formatted as a
// JSON object. The schema of this method is undocumented and is not intended
// for general use.
//
// Experimental: This method is not intended for general use and could have its
// signature and/or behaviour changed outside of major version bumps.
func (v *MethodView) FormatJSON() ([]byte, error) {
	return json.Marshal(v.spec)
}
