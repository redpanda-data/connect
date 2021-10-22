package interop

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// NewBloblangMapping parses a bloblang mapping using either the global
// environment, or, when the provided manager supports the newer
// BloblEnvironment method, the manager environment.
func NewBloblangMapping(mgr types.Manager, blobl string) (*mapping.Executor, error) {
	if nm, ok := mgr.(interface {
		BloblEnvironment() *bloblang.Environment
	}); ok {
		return nm.BloblEnvironment().NewMapping(blobl)
	}
	return bloblang.GlobalEnvironment().NewMapping(blobl)
}

// NewBloblangField parses a bloblang interpolated field using either the global
// environment, or, when the provided manager supports the newer
// BloblEnvironment method, the manager environment.
func NewBloblangField(mgr types.Manager, expr string) (*field.Expression, error) {
	if nm, ok := mgr.(interface {
		BloblEnvironment() *bloblang.Environment
	}); ok {
		return nm.BloblEnvironment().NewField(expr)
	}
	return bloblang.GlobalEnvironment().NewField(expr)
}
