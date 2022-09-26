package interop

import (
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/public/service"
)

// Unwrap a public *service.ConfigField type into an internal docs.FieldSpec.
// This is useful in situations where a config spec needs to be shared by new
// components built by the service package at the same time as older components
// using the internal APIs directly.
//
// In these cases we want the canonical spec to be made with the service package
// but still extract a docs.FieldSpec from it.
func Unwrap(f *service.ConfigField) docs.FieldSpec {
	return f.XUnwrapper().(interface {
		Unwrap() docs.FieldSpec
	}).Unwrap()
}
