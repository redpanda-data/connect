package client

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	ihttpdocs "github.com/Jeffail/benthos/v3/internal/http/docs"
)

// FieldSpec returns the field spec for an HTTP type.
// TODO: V4 Remove this
func FieldSpec(extraChildren ...docs.FieldSpec) docs.FieldSpec {
	return ihttpdocs.ClientFieldSpec(false, extraChildren...)
}
