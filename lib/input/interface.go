package input

import (
	"github.com/Jeffail/benthos/v3/lib/types"
)

// Type is the standard interface of an input type.
type Type interface {
	types.Closable
	types.Producer

	// Connected returns a boolean indicating whether this input is currently
	// connected to its target.
	Connected() bool
}
