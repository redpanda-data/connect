package output

import "github.com/Jeffail/benthos/v3/lib/types"

// Type is the standard interface of an output type.
type Type interface {
	types.Closable
	types.Consumer

	// Connected returns a boolean indicating whether this output is currently
	// connected to its target.
	Connected() bool
}
