package condition

import (
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Type reads a message, calculates a condition and returns a boolean.
type Type interface {
	// Check tests a message against a configured condition.
	Check(msg types.Message) bool
}

//------------------------------------------------------------------------------
