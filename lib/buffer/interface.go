package buffer

import "github.com/Jeffail/benthos/v3/lib/types"

// Type is an interface implemented by all buffer types.
type Type interface {
	types.Producer
	types.Consumer
	types.Closable

	// StopConsuming instructs the buffer to cut off the producer it is
	// consuming from. It will then enter a mode whereby messages can only be
	// read, and when the buffer is empty it will shut down.
	StopConsuming()
}
