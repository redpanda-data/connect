package types

import (
	"net/http"

	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/message"
)

// Manager is an interface expected by Benthos components that allows them to
// register their service wide behaviours such as HTTP endpoints and event
// listeners, and obtain service wide shared resources such as caches.
type Manager interface {
	// RegisterEndpoint registers a server wide HTTP endpoint.
	RegisterEndpoint(path, desc string, h http.HandlerFunc)

	// GetInput attempts to find a service wide input by its name.
	// TODO: V4 Add this
	// GetInput(name string) (Input, error)

	// GetCache attempts to find a service wide cache by its name.
	GetCache(name string) (cache.V1, error)

	// GetProcessor attempts to find a service wide processor by its name.
	// TODO: V4 Add this
	// GetProcessor(name string) (Processor, error)

	// GetRateLimit attempts to find a service wide rate limit by its name.
	GetRateLimit(name string) (ratelimit.V1, error)

	// GetOutput attempts to find a service wide output by its name.
	// TODO: V4 Add this
	// GetOutput(name string) (OutputWriter, error)

	// GetPipe attempts to find a service wide transaction chan by its name.
	GetPipe(name string) (<-chan message.Transaction, error)

	// SetPipe registers a transaction chan under a name.
	SetPipe(name string, t <-chan message.Transaction)

	// UnsetPipe removes a named transaction chan.
	UnsetPipe(name string, t <-chan message.Transaction)
}
