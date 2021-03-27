package manager

import (
	"errors"
	"net/http"
	"path"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// TODO: V4 Delete this

// NamespacedManager is a types.Manager implementation that wraps an underlying
// implementation with a namespace that prefixes registered endpoints, etc.
type NamespacedManager struct {
	ns  string
	mgr types.Manager
}

// RegisterEndpoint registers a server wide HTTP endpoint.
func (n *NamespacedManager) RegisterEndpoint(p, desc string, h http.HandlerFunc) {
	n.mgr.RegisterEndpoint(path.Join(n.ns, p), desc, h)
}

// GetOutput attempts to find a service wide output by its name.
func (n *NamespacedManager) GetOutput(name string) (types.OutputWriter, error) {
	// TODO: V4 Simplify this.
	if outProv, ok := n.mgr.(interface {
		GetOutput(name string) (types.OutputWriter, error)
	}); ok {
		return outProv.GetOutput(name)
	}
	return nil, errors.New("wrapped manager does not support output resources")
}

// GetInput attempts to find a service wide input by its name.
func (n *NamespacedManager) GetInput(name string) (types.Input, error) {
	// TODO: V4 Simplify this.
	if inProv, ok := n.mgr.(interface {
		GetInput(name string) (types.Input, error)
	}); ok {
		return inProv.GetInput(name)
	}
	return nil, errors.New("wrapped manager does not support input resources")
}

// GetCache attempts to find a service wide cache by its name.
func (n *NamespacedManager) GetCache(name string) (types.Cache, error) {
	return n.mgr.GetCache(name)
}

// GetCondition attempts to find a service wide condition by its name.
func (n *NamespacedManager) GetCondition(name string) (types.Condition, error) {
	return n.mgr.GetCondition(name)
}

// GetProcessor attempts to find a service wide processor by its name.
func (n *NamespacedManager) GetProcessor(name string) (types.Processor, error) {
	// TODO: V4 Simplify this.
	if procProv, ok := n.mgr.(interface {
		GetProcessor(name string) (types.Processor, error)
	}); ok {
		return procProv.GetProcessor(name)
	}
	return nil, errors.New("wrapped manager does not support processor resources")
}

// GetRateLimit attempts to find a service wide rate limit by its name.
func (n *NamespacedManager) GetRateLimit(name string) (types.RateLimit, error) {
	return n.mgr.GetRateLimit(name)
}

// GetPlugin attempts to find a service wide resource plugin by its name.
func (n *NamespacedManager) GetPlugin(name string) (interface{}, error) {
	return n.mgr.GetPlugin(name)
}

// GetPipe returns a named pipe transaction channel.
func (n *NamespacedManager) GetPipe(name string) (<-chan types.Transaction, error) {
	// Pipes are always absolute.
	return n.mgr.GetPipe(name)
}

// SetPipe sets a named pipe.
func (n *NamespacedManager) SetPipe(name string, t <-chan types.Transaction) {
	// Pipes are always absolute.
	n.mgr.SetPipe(name, t)
}

// UnsetPipe unsets a named pipe.
func (n *NamespacedManager) UnsetPipe(name string, t <-chan types.Transaction) {
	// Pipes are always absolute.
	n.mgr.UnsetPipe(name, t)
}

// GetUnderlying returns the underlying types.Manager implementation.
func (n *NamespacedManager) GetUnderlying() types.Manager {
	return n.mgr
}

//------------------------------------------------------------------------------
