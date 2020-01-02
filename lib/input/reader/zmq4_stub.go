// +build !ZMQ4

package reader

//------------------------------------------------------------------------------

// ZMQ4Config is an empty stub for when ZMQ4 is not compiled.
type ZMQ4Config struct{}

// NewZMQ4Config returns nil.
func NewZMQ4Config() *ZMQ4Config {
	return nil
}

//------------------------------------------------------------------------------
