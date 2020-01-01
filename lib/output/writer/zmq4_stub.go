// +build !ZMQ4

package writer

//------------------------------------------------------------------------------

// ZMQ4Config empty stub for when ZMQ4 is not compiled.
type ZMQ4Config struct{}

// NewZMQ4Config returns nil.
func NewZMQ4Config() *ZMQ4Config {
	return nil
}

//------------------------------------------------------------------------------
