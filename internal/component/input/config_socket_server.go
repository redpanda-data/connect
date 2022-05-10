package input

// SocketServerConfig contains configuration for the SocketServer input type.
type SocketServerConfig struct {
	Network   string `json:"network" yaml:"network"`
	Address   string `json:"address" yaml:"address"`
	Codec     string `json:"codec" yaml:"codec"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
}

// NewSocketServerConfig creates a new SocketServerConfig with default values.
func NewSocketServerConfig() SocketServerConfig {
	return SocketServerConfig{
		Network:   "",
		Address:   "",
		Codec:     "lines",
		MaxBuffer: 1000000,
	}
}
