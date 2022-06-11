package input

// SocketConfig contains configuration values for the Socket input type.
type SocketConfig struct {
	Network   string `json:"network" yaml:"network"`
	Address   string `json:"address" yaml:"address"`
	Codec     string `json:"codec" yaml:"codec"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
}

// NewSocketConfig creates a new SocketConfig with default values.
func NewSocketConfig() SocketConfig {
	return SocketConfig{
		Network:   "",
		Address:   "",
		Codec:     "lines",
		MaxBuffer: 1000000,
	}
}
