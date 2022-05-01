package output

// SocketConfig contains configuration fields for the Socket output type.
type SocketConfig struct {
	Network string `json:"network" yaml:"network"`
	Address string `json:"address" yaml:"address"`
	Codec   string `json:"codec" yaml:"codec"`
}

// NewSocketConfig creates a new SocketConfig with default values.
func NewSocketConfig() SocketConfig {
	return SocketConfig{
		Network: "",
		Address: "",
		Codec:   "lines",
	}
}
