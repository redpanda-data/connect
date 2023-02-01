package input

// SocketServerTLSConfig contains config for TLS.
type SocketServerTLSConfig struct {
	CertFile   string `json:"cert_file" yaml:"cert_file"`
	KeyFile    string `json:"key_file" yaml:"key_file"`
	SelfSigned bool   `json:"self_signed" yaml:"self_signed"`
}

// SocketServerConfig contains configuration for the SocketServer input type.
type SocketServerConfig struct {
	Network   string                `json:"network" yaml:"network"`
	Address   string                `json:"address" yaml:"address"`
	Codec     string                `json:"codec" yaml:"codec"`
	MaxBuffer int                   `json:"max_buffer" yaml:"max_buffer"`
	TLS       SocketServerTLSConfig `json:"tls" yaml:"tls"`
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
