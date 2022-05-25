package input

// STDINConfig contains config fields for the STDIN input type.
type STDINConfig struct {
	Codec     string `json:"codec" yaml:"codec"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
}

// NewSTDINConfig creates a STDINConfig populated with default values.
func NewSTDINConfig() STDINConfig {
	return STDINConfig{
		Codec:     "lines",
		MaxBuffer: 1000000,
	}
}
