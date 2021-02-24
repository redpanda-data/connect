package output

// PulsarConfig contains configuration for the Pulsar input type.
type PulsarConfig struct {
	URL         string `json:"url" yaml:"url"`
	Topic       string `json:"topic" yaml:"topic"`
	MaxInFlight int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewPulsarConfig creates a new PulsarConfig with default values.
func NewPulsarConfig() PulsarConfig {
	return PulsarConfig{
		URL:         "",
		Topic:       "",
		MaxInFlight: 1,
	}
}
