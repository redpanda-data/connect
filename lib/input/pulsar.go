package input

// PulsarConfig contains configuration for the Pulsar input type.
type PulsarConfig struct {
	URL              string   `json:"url" yaml:"url"`
	Topics           []string `json:"topics" yaml:"topics"`
	SubscriptionName string   `json:"subscription_name" yaml:"subscription_name"`
}

// NewPulsarConfig creates a new PulsarConfig with default values.
func NewPulsarConfig() PulsarConfig {
	return PulsarConfig{
		URL:              "",
		Topics:           []string{},
		SubscriptionName: "",
	}
}
