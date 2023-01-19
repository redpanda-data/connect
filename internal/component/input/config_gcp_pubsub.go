package input

// GCPPubSubConfig contains configuration values for the input type.
type GCPPubSubConfig struct {
	ProjectID              string                      `json:"project" yaml:"project"`
	SubscriptionID         string                      `json:"subscription" yaml:"subscription"`
	Endpoint               string                      `json:"endpoint" yaml:"endpoint"`
	MaxOutstandingMessages int                         `json:"max_outstanding_messages" yaml:"max_outstanding_messages"`
	MaxOutstandingBytes    int                         `json:"max_outstanding_bytes" yaml:"max_outstanding_bytes"`
	Sync                   bool                        `json:"sync" yaml:"sync"`
	CreateSubscription     GCPPubSubSubscriptionConfig `json:"create_subscription" yaml:"create_subscription"`
}

// GCPPubSubSubscriptionConfig contains config values for subscription creation.
type GCPPubSubSubscriptionConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	TopicID string `json:"topic" yaml:"topic"`
}

// NewGCPPubSubConfig creates a new Config with default values.
func NewGCPPubSubConfig() GCPPubSubConfig {
	return GCPPubSubConfig{
		ProjectID:              "",
		SubscriptionID:         "",
		MaxOutstandingMessages: 1000, // pubsub.DefaultReceiveSettings.MaxOutstandingMessages
		MaxOutstandingBytes:    1e9,  // pubsub.DefaultReceiveSettings.MaxOutstandingBytes (1G)
		Sync:                   false,
		CreateSubscription: GCPPubSubSubscriptionConfig{
			Enabled: false,
			TopicID: "",
		},
	}
}
