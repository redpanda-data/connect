package input

// GCPPubSubConfig contains configuration values for the input type.
type GCPPubSubConfig struct {
	ProjectID              string `json:"project" yaml:"project"`
	SubscriptionID         string `json:"subscription" yaml:"subscription"`
	TopicID                string `json:"topic" yaml:"topic"`
	MaxOutstandingMessages int    `json:"max_outstanding_messages" yaml:"max_outstanding_messages"`
	MaxOutstandingBytes    int    `json:"max_outstanding_bytes" yaml:"max_outstanding_bytes"`
	Sync                   bool   `json:"sync" yaml:"sync"`
	CreateSubscription     bool   `json:"create_subscription" yaml:"create_subscription"`
}

// NewGCPPubSubConfig creates a new Config with default values.
func NewGCPPubSubConfig() GCPPubSubConfig {
	return GCPPubSubConfig{
		ProjectID:              "",
		SubscriptionID:         "",
		TopicID:                "",
		MaxOutstandingMessages: 1000, // pubsub.DefaultReceiveSettings.MaxOutstandingMessages
		MaxOutstandingBytes:    1e9,  // pubsub.DefaultReceiveSettings.MaxOutstandingBytes (1G)
		Sync:                   false,
		CreateSubscription:     false,
	}
}
