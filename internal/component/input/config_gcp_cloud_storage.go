package input

// GCPGCSPubSubConfig contains configuration for hooking up the GCS input with a Pub/Sub subscription.
type GCPGCSPubSubConfig struct {
	Project                string `json:"project" yaml:"project"`
	Subscription           string `json:"subscription" yaml:"subscription"`
	MaxOutstandingMessages int    `json:"max_outstanding_messages" yaml:"max_outstanding_messages"`
	MaxOutstandingBytes    int    `json:"max_outstanding_bytes" yaml:"max_outstanding_bytes"`
	Sync                   bool   `json:"sync" yaml:"sync"`
}

// NewGCPGCSPubSubConfig creates a new GCPGCSPubSubConfig with default values.
func NewGCPGCSPubSubConfig() GCPGCSPubSubConfig {
	return GCPGCSPubSubConfig{
		Project:                "",
		Subscription:           "",
		MaxOutstandingMessages: 1000, // pubsub.DefaultReceiveSettings.MaxOutstandingMessages
		MaxOutstandingBytes:    1e9,  // pubsub.DefaultReceiveSettings.MaxOutstandingBytes (1G)
		Sync:                   false,
	}
}

// GCPCloudStorageConfig contains configuration fields for the Google Cloud
// Storage input type.
type GCPCloudStorageConfig struct {
	Bucket        string             `json:"bucket" yaml:"bucket"`
	Prefix        string             `json:"prefix" yaml:"prefix"`
	Codec         string             `json:"codec" yaml:"codec"`
	MaxBuffer     int                `json:"max_buffer" yaml:"max_buffer"`
	DeleteObjects bool               `json:"delete_objects" yaml:"delete_objects"`
	PubSub        GCPGCSPubSubConfig `json:"pubsub" yaml:"pubsub"`
}

// NewGCPCloudStorageConfig creates a new GCPCloudStorageConfig with default
// values.
func NewGCPCloudStorageConfig() GCPCloudStorageConfig {
	return GCPCloudStorageConfig{
		Codec:     "all-bytes",
		MaxBuffer: 1000000,
		PubSub:    NewGCPGCSPubSubConfig(),
	}
}
