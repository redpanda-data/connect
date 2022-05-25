package input

// GCPCloudStorageConfig contains configuration fields for the Google Cloud
// Storage input type.
type GCPCloudStorageConfig struct {
	Bucket        string `json:"bucket" yaml:"bucket"`
	Prefix        string `json:"prefix" yaml:"prefix"`
	Codec         string `json:"codec" yaml:"codec"`
	DeleteObjects bool   `json:"delete_objects" yaml:"delete_objects"`
}

// NewGCPCloudStorageConfig creates a new GCPCloudStorageConfig with default
// values.
func NewGCPCloudStorageConfig() GCPCloudStorageConfig {
	return GCPCloudStorageConfig{
		Codec: "all-bytes",
	}
}
