package output

// CacheConfig contains configuration fields for the Cache output type.
type CacheConfig struct {
	Target      string `json:"target" yaml:"target"`
	Key         string `json:"key" yaml:"key"`
	TTL         string `json:"ttl" yaml:"ttl"`
	MaxInFlight int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewCacheConfig creates a new Config with default values.
func NewCacheConfig() CacheConfig {
	return CacheConfig{
		Target:      "",
		Key:         `${!count("items")}-${!timestamp_unix_nano()}`,
		MaxInFlight: 64,
	}
}
