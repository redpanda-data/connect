package processor

// CacheConfig contains configuration fields for the Cache processor.
type CacheConfig struct {
	Resource string `json:"resource" yaml:"resource"`
	Operator string `json:"operator" yaml:"operator"`
	Key      string `json:"key" yaml:"key"`
	Value    string `json:"value" yaml:"value"`
	TTL      string `json:"ttl" yaml:"ttl"`
}

// NewCacheConfig returns a CacheConfig with default values.
func NewCacheConfig() CacheConfig {
	return CacheConfig{
		Resource: "",
		Operator: "",
		Key:      "",
		Value:    "",
		TTL:      "",
	}
}
