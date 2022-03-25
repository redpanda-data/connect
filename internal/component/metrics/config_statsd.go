package metrics

// StatsdConfig is config for the Statsd metrics type.
type StatsdConfig struct {
	Address     string `json:"address" yaml:"address"`
	FlushPeriod string `json:"flush_period" yaml:"flush_period"`
	TagFormat   string `json:"tag_format" yaml:"tag_format"`
}

// NewStatsdConfig creates an StatsdConfig struct with default values.
func NewStatsdConfig() StatsdConfig {
	return StatsdConfig{
		Address:     "",
		FlushPeriod: "100ms",
		TagFormat:   "none",
	}
}
