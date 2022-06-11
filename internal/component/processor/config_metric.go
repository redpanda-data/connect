package processor

// MetricConfig contains configuration fields for the Metric processor.
type MetricConfig struct {
	Type   string            `json:"type" yaml:"type"`
	Name   string            `json:"name" yaml:"name"`
	Labels map[string]string `json:"labels" yaml:"labels"`
	Value  string            `json:"value" yaml:"value"`
}

// NewMetricConfig returns a MetricConfig with default values.
func NewMetricConfig() MetricConfig {
	return MetricConfig{
		Type:   "",
		Name:   "",
		Labels: map[string]string{},
		Value:  "",
	}
}
