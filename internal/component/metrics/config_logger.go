package metrics

// LoggerConfig contains configuration parameters for the Stdout metrics
// aggregator.
type LoggerConfig struct {
	PushInterval string `json:"push_interval" yaml:"push_interval"`
	FlushMetrics bool   `json:"flush_metrics" yaml:"flush_metrics"`
}

// NewLoggerConfig returns a new StdoutConfig with default values.
func NewLoggerConfig() LoggerConfig {
	return LoggerConfig{
		PushInterval: "",
		FlushMetrics: false,
	}
}
