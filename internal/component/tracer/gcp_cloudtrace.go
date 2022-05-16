package tracer

// CloudTraceConfig is config for the Google Cloud Trace tracer.
type CloudTraceConfig struct {
	Project       string            `json:"project" yaml:"project"`
	SamplingRatio float64           `json:"sampling_ratio" yaml:"sampling_ratio"`
	Tags          map[string]string `json:"tags" yaml:"tags"`
	FlushInterval string            `json:"flush_interval" yaml:"flush_interval"`
}

// NewCloudTraceConfig creates an CloudTraceConfig struct with default values.
func NewCloudTraceConfig() CloudTraceConfig {
	return CloudTraceConfig{
		Project:       "",
		SamplingRatio: 1.0,
		Tags:          map[string]string{},
		FlushInterval: "",
	}
}
