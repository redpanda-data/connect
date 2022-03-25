package tracer

// JaegerConfig is config for the Jaeger metrics type.
type JaegerConfig struct {
	AgentAddress  string            `json:"agent_address" yaml:"agent_address"`
	CollectorURL  string            `json:"collector_url" yaml:"collector_url"`
	SamplerType   string            `json:"sampler_type" yaml:"sampler_type"`
	SamplerParam  float64           `json:"sampler_param" yaml:"sampler_param"`
	Tags          map[string]string `json:"tags" yaml:"tags"`
	FlushInterval string            `json:"flush_interval" yaml:"flush_interval"`
}

// NewJaegerConfig creates an JaegerConfig struct with default values.
func NewJaegerConfig() JaegerConfig {
	return JaegerConfig{
		AgentAddress:  "",
		CollectorURL:  "",
		SamplerType:   "const",
		SamplerParam:  1.0,
		Tags:          map[string]string{},
		FlushInterval: "",
	}
}
