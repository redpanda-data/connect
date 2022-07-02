package metrics

// PrometheusConfig is config for the Prometheus metrics type.
type PrometheusConfig struct {
	UseHistogramTiming bool                          `json:"use_histogram_timing" yaml:"use_histogram_timing"`
	HistogramBuckets   []float64                     `json:"histogram_buckets" yaml:"histogram_buckets"`
	AddProcessMetrics  bool                          `json:"add_process_metrics" yaml:"add_process_metrics"`
	AddGoMetrics       bool                          `json:"add_go_metrics" yaml:"add_go_metrics"`
	PushURL            string                        `json:"push_url" yaml:"push_url"`
	PushBasicAuth      PrometheusPushBasicAuthConfig `json:"push_basic_auth" yaml:"push_basic_auth"`
	PushInterval       string                        `json:"push_interval" yaml:"push_interval"`
	PushJobName        string                        `json:"push_job_name" yaml:"push_job_name"`
	FileOutputPath     string                        `json:"file_output_path" yaml:"file_output_path"`
}

// PrometheusPushBasicAuthConfig contains parameters for establishing basic
// authentication against a push service.
type PrometheusPushBasicAuthConfig struct {
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// NewPrometheusPushBasicAuthConfig creates a new NewPrometheusPushBasicAuthConfig with default values.
func NewPrometheusPushBasicAuthConfig() PrometheusPushBasicAuthConfig {
	return PrometheusPushBasicAuthConfig{
		Username: "",
		Password: "",
	}
}

// NewPrometheusConfig creates an PrometheusConfig struct with default values.
func NewPrometheusConfig() PrometheusConfig {
	return PrometheusConfig{
		UseHistogramTiming: false,
		HistogramBuckets:   []float64{},
		PushURL:            "",
		PushBasicAuth:      NewPrometheusPushBasicAuthConfig(),
		PushInterval:       "",
		PushJobName:        "benthos_push",
		FileOutputPath:     "",
	}
}
