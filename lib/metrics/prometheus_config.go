package metrics

//------------------------------------------------------------------------------

func init() {
	Constructors[TypePrometheus] = TypeSpec{
		constructor: NewPrometheus,
		Description: `
Host endpoints (` + "`/metrics` and `/stats`" + `) for Prometheus scraping.
Metrics paths will differ from [the list](/docs/components/metrics/about#paths) in that dot separators will
instead be underscores.

### Push Gateway

The field ` + "`push_url`" + ` is optional and when set will trigger a push of
metrics to a [Prometheus Push Gateway](https://prometheus.io/docs/instrumenting/pushing/)
once Benthos shuts down. It is also possible to specify a
` + "`push_interval`" + ` which results in periodic pushes.

The Push Gateway is useful for when Benthos instances are short lived. Do not
include the "/metrics/jobs/..." path in the push URL.`,
	}
}

//------------------------------------------------------------------------------

// PrometheusConfig is config for the Prometheus metrics type.
type PrometheusConfig struct {
	Prefix       string `json:"prefix" yaml:"prefix"`
	PushURL      string `json:"push_url" yaml:"push_url"`
	PushInterval string `json:"push_interval" yaml:"push_interval"`
	PushJobName  string `json:"push_job_name" yaml:"push_job_name"`
}

// NewPrometheusConfig creates an PrometheusConfig struct with default values.
func NewPrometheusConfig() PrometheusConfig {
	return PrometheusConfig{
		Prefix:       "benthos",
		PushURL:      "",
		PushInterval: "",
		PushJobName:  "benthos_push",
	}
}

//------------------------------------------------------------------------------
