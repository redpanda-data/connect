package metrics

import "github.com/Jeffail/benthos/v3/internal/docs"

//------------------------------------------------------------------------------

func init() {
	Constructors[TypePrometheus] = TypeSpec{
		constructor: NewPrometheus,
		Summary: `
Host endpoints (` + "`/metrics` and `/stats`" + `) for Prometheus scraping.`,
		Description: `
Metrics paths will differ from [the list](/docs/components/metrics/about#paths) in that dot separators will
instead be underscores.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("prefix", "A string prefix to add to all metrics."),
			pathMappingDocs(true, true),
			docs.FieldAdvanced("push_url", "An optional [Push Gateway URL](#push-gateway) to push metrics to."),
			docs.FieldAdvanced("push_interval", "The period of time between each push when sending metrics to a Push Gateway."),
			docs.FieldAdvanced("push_job_name", "An identifier for push jobs."),
			docs.FieldAdvanced("push_basic_auth", "The Basic Authentication credentials.").WithChildren(
				docs.FieldCommon("username", "The Basic Authentication username."),
				docs.FieldCommon("password", "The Basic Authentication password."),
			),
		},
		Footnotes: `
## Push Gateway

The field ` + "`push_url`" + ` is optional and when set will trigger a push of
metrics to a [Prometheus Push Gateway](https://prometheus.io/docs/instrumenting/pushing/)
once Benthos shuts down. It is also possible to specify a
` + "`push_interval`" + ` which results in periodic pushes.

The Push Gateway is useful for when Benthos instances are short lived. Do not
include the "/metrics/jobs/..." path in the push URL.

If the Push Gateway requires HTTP Basic Authentication it can be configured with
` + "`push_basic_auth`.",
	}
}

//------------------------------------------------------------------------------

// PrometheusConfig is config for the Prometheus metrics type.
type PrometheusConfig struct {
	Prefix        string                        `json:"prefix" yaml:"prefix"`
	PathMapping   string                        `json:"path_mapping" yaml:"path_mapping"`
	PushURL       string                        `json:"push_url" yaml:"push_url"`
	PushBasicAuth PrometheusPushBasicAuthConfig `json:"push_basic_auth" yaml:"push_basic_auth"`
	PushInterval  string                        `json:"push_interval" yaml:"push_interval"`
	PushJobName   string                        `json:"push_job_name" yaml:"push_job_name"`
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
		Prefix:        "benthos",
		PathMapping:   "",
		PushURL:       "",
		PushBasicAuth: NewPrometheusPushBasicAuthConfig(),
		PushInterval:  "",
		PushJobName:   "benthos_push",
	}
}

//------------------------------------------------------------------------------
