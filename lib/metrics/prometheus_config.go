// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

//------------------------------------------------------------------------------

func init() {
	Constructors[TypePrometheus] = TypeSpec{
		constructor: NewPrometheus,
		description: `
Host endpoints (` + "`/metrics` and `/stats`" + `) for Prometheus scraping.
Metrics paths will differ from [the list](paths.md) in that dot separators will
instead be underscores.

### Push Gateway

The field ` + "`push_url`" + ` is optional and when set will trigger a push of
metrics to a [Prometheus Push Gateway](https://prometheus.io/docs/instrumenting/pushing/)
once Benthos shuts down. It is also possible to specify a
` + "`push_interval`" + ` which results in periodic pushes.

The Push Gateway This is useful for when Benthos instances are short lived. Do
not include the "/metrics/jobs/..." path in the push URL.`,
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
