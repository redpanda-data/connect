package docs_test

import (
	"errors"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/stretchr/testify/require"
)

func BenchmarkFields(b *testing.B) {
	b.Run("with_options", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			metricsBundle := &bundle.MetricsSet{}

			require.NoError(b, metricsBundle.Add(func(conf metrics.Config, nm bundle.NewManagement) (metrics.Type, error) {
				return nil, errors.New("not implemented")
			}, docs.ComponentSpec{
				Name:   "statsd",
				Type:   docs.TypeMetrics,
				Status: docs.StatusStable,
				Summary: `
Pushes metrics using the [StatsD protocol](https://github.com/statsd/statsd).
Supported tagging formats are 'none', 'datadog' and 'influxdb'.`,
				Description: `
The underlying client library has recently been updated in order to support
tagging.`,
				Config: docs.FieldComponent().WithChildren(
					docs.FieldString("address", "The address to send metrics to.").HasDefault(""),
					docs.FieldString("flush_period", "The time interval between metrics flushes.").HasDefault("100ms"),
					docs.FieldString("tag_format", "Metrics tagging is supported in a variety of formats.").HasOptions(
						"none", "datadog", "influxdb",
					).HasDefault("none"),
				),
			}))
		}
	})

	b.Run("without_options", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			metricsBundle := &bundle.MetricsSet{}

			require.NoError(b, metricsBundle.Add(func(conf metrics.Config, nm bundle.NewManagement) (metrics.Type, error) {
				return nil, errors.New("not implemented")
			}, docs.ComponentSpec{
				Name:   "statsd",
				Type:   docs.TypeMetrics,
				Status: docs.StatusStable,
				Summary: `
Pushes metrics using the [StatsD protocol](https://github.com/statsd/statsd).
Supported tagging formats are 'none', 'datadog' and 'influxdb'.`,
				Description: `
The underlying client library has recently been updated in order to support
tagging.`,
				Config: docs.FieldComponent().WithChildren(
					docs.FieldString("address", "The address to send metrics to.").HasDefault(""),
					docs.FieldString("flush_period", "The time interval between metrics flushes.").HasDefault("100ms"),
					docs.FieldString("tag_format", "Metrics tagging is supported in a variety of formats.").HasDefault("none"),
				),
			}))
		}
	})
}
