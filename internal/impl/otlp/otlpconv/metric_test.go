// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlpconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
)

func createTestMetrics() pmetricotlp.ExportRequest {
	metrics := pmetric.NewMetrics()

	// Resource 1
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl("https://opentelemetry.io/schemas/1.21.0")
	resource := rm.Resource()
	resource.Attributes().PutStr("service.name", "metric-service")

	// Scope 1
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.SetSchemaUrl("https://opentelemetry.io/schemas/1.21.0")
	scope := sm.Scope()
	scope.SetName("test-meter")
	scope.SetVersion("v1.0.0")

	// Gauge metric
	gaugeMetric := sm.Metrics().AppendEmpty()
	gaugeMetric.SetName("test.gauge")
	gaugeMetric.SetDescription("Test gauge metric")
	gaugeMetric.SetUnit("1")
	gauge := gaugeMetric.SetEmptyGauge()
	dp1 := gauge.DataPoints().AppendEmpty()
	dp1.SetTimestamp(pcommon.Timestamp(1609459200000000000))
	dp1.SetIntValue(42)
	dp1.Attributes().PutStr("key", "value")

	// Sum metric
	sumMetric := sm.Metrics().AppendEmpty()
	sumMetric.SetName("test.sum")
	sumMetric.SetDescription("Test sum metric")
	sumMetric.SetUnit("bytes")
	sum := sumMetric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)
	dp2 := sum.DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.Timestamp(1609459201000000000))
	dp2.SetDoubleValue(123.45)

	// Histogram metric
	histMetric := sm.Metrics().AppendEmpty()
	histMetric.SetName("test.histogram")
	histogram := histMetric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dpHist := histogram.DataPoints().AppendEmpty()
	dpHist.SetTimestamp(pcommon.Timestamp(1609459202000000000))
	dpHist.SetCount(100)
	dpHist.SetSum(500.0)
	dpHist.ExplicitBounds().FromRaw([]float64{0, 10, 20, 30})
	dpHist.BucketCounts().FromRaw([]uint64{10, 20, 30, 40})

	return pmetricotlp.NewExportRequestFromMetrics(metrics)
}

func TestMetricsRoundtrip(t *testing.T) {
	// Create original request
	original := createTestMetrics()

	// Convert to Redpanda
	redpandaMetrics := MetricsToRedpanda(original)
	require.Len(t, redpandaMetrics, 3)

	// Verify metric types
	assert.Equal(t, "test.gauge", redpandaMetrics[0].Name)
	assert.NotNil(t, redpandaMetrics[0].GetGauge())

	assert.Equal(t, "test.sum", redpandaMetrics[1].Name)
	assert.NotNil(t, redpandaMetrics[1].GetSum())

	assert.Equal(t, "test.histogram", redpandaMetrics[2].Name)
	assert.NotNil(t, redpandaMetrics[2].GetHistogram())

	// Convert back to OTLP
	reconstructed := MetricsFromRedpanda(redpandaMetrics)

	// Verify structure
	reconstructedMetrics := reconstructed.Metrics()
	assert.Equal(t, 1, reconstructedMetrics.ResourceMetrics().Len())

	rm := reconstructedMetrics.ResourceMetrics().At(0)
	v, ok := rm.Resource().Attributes().Get("service.name")
	assert.True(t, ok)
	assert.Equal(t, "metric-service", v.Str())
	assert.Equal(t, 1, rm.ScopeMetrics().Len())

	sm := rm.ScopeMetrics().At(0)
	assert.Equal(t, "test-meter", sm.Scope().Name())
	assert.Equal(t, 3, sm.Metrics().Len())

	// Verify metrics
	recGauge := sm.Metrics().At(0)
	assert.Equal(t, "test.gauge", recGauge.Name())
	assert.Equal(t, pmetric.MetricTypeGauge, recGauge.Type())

	recSum := sm.Metrics().At(1)
	assert.Equal(t, "test.sum", recSum.Name())
	assert.Equal(t, pmetric.MetricTypeSum, recSum.Type())
	assert.True(t, recSum.Sum().IsMonotonic())

	recHist := sm.Metrics().At(2)
	assert.Equal(t, "test.histogram", recHist.Name())
	assert.Equal(t, pmetric.MetricTypeHistogram, recHist.Type())
}

func TestGaugeMetric(t *testing.T) {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	metric := sm.Metrics().AppendEmpty()
	metric.SetName("gauge.metric")
	metric.SetDescription("Gauge description")
	metric.SetUnit("ms")

	gauge := metric.SetEmptyGauge()
	dp := gauge.DataPoints().AppendEmpty()
	dp.SetStartTimestamp(pcommon.Timestamp(1000000000))
	dp.SetTimestamp(pcommon.Timestamp(2000000000))
	dp.SetDoubleValue(98.6)
	dp.Attributes().PutStr("attr", "value")

	req := pmetricotlp.NewExportRequestFromMetrics(metrics)

	// Roundtrip
	redpandaMetrics := MetricsToRedpanda(req)
	require.Len(t, redpandaMetrics, 1)

	pbMetric := &redpandaMetrics[0]
	assert.Equal(t, "gauge.metric", pbMetric.Name)
	assert.Equal(t, "Gauge description", pbMetric.Description)
	assert.Equal(t, "ms", pbMetric.Unit)
	assert.NotNil(t, pbMetric.GetGauge())

	// Convert back
	reconstructed := MetricsFromRedpanda(redpandaMetrics)

	recMetric := reconstructed.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	assert.Equal(t, "gauge.metric", recMetric.Name())
	assert.Equal(t, pmetric.MetricTypeGauge, recMetric.Type())
	assert.Equal(t, 1, recMetric.Gauge().DataPoints().Len())
}

func TestSumMetric(t *testing.T) {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	metric := sm.Metrics().AppendEmpty()
	metric.SetName("sum.metric")

	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	sum.SetIsMonotonic(true)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetIntValue(1000)

	req := pmetricotlp.NewExportRequestFromMetrics(metrics)

	// Roundtrip
	redpandaMetrics := MetricsToRedpanda(req)

	pbSum := redpandaMetrics[0].GetSum()
	require.NotNil(t, pbSum)
	assert.Equal(t, pb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA, pbSum.AggregationTemporality)
	assert.True(t, pbSum.IsMonotonic)

	// Convert back
	reconstructed := MetricsFromRedpanda(redpandaMetrics)

	recSum := reconstructed.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum()
	assert.Equal(t, pmetric.AggregationTemporalityDelta, recSum.AggregationTemporality())
	assert.True(t, recSum.IsMonotonic())
}

func TestHistogramMetric(t *testing.T) {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	metric := sm.Metrics().AppendEmpty()
	metric.SetName("histogram.metric")

	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := histogram.DataPoints().AppendEmpty()
	dp.SetCount(500)
	dp.SetSum(1234.56)
	dp.SetMin(1.0)
	dp.SetMax(100.0)
	dp.ExplicitBounds().FromRaw([]float64{10.0, 20.0, 50.0, 100.0})
	dp.BucketCounts().FromRaw([]uint64{50, 100, 200, 100, 50})

	req := pmetricotlp.NewExportRequestFromMetrics(metrics)

	// Roundtrip
	redpandaMetrics := MetricsToRedpanda(req)

	pbHist := redpandaMetrics[0].GetHistogram()
	require.NotNil(t, pbHist)
	require.Len(t, pbHist.DataPoints, 1)

	pbDp := pbHist.DataPoints[0]
	assert.Equal(t, uint64(500), pbDp.Count)
	assert.NotNil(t, pbDp.Sum)
	assert.Equal(t, 1234.56, *pbDp.Sum)
	assert.NotNil(t, pbDp.Min)
	assert.Equal(t, 1.0, *pbDp.Min)
	assert.NotNil(t, pbDp.Max)
	assert.Equal(t, 100.0, *pbDp.Max)
	assert.Equal(t, []float64{10.0, 20.0, 50.0, 100.0}, pbDp.ExplicitBounds)
	assert.Equal(t, []uint64{50, 100, 200, 100, 50}, pbDp.BucketCounts)

	// Convert back
	reconstructed := MetricsFromRedpanda(redpandaMetrics)

	recHist := reconstructed.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Histogram()
	assert.Equal(t, pmetric.AggregationTemporalityCumulative, recHist.AggregationTemporality())
	recDp := recHist.DataPoints().At(0)
	assert.Equal(t, uint64(500), recDp.Count())
	assert.True(t, recDp.HasSum())
	assert.Equal(t, 1234.56, recDp.Sum())
}

func TestExponentialHistogramMetric(t *testing.T) {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	metric := sm.Metrics().AppendEmpty()
	metric.SetName("exp.histogram")

	expHist := metric.SetEmptyExponentialHistogram()
	expHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	dp := expHist.DataPoints().AppendEmpty()
	dp.SetCount(100)
	dp.SetSum(500.0)
	dp.SetScale(5)
	dp.SetZeroCount(10)
	dp.SetZeroThreshold(0.001)

	dp.Positive().SetOffset(2)
	dp.Positive().BucketCounts().FromRaw([]uint64{5, 10, 15, 20})

	dp.Negative().SetOffset(-2)
	dp.Negative().BucketCounts().FromRaw([]uint64{3, 7, 10})

	req := pmetricotlp.NewExportRequestFromMetrics(metrics)

	// Roundtrip
	redpandaMetrics := MetricsToRedpanda(req)

	pbExpHist := redpandaMetrics[0].GetExponentialHistogram()
	require.NotNil(t, pbExpHist)
	require.Len(t, pbExpHist.DataPoints, 1)

	pbDp := pbExpHist.DataPoints[0]
	assert.Equal(t, int32(5), pbDp.Scale)
	assert.Equal(t, uint64(10), pbDp.ZeroCount)
	assert.Equal(t, 0.001, pbDp.ZeroThreshold)
	assert.NotNil(t, pbDp.Positive)
	assert.Equal(t, int32(2), pbDp.Positive.Offset)
	assert.Equal(t, []uint64{5, 10, 15, 20}, pbDp.Positive.BucketCounts)

	// Convert back
	reconstructed := MetricsFromRedpanda(redpandaMetrics)

	recExpHist := reconstructed.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).ExponentialHistogram()
	recDp := recExpHist.DataPoints().At(0)
	assert.Equal(t, int32(5), recDp.Scale())
	assert.Equal(t, uint64(10), recDp.ZeroCount())
	assert.Equal(t, int32(2), recDp.Positive().Offset())
}

func TestSummaryMetric(t *testing.T) {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	metric := sm.Metrics().AppendEmpty()
	metric.SetName("summary.metric")

	summary := metric.SetEmptySummary()
	dp := summary.DataPoints().AppendEmpty()
	dp.SetCount(100)
	dp.SetSum(5000.0)

	// Add quantiles
	qv1 := dp.QuantileValues().AppendEmpty()
	qv1.SetQuantile(0.5)
	qv1.SetValue(45.0)

	qv2 := dp.QuantileValues().AppendEmpty()
	qv2.SetQuantile(0.95)
	qv2.SetValue(95.0)

	qv3 := dp.QuantileValues().AppendEmpty()
	qv3.SetQuantile(0.99)
	qv3.SetValue(99.0)

	req := pmetricotlp.NewExportRequestFromMetrics(metrics)

	// Roundtrip
	redpandaMetrics := MetricsToRedpanda(req)

	pbSummary := redpandaMetrics[0].GetSummary()
	require.NotNil(t, pbSummary)
	require.Len(t, pbSummary.DataPoints, 1)

	pbDp := pbSummary.DataPoints[0]
	assert.Equal(t, uint64(100), pbDp.Count)
	assert.Equal(t, 5000.0, pbDp.Sum)
	require.Len(t, pbDp.QuantileValues, 3)
	assert.Equal(t, 0.5, pbDp.QuantileValues[0].Quantile)
	assert.Equal(t, 45.0, pbDp.QuantileValues[0].Value)

	// Convert back
	reconstructed := MetricsFromRedpanda(redpandaMetrics)

	recSummary := reconstructed.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Summary()
	recDp := recSummary.DataPoints().At(0)
	assert.Equal(t, uint64(100), recDp.Count())
	assert.Equal(t, 5000.0, recDp.Sum())
	assert.Equal(t, 3, recDp.QuantileValues().Len())
}

func TestMetricWithExemplars(t *testing.T) {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	metric := sm.Metrics().AppendEmpty()
	metric.SetName("metric.with.exemplars")

	gauge := metric.SetEmptyGauge()
	dp := gauge.DataPoints().AppendEmpty()
	dp.SetDoubleValue(42.0)

	// Add exemplar with trace context
	ex := dp.Exemplars().AppendEmpty()
	ex.SetTimestamp(pcommon.Timestamp(1234567890))
	ex.SetDoubleValue(42.5)
	ex.SetTraceID([16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
	ex.SetSpanID([8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18})
	ex.FilteredAttributes().PutStr("key", "value")

	req := pmetricotlp.NewExportRequestFromMetrics(metrics)

	// Roundtrip
	redpandaMetrics := MetricsToRedpanda(req)

	pbGauge := redpandaMetrics[0].GetGauge()
	require.NotNil(t, pbGauge)
	require.Len(t, pbGauge.DataPoints, 1)

	pbDp := pbGauge.DataPoints[0]
	require.Len(t, pbDp.Exemplars, 1)

	pbEx := pbDp.Exemplars[0]
	assert.NotEmpty(t, pbEx.TraceId)
	assert.NotEmpty(t, pbEx.SpanId)
	assert.NotNil(t, pbEx.GetAsDouble())

	// Convert back
	reconstructed := MetricsFromRedpanda(redpandaMetrics)

	recGauge := reconstructed.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge()
	recEx := recGauge.DataPoints().At(0).Exemplars().At(0)
	assert.False(t, recEx.TraceID().IsEmpty())
	assert.False(t, recEx.SpanID().IsEmpty())
}

func TestEmptyMetricsRequest(t *testing.T) {
	// Create empty request
	metrics := pmetric.NewMetrics()
	req := pmetricotlp.NewExportRequestFromMetrics(metrics)

	// Convert to Redpanda
	redpandaMetrics := MetricsToRedpanda(req)
	assert.Empty(t, redpandaMetrics)

	// Convert back
	reconstructed := MetricsFromRedpanda(redpandaMetrics)
	assert.Equal(t, 0, reconstructed.Metrics().ResourceMetrics().Len())
}
