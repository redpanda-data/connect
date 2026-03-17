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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	pb "buf.build/gen/go/redpandadata/otel/protocolbuffers/go/redpanda/otel/v1"
)

// MetricsCount counts the total number of metrics in the request.
func MetricsCount(req pmetricotlp.ExportRequest) int {
	metrics := req.Metrics()
	resourceMetrics := metrics.ResourceMetrics()

	n := 0
	for i := range resourceMetrics.Len() {
		scopeMetrics := resourceMetrics.At(i).ScopeMetrics()
		for j := range scopeMetrics.Len() {
			n += scopeMetrics.At(j).Metrics().Len()
		}
	}
	return n
}

// MetricsToRedpandaFunc converts OTLP metric export request to individual Redpanda
// metric records via callback. Each metric from the batch becomes a self-contained
// message with embedded Resource/Scope. The callback receives a pointer to the
// metric and can process or store it. The callback returns true to continue
// processing or false to stop early.
func MetricsToRedpandaFunc(req pmetricotlp.ExportRequest, cb func(*pb.Metric) bool) {
	metrics := req.Metrics()
	resourceMetrics := metrics.ResourceMetrics()

	for i := range resourceMetrics.Len() {
		rm := resourceMetrics.At(i)
		resource := rm.Resource()
		resourceSchemaURL := rm.SchemaUrl()

		scopeMetrics := rm.ScopeMetrics()
		for j := range scopeMetrics.Len() {
			sm := scopeMetrics.At(j)
			scope := sm.Scope()
			scopeSchemaURL := sm.SchemaUrl()

			metricsSlice := sm.Metrics()
			for k := range metricsSlice.Len() {
				var m pb.Metric
				metric := metricsSlice.At(k)
				metricToRedpanda(&m, metric,
					resource, resourceSchemaURL, scope, scopeSchemaURL)
				if !cb(&m) {
					return
				}
			}
		}
	}
}

// MetricsFromRedpanda converts individual Redpanda metric records to OTLP
// metric export request. Groups metrics by Resource and Scope to create
// efficient batch structure.
func MetricsFromRedpanda(metrics []pb.Metric) pmetricotlp.ExportRequest {
	pMetrics := pmetric.NewMetrics()

	if len(metrics) == 0 {
		return pmetricotlp.NewExportRequestFromMetrics(pMetrics)
	}

	var (
		curResourceMetrics pmetric.ResourceMetrics
		curScopeMetrics    pmetric.ScopeMetrics

		curResHash   = "-"
		curScopeHash = "-"
	)
	for i := range metrics {
		metric := &metrics[i]
		resHash := ResourceHash(metric.Resource)
		scopeHash := ScopeHash(metric.Scope)

		// Check if resource changed
		if resHash != curResHash {
			curResourceMetrics = pMetrics.ResourceMetrics().AppendEmpty()
			resourceFromRedpanda(metric.Resource, curResourceMetrics.Resource())
			curResourceMetrics.SetSchemaUrl(metric.ResourceSchemaUrl)
			curResHash = resHash
			curScopeHash = "" // Reset scope hash
		}
		if scopeHash != curScopeHash {
			curScopeMetrics = curResourceMetrics.ScopeMetrics().AppendEmpty()
			scopeFromRedpanda(metric.Scope, curScopeMetrics.Scope())
			curScopeMetrics.SetSchemaUrl(metric.ScopeSchemaUrl)
			curScopeHash = scopeHash
		}

		// Add metric to current scope
		m := curScopeMetrics.Metrics().AppendEmpty()
		metricFromRedpanda(&m, metric)
	}

	return pmetricotlp.NewExportRequestFromMetrics(pMetrics)
}

// metricToRedpanda converts a single pdata Metric to Redpanda protobuf Metric.
func metricToRedpanda(
	dst *pb.Metric,
	src pmetric.Metric,
	resource pcommon.Resource,
	resourceSchemaURL string,
	scope pcommon.InstrumentationScope,
	scopeSchemaURL string,
) {
	dst.Resource = resourceToRedpanda(resource)
	dst.ResourceSchemaUrl = resourceSchemaURL
	dst.Scope = scopeToRedpanda(scope)
	dst.ScopeSchemaUrl = scopeSchemaURL
	dst.Name = src.Name()
	dst.Description = src.Description()
	dst.Unit = src.Unit()

	// Handle different metric types
	switch src.Type() {
	case pmetric.MetricTypeGauge:
		dst.Data = &pb.Metric_Gauge{
			Gauge: gaugeToRedpanda(src.Gauge()),
		}
	case pmetric.MetricTypeSum:
		dst.Data = &pb.Metric_Sum{
			Sum: sumToRedpanda(src.Sum()),
		}
	case pmetric.MetricTypeHistogram:
		dst.Data = &pb.Metric_Histogram{
			Histogram: histogramToRedpanda(src.Histogram()),
		}
	case pmetric.MetricTypeExponentialHistogram:
		dst.Data = &pb.Metric_ExponentialHistogram{
			ExponentialHistogram: exponentialHistogramToRedpanda(src.ExponentialHistogram()),
		}
	case pmetric.MetricTypeSummary:
		dst.Data = &pb.Metric_Summary{
			Summary: summaryToRedpanda(src.Summary()),
		}
	}
}

// metricFromRedpanda converts Redpanda protobuf Metric to pdata Metric.
func metricFromRedpanda(dst *pmetric.Metric, src *pb.Metric) {
	dst.SetName(src.Name)
	dst.SetDescription(src.Description)
	dst.SetUnit(src.Unit)

	// Handle different metric types
	switch data := src.Data.(type) {
	case *pb.Metric_Gauge:
		gaugeFromRedpanda(data.Gauge, dst.SetEmptyGauge())
	case *pb.Metric_Sum:
		sumFromRedpanda(data.Sum, dst.SetEmptySum())
	case *pb.Metric_Histogram:
		histogramFromRedpanda(data.Histogram, dst.SetEmptyHistogram())
	case *pb.Metric_ExponentialHistogram:
		exponentialHistogramFromRedpanda(data.ExponentialHistogram, dst.SetEmptyExponentialHistogram())
	case *pb.Metric_Summary:
		summaryFromRedpanda(data.Summary, dst.SetEmptySummary())
	}
}

// gaugeToRedpanda converts pdata Gauge to Redpanda protobuf Gauge.
func gaugeToRedpanda(src pmetric.Gauge) *pb.Gauge {
	return &pb.Gauge{
		DataPoints: numberDataPointsToRedpanda(src.DataPoints()),
	}
}

// gaugeFromRedpanda converts Redpanda protobuf Gauge to pdata Gauge.
func gaugeFromRedpanda(src *pb.Gauge, dest pmetric.Gauge) {
	if src == nil {
		return
	}
	numberDataPointsFromRedpanda(src.DataPoints, dest.DataPoints())
}

// sumToRedpanda converts pdata Sum to Redpanda protobuf Sum.
func sumToRedpanda(src pmetric.Sum) *pb.Sum {
	return &pb.Sum{
		DataPoints:             numberDataPointsToRedpanda(src.DataPoints()),
		AggregationTemporality: aggregationTemporalityToRedpanda(src.AggregationTemporality()),
		IsMonotonic:            src.IsMonotonic(),
	}
}

// sumFromRedpanda converts Redpanda protobuf Sum to pdata Sum.
func sumFromRedpanda(src *pb.Sum, dest pmetric.Sum) {
	if src == nil {
		return
	}
	numberDataPointsFromRedpanda(src.DataPoints, dest.DataPoints())
	dest.SetAggregationTemporality(aggregationTemporalityFromRedpanda(src.AggregationTemporality))
	dest.SetIsMonotonic(src.IsMonotonic)
}

// histogramToRedpanda converts pdata Histogram to Redpanda protobuf Histogram.
func histogramToRedpanda(src pmetric.Histogram) *pb.Histogram {
	return &pb.Histogram{
		DataPoints:             histogramDataPointsToRedpanda(src.DataPoints()),
		AggregationTemporality: aggregationTemporalityToRedpanda(src.AggregationTemporality()),
	}
}

// histogramFromRedpanda converts Redpanda protobuf Histogram to pdata Histogram.
func histogramFromRedpanda(src *pb.Histogram, dest pmetric.Histogram) {
	if src == nil {
		return
	}
	histogramDataPointsFromRedpanda(src.DataPoints, dest.DataPoints())
	dest.SetAggregationTemporality(aggregationTemporalityFromRedpanda(src.AggregationTemporality))
}

// exponentialHistogramToRedpanda converts pdata ExponentialHistogram to Redpanda protobuf ExponentialHistogram.
func exponentialHistogramToRedpanda(src pmetric.ExponentialHistogram) *pb.ExponentialHistogram {
	return &pb.ExponentialHistogram{
		DataPoints:             exponentialHistogramDataPointsToRedpanda(src.DataPoints()),
		AggregationTemporality: aggregationTemporalityToRedpanda(src.AggregationTemporality()),
	}
}

// exponentialHistogramFromRedpanda converts Redpanda protobuf ExponentialHistogram to pdata ExponentialHistogram.
func exponentialHistogramFromRedpanda(src *pb.ExponentialHistogram, dest pmetric.ExponentialHistogram) {
	if src == nil {
		return
	}
	exponentialHistogramDataPointsFromRedpanda(src.DataPoints, dest.DataPoints())
	dest.SetAggregationTemporality(aggregationTemporalityFromRedpanda(src.AggregationTemporality))
}

// summaryToRedpanda converts pdata Summary to Redpanda protobuf Summary.
func summaryToRedpanda(src pmetric.Summary) *pb.Summary {
	return &pb.Summary{
		DataPoints: summaryDataPointsToRedpanda(src.DataPoints()),
	}
}

// summaryFromRedpanda converts Redpanda protobuf Summary to pdata Summary.
func summaryFromRedpanda(src *pb.Summary, dest pmetric.Summary) {
	if src == nil {
		return
	}
	summaryDataPointsFromRedpanda(src.DataPoints, dest.DataPoints())
}

// aggregationTemporalityToRedpanda converts pdata AggregationTemporality to Redpanda protobuf AggregationTemporality.
func aggregationTemporalityToRedpanda(src pmetric.AggregationTemporality) pb.AggregationTemporality {
	switch src {
	case pmetric.AggregationTemporalityDelta:
		return pb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
	case pmetric.AggregationTemporalityCumulative:
		return pb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
	default:
		return pb.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED
	}
}

// aggregationTemporalityFromRedpanda converts Redpanda protobuf AggregationTemporality to pdata AggregationTemporality.
func aggregationTemporalityFromRedpanda(src pb.AggregationTemporality) pmetric.AggregationTemporality {
	switch src {
	case pb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA:
		return pmetric.AggregationTemporalityDelta
	case pb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE:
		return pmetric.AggregationTemporalityCumulative
	default:
		return pmetric.AggregationTemporalityUnspecified
	}
}

// numberDataPointsToRedpanda converts pdata NumberDataPointSlice to Redpanda protobuf NumberDataPoint slice.
func numberDataPointsToRedpanda(src pmetric.NumberDataPointSlice) []*pb.NumberDataPoint {
	if src.Len() == 0 {
		return nil
	}

	dataPoints := make([]*pb.NumberDataPoint, 0, src.Len())
	for i := range src.Len() {
		dp := src.At(i)
		pbDataPoint := &pb.NumberDataPoint{
			Attributes:        attributesToRedpanda(dp.Attributes()),
			StartTimeUnixNano: int64ToUint64(int64(dp.StartTimestamp())),
			TimeUnixNano:      int64ToUint64(int64(dp.Timestamp())),
			Exemplars:         exemplarsToRedpanda(dp.Exemplars()),
			Flags:             uint32(dp.Flags()),
		}

		// Set value based on type
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			pbDataPoint.Value = &pb.NumberDataPoint_AsInt{AsInt: dp.IntValue()}
		case pmetric.NumberDataPointValueTypeDouble:
			pbDataPoint.Value = &pb.NumberDataPoint_AsDouble{AsDouble: dp.DoubleValue()}
		}

		dataPoints = append(dataPoints, pbDataPoint)
	}
	return dataPoints
}

// numberDataPointsFromRedpanda converts Redpanda protobuf NumberDataPoint slice to pdata NumberDataPointSlice.
func numberDataPointsFromRedpanda(src []*pb.NumberDataPoint, dest pmetric.NumberDataPointSlice) {
	if len(src) == 0 {
		return
	}

	dest.EnsureCapacity(len(src))
	for _, pbDp := range src {
		dp := dest.AppendEmpty()
		attributesFromRedpanda(pbDp.Attributes, dp.Attributes())
		dp.SetStartTimestamp(pcommon.Timestamp(uint64ToInt64(pbDp.StartTimeUnixNano)))
		dp.SetTimestamp(pcommon.Timestamp(uint64ToInt64(pbDp.TimeUnixNano)))

		// Set value based on type
		switch v := pbDp.Value.(type) {
		case *pb.NumberDataPoint_AsInt:
			dp.SetIntValue(v.AsInt)
		case *pb.NumberDataPoint_AsDouble:
			dp.SetDoubleValue(v.AsDouble)
		}

		exemplarsFromRedpanda(pbDp.Exemplars, dp.Exemplars())
		dp.SetFlags(pmetric.DataPointFlags(pbDp.Flags))
	}
}

// histogramDataPointsToRedpanda converts pdata HistogramDataPointSlice to Redpanda protobuf HistogramDataPoint slice.
func histogramDataPointsToRedpanda(src pmetric.HistogramDataPointSlice) []*pb.HistogramDataPoint {
	if src.Len() == 0 {
		return nil
	}

	dataPoints := make([]*pb.HistogramDataPoint, 0, src.Len())
	for i := range src.Len() {
		dp := src.At(i)
		pbDataPoint := &pb.HistogramDataPoint{
			Attributes:        attributesToRedpanda(dp.Attributes()),
			StartTimeUnixNano: int64ToUint64(int64(dp.StartTimestamp())),
			TimeUnixNano:      int64ToUint64(int64(dp.Timestamp())),
			Count:             dp.Count(),
			ExplicitBounds:    dp.ExplicitBounds().AsRaw(),
			BucketCounts:      dp.BucketCounts().AsRaw(),
			Exemplars:         exemplarsToRedpanda(dp.Exemplars()),
			Flags:             uint32(dp.Flags()),
		}

		// Optional sum
		if dp.HasSum() {
			sum := dp.Sum()
			pbDataPoint.Sum = &sum
		}

		// Optional min
		if dp.HasMin() {
			minVal := dp.Min()
			pbDataPoint.Min = &minVal
		}

		// Optional max
		if dp.HasMax() {
			maxVal := dp.Max()
			pbDataPoint.Max = &maxVal
		}

		dataPoints = append(dataPoints, pbDataPoint)
	}
	return dataPoints
}

// histogramDataPointsFromRedpanda converts Redpanda protobuf HistogramDataPoint slice to pdata HistogramDataPointSlice.
func histogramDataPointsFromRedpanda(src []*pb.HistogramDataPoint, dest pmetric.HistogramDataPointSlice) {
	if len(src) == 0 {
		return
	}

	dest.EnsureCapacity(len(src))
	for _, pbDp := range src {
		dp := dest.AppendEmpty()
		attributesFromRedpanda(pbDp.Attributes, dp.Attributes())
		dp.SetStartTimestamp(pcommon.Timestamp(uint64ToInt64(pbDp.StartTimeUnixNano)))
		dp.SetTimestamp(pcommon.Timestamp(uint64ToInt64(pbDp.TimeUnixNano)))
		dp.SetCount(pbDp.Count)

		if pbDp.Sum != nil {
			dp.SetSum(*pbDp.Sum)
		}
		if pbDp.Min != nil {
			dp.SetMin(*pbDp.Min)
		}
		if pbDp.Max != nil {
			dp.SetMax(*pbDp.Max)
		}

		dp.ExplicitBounds().FromRaw(pbDp.ExplicitBounds)
		dp.BucketCounts().FromRaw(pbDp.BucketCounts)

		exemplarsFromRedpanda(pbDp.Exemplars, dp.Exemplars())
		dp.SetFlags(pmetric.DataPointFlags(pbDp.Flags))
	}
}

// exponentialHistogramDataPointsToRedpanda converts pdata ExponentialHistogramDataPointSlice to Redpanda protobuf slice.
func exponentialHistogramDataPointsToRedpanda(src pmetric.ExponentialHistogramDataPointSlice) []*pb.ExponentialHistogramDataPoint {
	if src.Len() == 0 {
		return nil
	}

	dataPoints := make([]*pb.ExponentialHistogramDataPoint, 0, src.Len())
	for i := range src.Len() {
		dp := src.At(i)
		pbDataPoint := &pb.ExponentialHistogramDataPoint{
			Attributes:        attributesToRedpanda(dp.Attributes()),
			StartTimeUnixNano: int64ToUint64(int64(dp.StartTimestamp())),
			TimeUnixNano:      int64ToUint64(int64(dp.Timestamp())),
			Count:             dp.Count(),
			Scale:             dp.Scale(),
			ZeroCount:         dp.ZeroCount(),
			ZeroThreshold:     dp.ZeroThreshold(),
			Positive: &pb.ExponentialHistogramDataPoint_Buckets{
				Offset:       dp.Positive().Offset(),
				BucketCounts: dp.Positive().BucketCounts().AsRaw(),
			},
			Negative: &pb.ExponentialHistogramDataPoint_Buckets{
				Offset:       dp.Negative().Offset(),
				BucketCounts: dp.Negative().BucketCounts().AsRaw(),
			},
			Exemplars: exemplarsToRedpanda(dp.Exemplars()),
			Flags:     uint32(dp.Flags()),
		}

		// Optional sum
		if dp.HasSum() {
			sum := dp.Sum()
			pbDataPoint.Sum = &sum
		}

		// Optional min
		if dp.HasMin() {
			minVal := dp.Min()
			pbDataPoint.Min = &minVal
		}

		// Optional max
		if dp.HasMax() {
			maxVal := dp.Max()
			pbDataPoint.Max = &maxVal
		}

		dataPoints = append(dataPoints, pbDataPoint)
	}
	return dataPoints
}

// exponentialHistogramDataPointsFromRedpanda converts Redpanda protobuf slice to pdata ExponentialHistogramDataPointSlice.
func exponentialHistogramDataPointsFromRedpanda(src []*pb.ExponentialHistogramDataPoint, dest pmetric.ExponentialHistogramDataPointSlice) {
	if len(src) == 0 {
		return
	}

	dest.EnsureCapacity(len(src))
	for _, pbDp := range src {
		dp := dest.AppendEmpty()
		attributesFromRedpanda(pbDp.Attributes, dp.Attributes())
		dp.SetStartTimestamp(pcommon.Timestamp(uint64ToInt64(pbDp.StartTimeUnixNano)))
		dp.SetTimestamp(pcommon.Timestamp(uint64ToInt64(pbDp.TimeUnixNano)))
		dp.SetCount(pbDp.Count)

		if pbDp.Sum != nil {
			dp.SetSum(*pbDp.Sum)
		}
		if pbDp.Min != nil {
			dp.SetMin(*pbDp.Min)
		}
		if pbDp.Max != nil {
			dp.SetMax(*pbDp.Max)
		}

		dp.SetScale(pbDp.Scale)
		dp.SetZeroCount(pbDp.ZeroCount)
		dp.SetZeroThreshold(pbDp.ZeroThreshold)

		if pbDp.Positive != nil {
			dp.Positive().SetOffset(pbDp.Positive.Offset)
			dp.Positive().BucketCounts().FromRaw(pbDp.Positive.BucketCounts)
		}

		if pbDp.Negative != nil {
			dp.Negative().SetOffset(pbDp.Negative.Offset)
			dp.Negative().BucketCounts().FromRaw(pbDp.Negative.BucketCounts)
		}

		exemplarsFromRedpanda(pbDp.Exemplars, dp.Exemplars())
		dp.SetFlags(pmetric.DataPointFlags(pbDp.Flags))
	}
}

// summaryDataPointsToRedpanda converts pdata SummaryDataPointSlice to Redpanda protobuf SummaryDataPoint slice.
func summaryDataPointsToRedpanda(src pmetric.SummaryDataPointSlice) []*pb.SummaryDataPoint {
	if src.Len() == 0 {
		return nil
	}

	dataPoints := make([]*pb.SummaryDataPoint, 0, src.Len())
	for i := range src.Len() {
		dp := src.At(i)
		pbDataPoint := &pb.SummaryDataPoint{
			Attributes:        attributesToRedpanda(dp.Attributes()),
			StartTimeUnixNano: int64ToUint64(int64(dp.StartTimestamp())),
			TimeUnixNano:      int64ToUint64(int64(dp.Timestamp())),
			Count:             dp.Count(),
			Sum:               dp.Sum(),
			Flags:             uint32(dp.Flags()),
		}

		// Convert quantile values
		quantileValues := dp.QuantileValues()
		if quantileValues.Len() > 0 {
			pbDataPoint.QuantileValues = make([]*pb.SummaryDataPoint_ValueAtQuantile, 0, quantileValues.Len())
			for j := range quantileValues.Len() {
				qv := quantileValues.At(j)
				pbDataPoint.QuantileValues = append(pbDataPoint.QuantileValues, &pb.SummaryDataPoint_ValueAtQuantile{
					Quantile: qv.Quantile(),
					Value:    qv.Value(),
				})
			}
		}

		dataPoints = append(dataPoints, pbDataPoint)
	}
	return dataPoints
}

// summaryDataPointsFromRedpanda converts Redpanda protobuf SummaryDataPoint slice to pdata SummaryDataPointSlice.
func summaryDataPointsFromRedpanda(src []*pb.SummaryDataPoint, dest pmetric.SummaryDataPointSlice) {
	if len(src) == 0 {
		return
	}

	dest.EnsureCapacity(len(src))
	for _, pbDp := range src {
		dp := dest.AppendEmpty()
		attributesFromRedpanda(pbDp.Attributes, dp.Attributes())
		dp.SetStartTimestamp(pcommon.Timestamp(uint64ToInt64(pbDp.StartTimeUnixNano)))
		dp.SetTimestamp(pcommon.Timestamp(uint64ToInt64(pbDp.TimeUnixNano)))
		dp.SetCount(pbDp.Count)
		dp.SetSum(pbDp.Sum)

		// Convert quantile values
		if len(pbDp.QuantileValues) > 0 {
			qvSlice := dp.QuantileValues()
			qvSlice.EnsureCapacity(len(pbDp.QuantileValues))
			for _, pbQv := range pbDp.QuantileValues {
				qv := qvSlice.AppendEmpty()
				qv.SetQuantile(pbQv.Quantile)
				qv.SetValue(pbQv.Value)
			}
		}

		dp.SetFlags(pmetric.DataPointFlags(pbDp.Flags))
	}
}

// exemplarsToRedpanda converts pdata ExemplarSlice to Redpanda protobuf Exemplar slice.
func exemplarsToRedpanda(src pmetric.ExemplarSlice) []*pb.Exemplar {
	if src.Len() == 0 {
		return nil
	}

	exemplars := make([]*pb.Exemplar, 0, src.Len())
	for i := range src.Len() {
		ex := src.At(i)
		pbExemplar := &pb.Exemplar{
			FilteredAttributes: attributesToRedpanda(ex.FilteredAttributes()),
			TimeUnixNano:       int64ToUint64(int64(ex.Timestamp())),
		}

		// Set value based on type
		switch ex.ValueType() {
		case pmetric.ExemplarValueTypeInt:
			pbExemplar.Value = &pb.Exemplar_AsInt{AsInt: ex.IntValue()}
		case pmetric.ExemplarValueTypeDouble:
			pbExemplar.Value = &pb.Exemplar_AsDouble{AsDouble: ex.DoubleValue()}
		}

		// Add trace context if present
		traceID := ex.TraceID()
		if !traceID.IsEmpty() {
			pbExemplar.TraceId = traceID[:]
		}

		spanID := ex.SpanID()
		if !spanID.IsEmpty() {
			pbExemplar.SpanId = spanID[:]
		}

		exemplars = append(exemplars, pbExemplar)
	}
	return exemplars
}

// exemplarsFromRedpanda converts Redpanda protobuf Exemplar slice to pdata ExemplarSlice.
func exemplarsFromRedpanda(src []*pb.Exemplar, dest pmetric.ExemplarSlice) {
	if len(src) == 0 {
		return
	}

	dest.EnsureCapacity(len(src))
	for _, pbEx := range src {
		ex := dest.AppendEmpty()
		attributesFromRedpanda(pbEx.FilteredAttributes, ex.FilteredAttributes())
		ex.SetTimestamp(pcommon.Timestamp(uint64ToInt64(pbEx.TimeUnixNano)))

		// Set value based on type
		switch v := pbEx.Value.(type) {
		case *pb.Exemplar_AsInt:
			ex.SetIntValue(v.AsInt)
		case *pb.Exemplar_AsDouble:
			ex.SetDoubleValue(v.AsDouble)
		}

		// Add trace context if present
		if len(pbEx.TraceId) == 16 {
			var traceID [16]byte
			copy(traceID[:], pbEx.TraceId)
			ex.SetTraceID(traceID)
		}

		if len(pbEx.SpanId) == 8 {
			var spanID [8]byte
			copy(spanID[:], pbEx.SpanId)
			ex.SetSpanID(spanID)
		}
	}
}
