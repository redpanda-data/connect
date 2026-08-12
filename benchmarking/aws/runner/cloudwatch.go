// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

// CloudWatchNamespace is the fixed namespace every soak-run datum is
// published under. Dashboards are built against this exact string — do not
// change it without updating every consumer.
const CloudWatchNamespace = "RedpandaConnect/Bench"

// CloudWatch dimension names. Every datum this package emits carries exactly
// these two, and no others — see MetricsEmitter.
const (
	dimensionConnector = "Connector"
	dimensionScenario  = "Scenario"
)

// Metric names published under CloudWatchNamespace. This is the contract a
// dashboard is built against — do not rename without a coordinated change.
const (
	metricThroughputMBps    = "ThroughputMBps"
	metricRecordsPerSec     = "RecordsPerSec"
	metricLogThroughputMBps = "LogThroughputMBps"
	metricRSSBytes          = "RSSBytes"
	metricHeapInUseBytes    = "HeapInUseBytes"
	metricGoroutines        = "Goroutines"
	metricBacklogSeconds    = "BacklogSeconds"
	metricRunActive         = "RunActive"
)

// CloudWatch unit strings (cwtypes.StandardUnit's own enum values).
const (
	unitMegabytesPerSecond = "Megabytes/Second"
	unitCountPerSecond     = "Count/Second"
	unitBytes              = "Bytes"
	unitCount              = "Count"
	unitSeconds            = "Seconds"
)

// secondsPerMinute is the bucket width aggregateSoakMinutes groups every
// series into, matching the per-minute datum cadence the dashboard contract
// specifies.
const secondsPerMinute = 60

// MetricDatum is one CloudWatch data point. Dimensions (Connector, Scenario)
// are NOT carried here — every datum in a run shares the same two dimension
// values, which the MetricsEmitter implementation applies once, at
// construction, rather than per-datum.
type MetricDatum struct {
	Name  string
	Value float64
	Unit  string
	At    time.Time
}

// MetricsEmitter publishes soak-run metrics to an external system.
//
// Only the orchestrator (this Go process, running on the operator's
// machine or in CI) ever calls Emit. The EC2 instance role the bench script
// runs under is deliberately never given cloudwatch:PutMetricData: a future
// PR-mode binary running untrusted code under that role must not be able to
// spoof the very metrics that judge it.
type MetricsEmitter interface {
	Emit(ctx context.Context, data []MetricDatum) error
}

// cloudWatchPutChunk caps how many MetricDatum entries go into a single
// PutMetricData call. The API allows up to 1000 per call; chunking at half
// that keeps the request comfortably under the 1MB payload limit even when
// every datum carries a long metric name, two dimensions, and a
// full-precision timestamp.
const cloudWatchPutChunk = 500

// awsCloudWatch is the production MetricsEmitter, backed by the real
// CloudWatch API. Namespace and the two dimension values are fixed at
// construction, so every Emit call from one instance is self-consistent
// with the metric contract dashboards are built against.
type awsCloudWatch struct {
	client    *cloudwatch.Client
	namespace string
	connector string
	scenario  string
}

// NewCloudWatchEmitter builds a MetricsEmitter backed by the AWS SDK in the
// given region. connector and scenario become the Connector/Scenario
// dimensions on every datum this emitter publishes; namespace is expected to
// be CloudWatchNamespace in production (a parameter, not a constant, so
// tests can point at a scratch namespace).
func NewCloudWatchEmitter(ctx context.Context, region, namespace, connector, scenario string) (MetricsEmitter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return &awsCloudWatch{
		client:    cloudwatch.NewFromConfig(cfg),
		namespace: namespace,
		connector: connector,
		scenario:  scenario,
	}, nil
}

func (c *awsCloudWatch) Emit(ctx context.Context, data []MetricDatum) error {
	dims := []cwtypes.Dimension{
		{Name: strPtr(dimensionConnector), Value: strPtr(c.connector)},
		{Name: strPtr(dimensionScenario), Value: strPtr(c.scenario)},
	}
	for start := 0; start < len(data); start += cloudWatchPutChunk {
		end := min(start+cloudWatchPutChunk, len(data))
		chunk := data[start:end]
		md := make([]cwtypes.MetricDatum, len(chunk))
		for i, d := range chunk {
			at := d.At
			md[i] = cwtypes.MetricDatum{
				MetricName: strPtr(d.Name),
				Value:      floatPtr(d.Value),
				Unit:       cwtypes.StandardUnit(d.Unit),
				Timestamp:  &at,
				Dimensions: dims,
			}
		}
		if _, err := c.client.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
			Namespace:  strPtr(c.namespace),
			MetricData: md,
		}); err != nil {
			return fmt.Errorf("put metric data (namespace %s, %d data): %w", c.namespace, len(chunk), err)
		}
	}
	return nil
}

func strPtr(s string) *string { return &s }

func floatPtr(f float64) *float64 { return &f }

// FakeEmitter is a MetricsEmitter that records every Emit call instead of
// touching AWS — for tests.
type FakeEmitter struct {
	mu    sync.Mutex
	Calls [][]MetricDatum
	// Err, when non-nil, is returned by every Emit call (and nothing is
	// recorded) — for testing the emit-failure-is-non-fatal path.
	Err error
}

func (f *FakeEmitter) Emit(_ context.Context, data []MetricDatum) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	f.Calls = append(f.Calls, append([]MetricDatum(nil), data...))
	return nil
}

// All flattens every recorded Emit call into one slice, in call order — the
// shape most tests want when asserting on emitted data regardless of how
// many cycles produced it.
func (f *FakeEmitter) All() []MetricDatum {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []MetricDatum
	for _, c := range f.Calls {
		out = append(out, c...)
	}
	return out
}

// CallCount returns how many Emit calls have been recorded so far. A
// locked accessor, not direct len(f.Calls) — a test polling this alongside
// the mid-run emit goroutine (e.g. via require.Eventually) would otherwise
// race with Emit's own append under f.mu.
func (f *FakeEmitter) CallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Calls)
}

// LastCall returns the most recent Emit call's data, or nil if none yet.
// Also locked, for the same reason as CallCount.
func (f *FakeEmitter) LastCall() []MetricDatum {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.Calls) == 0 {
		return nil
	}
	return f.Calls[len(f.Calls)-1]
}

// minuteAgg accumulates one minute bucket's worth of samples for one metric,
// carrying both the running mean (sum/count) and the last-seen value —
// callers pick whichever aggregateSoakMinutes' metric contract calls for.
type minuteAgg struct {
	sum   float64
	count int
	last  float64
}

func (a *minuteAgg) addMean(v float64) {
	a.sum += v
	a.count++
}

func (a *minuteAgg) addLast(v float64) {
	a.last = v
}

func (a *minuteAgg) mean() float64 {
	if a.count == 0 {
		return 0
	}
	return a.sum / float64(a.count)
}

// bucketMean folds t (seconds since base) into its minute bucket in buckets,
// accumulating v for a mean, and returns the bucket's minute index while
// updating *maxMinute.
func bucketMean(buckets map[int]*minuteAgg, maxMinute *int, t int, v float64) {
	minute := t / secondsPerMinute
	if minute > *maxMinute {
		*maxMinute = minute
	}
	a, ok := buckets[minute]
	if !ok {
		a = &minuteAgg{}
		buckets[minute] = a
	}
	a.addMean(v)
}

// bucketLast is bucketMean's last-value counterpart. Series are assumed to
// already be in ascending-T order (every parser in this package produces
// them that way), so simply overwriting `last` on each call yields the
// correct "last value observed in this minute" semantics without tracking
// timestamps per bucket.
func bucketLast(buckets map[int]*minuteAgg, maxMinute *int, t int, v float64) {
	minute := t / secondsPerMinute
	if minute > *maxMinute {
		*maxMinute = minute
	}
	a, ok := buckets[minute]
	if !ok {
		a = &minuteAgg{}
		buckets[minute] = a
	}
	a.addLast(v)
}

// appendMean appends a MetricDatum for minute from buckets using the mean
// aggregation, or leaves data untouched if that minute has no samples for
// this metric (e.g. BacklogSeconds when the scenario set no expected rate).
func appendMean(data []MetricDatum, buckets map[int]*minuteAgg, minute int, name, unit string, at time.Time) []MetricDatum {
	a, ok := buckets[minute]
	if !ok {
		return data
	}
	return append(data, MetricDatum{Name: name, Value: a.mean(), Unit: unit, At: at})
}

// appendLast is appendMean's last-value counterpart.
func appendLast(data []MetricDatum, buckets map[int]*minuteAgg, minute int, name, unit string, at time.Time) []MetricDatum {
	a, ok := buckets[minute]
	if !ok {
		return data
	}
	return append(data, MetricDatum{Name: name, Value: a.last, Unit: unit, At: at})
}

// aggregateSoakMinutes buckets samples/prom/broker/backlog into whole
// minutes offset from base (the sweep point's own wall-clock launch time)
// and returns the MetricDatum set for every minute that is both newly
// available (strictly after sinceMinute) and no longer at risk of getting
// more data in a future checkpoint (strictly before the CURRENT incomplete
// minute).
//
// "Current incomplete minute" is derived purely from the data itself — the
// highest minute bucket touched by any of the four series — never from
// time.Now(). That is what makes this a pure, deterministic function: the
// same inputs always produce the same output, so the mid-run emit loop and
// the final post-point emit can share it without either one's behavior
// depending on wall-clock skew between the call and the data it's looking
// at.
//
// All four series MUST already share one base: T=0 means "at base" for
// every one of them. Sample.T does NOT satisfy this on its own — parseAndTrim
// drops warmup samples and reindexes T to 0 at end-of-warmup, so a raw
// Sample's T=0 is actually base+warmup, not base. Callers must shift Sample.T
// by +warmup (see offsetSampleT in matrix.go) before calling this function;
// aggregateSoakMinutes itself has no warmup concept and trusts the caller.
//
// Returns (nil, sinceMinute) unchanged when there is no data in any series,
// or when every minute present has already been emitted (max minute - 1 <=
// sinceMinute).
func aggregateSoakMinutes(
	samples []Sample,
	prom []PromPoint,
	broker []TopicPoint,
	backlog []BacklogPoint,
	base time.Time,
	sinceMinute int,
) ([]MetricDatum, int) {
	throughput := map[int]*minuteAgg{}
	records := map[int]*minuteAgg{}
	logThroughput := map[int]*minuteAgg{}
	rss := map[int]*minuteAgg{}
	heap := map[int]*minuteAgg{}
	goroutines := map[int]*minuteAgg{}
	backlogSec := map[int]*minuteAgg{}

	maxMinute := -1
	for _, p := range broker {
		bucketMean(throughput, &maxMinute, p.T, p.MBPerSec)
		bucketMean(records, &maxMinute, p.T, p.MsgPerSec)
	}
	for _, s := range samples {
		bucketMean(logThroughput, &maxMinute, s.T, s.MBPerSec)
	}
	for _, pp := range prom {
		bucketLast(rss, &maxMinute, pp.T, float64(pp.RSSBytes))
		bucketLast(heap, &maxMinute, pp.T, pp.HeapInUseMB*1_000_000)
		bucketLast(goroutines, &maxMinute, pp.T, float64(pp.Goroutines))
	}
	for _, b := range backlog {
		bucketLast(backlogSec, &maxMinute, b.T, b.BacklogSec)
	}

	if maxMinute < 0 {
		return nil, sinceMinute
	}

	var data []MetricDatum
	newHW := sinceMinute
	for minute := sinceMinute + 1; minute < maxMinute; minute++ {
		at := base.Add(time.Duration(minute) * time.Minute)
		data = appendMean(data, throughput, minute, metricThroughputMBps, unitMegabytesPerSecond, at)
		data = appendMean(data, records, minute, metricRecordsPerSec, unitCountPerSecond, at)
		data = appendMean(data, logThroughput, minute, metricLogThroughputMBps, unitMegabytesPerSecond, at)
		data = appendLast(data, rss, minute, metricRSSBytes, unitBytes, at)
		data = appendLast(data, heap, minute, metricHeapInUseBytes, unitBytes, at)
		data = appendLast(data, goroutines, minute, metricGoroutines, unitCount, at)
		data = appendLast(data, backlogSec, minute, metricBacklogSeconds, unitSeconds, at)
		newHW = minute
	}
	return data, newHW
}
