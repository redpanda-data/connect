// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestCacheSCNCountsWriteErrors makes sure that the write error counter is
// exported at 0 from the start, counts a failed write, and does not count a
// write that fails because the context is cancelled (the pipeline stops).
func TestCacheSCNCountsWriteErrors(t *testing.T) {
	const name = "oracledb_cdc_checkpoint_write_errors_total"

	exp := &counterExporter{counters: map[string]int64{}}
	o := &oracleDBCDCInput{
		cfg:                   Config{SCNCacheKey: "scn"},
		cpCache:               failingCache{},
		checkpointWriteErrors: newCheckpointWriteErrorsCounter(resourcesWithCounters(t, exp).Metrics()),
	}

	got, exported := exp.get(name)
	require.True(t, exported, "expected the counter to be exported before the first error")
	assert.Equal(t, int64(0), got)

	require.Error(t, o.cacheSCN(t.Context(), 100))
	got, _ = exp.get(name)
	assert.Equal(t, int64(1), got, "expected a failed write to be counted")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Error(t, o.cacheSCN(ctx, 200))
	got, _ = exp.get(name)
	assert.Equal(t, int64(1), got, "expected a write with a cancelled context not to be counted")
}

// failingCache is a service.Cache whose Set always fails.
type failingCache struct{ service.Cache }

func (failingCache) Set(context.Context, string, []byte, *time.Duration) error {
	return errors.New("ORA-41900: missing INSERT privilege")
}

// counterExporter records the total of every counter by name. A counter is
// in the map once it is exported, also with a total of 0.
type counterExporter struct {
	mu       sync.Mutex
	counters map[string]int64
}

func (e *counterExporter) NewCounterCtor(name string, _ ...string) service.MetricsExporterCounterCtor {
	return func(...string) service.MetricsExporterCounter { return counterHandle{e: e, name: name} }
}

func (*counterExporter) NewTimerCtor(string, ...string) service.MetricsExporterTimerCtor {
	return func(...string) service.MetricsExporterTimer { return noopExporterMetric{} }
}

func (*counterExporter) NewGaugeCtor(string, ...string) service.MetricsExporterGaugeCtor {
	return func(...string) service.MetricsExporterGauge { return noopExporterMetric{} }
}

func (*counterExporter) Close(context.Context) error { return nil }

func (e *counterExporter) add(name string, v int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.counters[name] += v
}

func (e *counterExporter) get(name string) (int64, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	v, ok := e.counters[name]
	return v, ok
}

type counterHandle struct {
	e    *counterExporter
	name string
}

func (c counterHandle) Incr(v int64) { c.e.add(c.name, v) }

func (c counterHandle) IncrFloat64(v float64) { c.Incr(int64(v)) }

type noopExporterMetric struct{}

func (noopExporterMetric) Set(int64)           {}
func (noopExporterMetric) Timing(int64)        {}
func (noopExporterMetric) IncrFloat64(float64) {}
func (noopExporterMetric) Incr(int64)          {}
func (noopExporterMetric) Decr(int64)          {}
func (noopExporterMetric) DecrFloat64(float64) {}
func (noopExporterMetric) SetFloat64(float64)  {}

// resourcesWithCounters builds resources with metrics routed to exp.
func resourcesWithCounters(t *testing.T, exp *counterExporter) *service.Resources {
	t.Helper()
	env := service.NewEnvironment()
	require.NoError(t, env.RegisterMetricsExporter("oracledb_test_counters", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Logger) (service.MetricsExporter, error) { return exp, nil }))
	b := env.NewResourceBuilder()
	require.NoError(t, b.SetMetricsYAML("oracledb_test_counters: {}"))
	res, closeFn, err := b.Build()
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn(context.Background()) })
	return res
}
