// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package otlp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsConfigSpec(t *testing.T) {
	spec := otlpMetricsSpec()
	pConf, err := spec.ParseYAML(`
http:
  - address: localhost:4318
grpc:
  - address: localhost:4317
    secure: true
tags:
  env: prod
service: my-service
`, nil)
	require.NoError(t, err)

	serviceName, err := pConf.FieldString("service")
	require.NoError(t, err)
	assert.Equal(t, "my-service", serviceName)

	tags, err := pConf.FieldStringMap("tags")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, tags)

	httpCollectors, err := parseCollectors(pConf, "http")
	require.NoError(t, err)
	require.Len(t, httpCollectors, 1)
	assert.Equal(t, "localhost:4318", httpCollectors[0].address)
	assert.False(t, httpCollectors[0].secure)

	grpcCollectors, err := parseCollectors(pConf, "grpc")
	require.NoError(t, err)
	require.Len(t, grpcCollectors, 1)
	assert.Equal(t, "localhost:4317", grpcCollectors[0].address)
	assert.True(t, grpcCollectors[0].secure)
}

func TestMetricsCounter(t *testing.T) {
	m := newTestMetrics(t)
	t.Cleanup(func() { require.NoError(t, m.Close(context.Background())) })

	ctr := m.NewCounterCtor("test_counter")()
	require.NotNil(t, ctr)
	ctr.Incr(1)
	ctr.Incr(5)
}

func TestMetricsCounterWithLabels(t *testing.T) {
	m := newTestMetrics(t)
	t.Cleanup(func() { require.NoError(t, m.Close(context.Background())) })

	ctor := m.NewCounterCtor("test_counter_labels", "method", "status")
	c1 := ctor("GET", "200")
	c2 := ctor("POST", "500")
	require.NotNil(t, c1)
	require.NotNil(t, c2)
	c1.Incr(1)
	c2.Incr(2)
}

func TestMetricsTimer(t *testing.T) {
	m := newTestMetrics(t)
	t.Cleanup(func() { require.NoError(t, m.Close(context.Background())) })

	tmr := m.NewTimerCtor("test_timer")()
	require.NotNil(t, tmr)
	tmr.Timing(1000)
	tmr.Timing(2000)
}

func TestMetricsTimerWithLabels(t *testing.T) {
	m := newTestMetrics(t)
	t.Cleanup(func() { require.NoError(t, m.Close(context.Background())) })

	ctor := m.NewTimerCtor("test_timer_labels", "endpoint")
	t1 := ctor("/api/v1")
	t2 := ctor("/api/v2")
	require.NotNil(t, t1)
	require.NotNil(t, t2)
	t1.Timing(100)
	t2.Timing(200)
}

func TestMetricsGauge(t *testing.T) {
	m := newTestMetrics(t)
	t.Cleanup(func() { require.NoError(t, m.Close(context.Background())) })

	gge := m.NewGaugeCtor("test_gauge")()
	require.NotNil(t, gge)
	gge.Set(42)
	gge.Set(0)
	gge.Set(-10)
}

func TestMetricsGaugeWithLabels(t *testing.T) {
	m := newTestMetrics(t)
	t.Cleanup(func() { require.NoError(t, m.Close(context.Background())) })

	ctor := m.NewGaugeCtor("test_gauge_labels", "pool")
	g1 := ctor("primary")
	g2 := ctor("secondary")
	require.NotNil(t, g1)
	require.NotNil(t, g2)
	g1.Set(100)
	g2.Set(50)
}

func TestMetricsHandlerFunc(t *testing.T) {
	m := newTestMetrics(t)
	t.Cleanup(func() { require.NoError(t, m.Close(context.Background())) })

	assert.Nil(t, m.HandlerFunc())
}

func TestMetricsClose(t *testing.T) {
	m := newTestMetrics(t)
	require.NoError(t, m.Close(t.Context()))
}

func TestZipAttrs(t *testing.T) {
	attrs := zipAttrs([]string{"a", "b", "c"}, []string{"1", "2", "3"})
	require.Len(t, attrs, 3)
	assert.Equal(t, "a", string(attrs[0].Key))
	assert.Equal(t, "1", attrs[0].Value.AsString())
	assert.Equal(t, "c", string(attrs[2].Key))
	assert.Equal(t, "3", attrs[2].Value.AsString())
}

func TestZipAttrsMismatchedLengths(t *testing.T) {
	attrs := zipAttrs([]string{"a", "b"}, []string{"1"})
	require.Len(t, attrs, 1)
	assert.Equal(t, "a", string(attrs[0].Key))
}

func TestZipAttrsEmpty(t *testing.T) {
	attrs := zipAttrs(nil, nil)
	assert.Empty(t, attrs)
}

// newTestMetrics creates a metrics exporter with no real exporters (no collectors configured),
// so it won't attempt network connections but still exercises all instrument creation paths.
func newTestMetrics(t *testing.T) *otlpMetrics {
	t.Helper()

	spec := otlpMetricsSpec()
	pConf, err := spec.ParseYAML(`
http: []
grpc: []
service: test-service
`, nil)
	require.NoError(t, err)

	m, err := newOtlpMetrics(pConf, nil)
	require.NoError(t, err)
	return m
}
