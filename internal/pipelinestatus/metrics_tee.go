// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pipelinestatus

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// messagesConsumedMetric is the metric summed into messages_consumed. It
// increments by the number of messages in each batch an input produces.
const messagesConsumedMetric = "input_received"

// inputPath is the path label of the pipeline's input component. Only that
// series counts. Every input, wrapper or not, is registered as a plugin and so
// reports every batch it produces there; a wrapping input such as `broker`
// counts the batches its children already counted under deeper paths, and a
// `metric` processor can emit a counter of the same name from anywhere else
// in the config.
const inputPath = "root.input"

// messagesConsumedTee is a metrics exporter that sums every series of the
// consumed metric into one total, records when it last moved, and ignores
// everything else. While the endpoint is disabled it ignores the consumed
// metric too, so a pipeline run without the endpoint only pays for no-op
// calls.
type messagesConsumedTee struct {
	total   *atomic.Int64
	lastAt  *atomic.Int64
	enabled *atomic.Bool
}

type consumedCounter struct {
	total   *atomic.Int64
	lastAt  *atomic.Int64
	enabled *atomic.Bool
}

// Incr is called once per batch an input produces, with the batch size, so
// the clock read is per batch rather than per message. An empty batch is not
// a message and leaves the timestamp alone. The timestamp only moves forward,
// so two inputs racing across a second boundary can't make it step back. The
// enabled check is here rather than at construction so the order of enabling
// and component construction doesn't matter.
func (c consumedCounter) Incr(count int64) {
	if count <= 0 || !c.enabled.Load() {
		return
	}
	c.total.Add(count)
	now := time.Now().Unix()
	for {
		cur := c.lastAt.Load()
		if now <= cur || c.lastAt.CompareAndSwap(cur, now) {
			return
		}
	}
}

type noopStat struct{}

func (noopStat) Incr(int64)   {}
func (noopStat) Timing(int64) {}
func (noopStat) Set(int64)    {}

func (m messagesConsumedTee) NewCounterCtor(name string, labelKeys ...string) service.MetricsExporterCounterCtor {
	noop := func(...string) service.MetricsExporterCounter { return noopStat{} }
	if name != messagesConsumedMetric {
		return noop
	}
	pathIdx := -1
	for i, k := range labelKeys {
		if k == "path" {
			pathIdx = i
		}
	}
	if pathIdx < 0 {
		return noop
	}
	return func(labelValues ...string) service.MetricsExporterCounter {
		if pathIdx >= len(labelValues) || labelValues[pathIdx] != inputPath {
			return noopStat{}
		}
		return consumedCounter(m)
	}
}

func (messagesConsumedTee) NewTimerCtor(string, ...string) service.MetricsExporterTimerCtor {
	return func(...string) service.MetricsExporterTimer { return noopStat{} }
}

func (messagesConsumedTee) NewGaugeCtor(string, ...string) service.MetricsExporterGaugeCtor {
	return func(...string) service.MetricsExporterGauge { return noopStat{} }
}

func (messagesConsumedTee) Close(context.Context) error {
	return nil
}
