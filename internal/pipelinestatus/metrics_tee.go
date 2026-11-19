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
// consumed metric into one total and ignores everything else. While the
// endpoint is disabled it ignores the consumed metric too, so a pipeline run
// without the endpoint only pays for no-op calls.
type messagesConsumedTee struct {
	total   *atomic.Int64
	enabled *atomic.Bool
}

type consumedCounter struct {
	total *atomic.Int64
}

func (c consumedCounter) Incr(count int64) {
	c.total.Add(count)
}

type noopStat struct{}

func (noopStat) Incr(int64)   {}
func (noopStat) Timing(int64) {}
func (noopStat) Set(int64)    {}

func (m messagesConsumedTee) NewCounterCtor(name string, labelKeys ...string) service.MetricsExporterCounterCtor {
	noop := func(...string) service.MetricsExporterCounter { return noopStat{} }
	if name != messagesConsumedMetric || !m.enabled.Load() {
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
		return consumedCounter{total: m.total}
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
