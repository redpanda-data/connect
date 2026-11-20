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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// messagesConsumedMetric is the metric summed into messages_consumed. It
// increments by the number of messages in each batch an input produces.
const messagesConsumedMetric = "input_received"

// inputPendingMetric is the gauge an input sets to the number of messages it
// knows remain at its source. The convention for inputs: a plain gauge under
// this name with no labels of its own, set whenever the estimate changes, left
// alone while the estimate is unknown, and set to zero when the input stops
// consuming. The sum of every series is messages_remaining. A wrapping input
// leaves it to its children.
const inputPendingMetric = "input_pending"

// inputPath is the path label of the pipeline's input component, and
// inputPathPrefix that of every component nested inside it, which includes
// the processors attached to an input under a processorsSegment. Every input,
// wrapper or not, is registered as a plugin and so reports every batch it
// produces at inputPath; a wrapping input such as `broker` counts the batches
// its children already counted under deeper paths.
const (
	inputPath         = "root.input"
	inputPathPrefix   = inputPath + "."
	processorsSegment = ".processors."
)

// inputSeriesFilter returns a predicate over a series' label values that
// accepts only series from input components, the top-level one alone when
// topLevelOnly is set, or nil when the label keys carry no path, in which case
// nothing can be told apart. A `metric` processor can emit a counter or gauge
// of any name from anywhere in the config, including the processors attached
// to an input, which is what the path rules out.
func inputSeriesFilter(labelKeys []string, topLevelOnly bool) func(labelValues []string) bool {
	pathIdx := -1
	for i, k := range labelKeys {
		if k == "path" {
			pathIdx = i
		}
	}
	if pathIdx < 0 {
		return nil
	}
	return func(labelValues []string) bool {
		if pathIdx >= len(labelValues) {
			return false
		}
		p := labelValues[pathIdx]
		if p == inputPath {
			return true
		}
		return !topLevelOnly && strings.HasPrefix(p, inputPathPrefix) && !strings.Contains(p, processorsSegment)
	}
}

// pendingSeries holds the latest value of every input_pending series, keyed
// by its label values, and remembers the labels so a purge can match them.
type pendingSeries struct {
	mut    sync.Mutex
	series map[string]pendingEntry
}

type pendingEntry struct {
	labels map[string]string
	value  int64
}

func (p *pendingSeries) set(key string, labels map[string]string, v int64) {
	p.mut.Lock()
	defer p.mut.Unlock()
	if p.series == nil {
		p.series = map[string]pendingEntry{}
	}
	p.series[key] = pendingEntry{labels: labels, value: v}
}

// purge drops every series whose labels include all the given ones. benthos
// calls this when a stream is removed in streams mode, so a deleted stream's
// backlog stops counting.
func (p *pendingSeries) purge(labels map[string]string) {
	p.mut.Lock()
	defer p.mut.Unlock()
	for key, e := range p.series {
		matches := true
		for k, v := range labels {
			if e.labels[k] != v {
				matches = false
				break
			}
		}
		if matches {
			delete(p.series, key)
		}
	}
}

// total sums the latest value of every series, and is false when no input
// has set one.
func (p *pendingSeries) total() (int64, bool) {
	p.mut.Lock()
	defer p.mut.Unlock()
	var sum int64
	for _, e := range p.series {
		sum += e.value
	}
	return sum, len(p.series) > 0
}

// pendingGauge is one input_pending series as seen by the tee.
type pendingGauge struct {
	key     string
	labels  map[string]string
	series  *pendingSeries
	enabled *atomic.Bool
}

func (g pendingGauge) Set(v int64) {
	if !g.enabled.Load() {
		return
	}
	g.series.set(g.key, g.labels, v)
}

// messagesConsumedTee is a metrics exporter that sums every series of the
// consumed metric into one total, keeps the latest value of every pending
// series, and ignores everything else. While the endpoint is disabled it
// ignores those too, so a pipeline run without the endpoint only pays for
// no-op calls.
type messagesConsumedTee struct {
	total   *atomic.Int64
	lastAt  *atomic.Int64
	pending *pendingSeries
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
	fromInput := inputSeriesFilter(labelKeys, true)
	if fromInput == nil {
		return noop
	}
	return func(labelValues ...string) service.MetricsExporterCounter {
		if !fromInput(labelValues) {
			return noopStat{}
		}
		return consumedCounter{total: m.total, lastAt: m.lastAt, enabled: m.enabled}
	}
}

func (messagesConsumedTee) NewTimerCtor(string, ...string) service.MetricsExporterTimerCtor {
	return func(...string) service.MetricsExporterTimer { return noopStat{} }
}

func (m messagesConsumedTee) NewGaugeCtor(name string, labelKeys ...string) service.MetricsExporterGaugeCtor {
	noop := func(...string) service.MetricsExporterGauge { return noopStat{} }
	if name != inputPendingMetric {
		return noop
	}
	fromInput := inputSeriesFilter(labelKeys, false)
	if fromInput == nil {
		return noop
	}
	return func(labelValues ...string) service.MetricsExporterGauge {
		if !fromInput(labelValues) {
			return noopStat{}
		}
		labels := make(map[string]string, len(labelKeys))
		for i, k := range labelKeys {
			if i < len(labelValues) {
				labels[k] = labelValues[i]
			}
		}
		return pendingGauge{key: strings.Join(labelValues, "\x00"), labels: labels, series: m.pending, enabled: m.enabled}
	}
}

// DeleteSeriesPartialMatch drops the pending series of components whose labels
// include all the given ones. benthos calls it when a stream is removed in
// streams mode.
func (m messagesConsumedTee) DeleteSeriesPartialMatch(labels map[string]string) {
	m.pending.purge(labels)
}

func (messagesConsumedTee) Close(context.Context) error {
	return nil
}
