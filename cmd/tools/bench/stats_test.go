// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStatsLine(t *testing.T) {
	tests := []struct {
		name   string
		line   string
		window string
		msg    float64
		mb     float64
	}{
		{
			name:   "logfmt prefers the exact fields",
			line:   `INFO rolling stats: 99000 msg/sec, 204 MB/sec    @service=redpanda-connect bytes/sec=2.03882848e+08 label="" msg/sec=99123 path=root.output.processors.0`,
			window: "rolling", msg: 99123, mb: 203.882848,
		},
		{
			name:   "json prefers the exact fields",
			line:   `{"@service":"redpanda-connect","bytes/sec":1500000,"level":"info","msg":"rolling stats: 12 msg/sec, 1.5 MB/sec","msg/sec":12.5}`,
			window: "rolling", msg: 12.5, mb: 1.5,
		},
		{
			name:   "message text only",
			line:   `rolling stats: 1 msg/sec, 407 B/sec`,
			window: "rolling", msg: 1, mb: 0.000407,
		},
		{
			name:   "kilobytes",
			line:   `rolling stats: 300 msg/sec, 500 kB/sec`,
			window: "rolling", msg: 300, mb: 0.5,
		},
		{
			name:   "gigabytes",
			line:   `rolling stats: 1e+06 msg/sec, 1.1 GB/sec`,
			window: "rolling", msg: 1e6, mb: 1100,
		},
		{
			name:   "count_bytes disabled",
			line:   `INFO rolling stats: 4200 msg/sec    @service=redpanda-connect msg/sec=4200`,
			window: "rolling", msg: 4200, mb: 0,
		},
		{
			name:   "total",
			line:   `INFO total stats: 40906 msg/sec, 85 MB/sec    bytes/sec=8.468e+07 msg/sec=40906.89`,
			window: "total", msg: 40906.89, mb: 84.68,
		},
		{
			name: "another line",
			line: `INFO Input type aws_dynamodb_cdc is now active`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			window, msg, mb := parseStatsLine(test.line)
			assert.Equal(t, test.window, window)
			assert.InDelta(t, test.msg, msg, 1e-9)
			assert.InDelta(t, test.mb, mb, 1e-9)
		})
	}
}

func TestSummariseUsesActiveWindow(t *testing.T) {
	log := strings.Join([]string{
		"INFO starting",
		"rolling stats: 0 msg/sec, 0 B/sec",
		"rolling stats: 0 msg/sec, 0 B/sec",
		"rolling stats: 100 msg/sec, 1 MB/sec",
		"rolling stats: 0 msg/sec, 0 B/sec",
		"rolling stats: 300 msg/sec, 3 MB/sec",
		"rolling stats: 200 msg/sec, 2 MB/sec",
		"rolling stats: 0 msg/sec, 0 B/sec",
		"rolling stats: 0 msg/sec, 0 B/sec",
		"total stats: 75 msg/sec, 750 kB/sec",
	}, "\n")

	stats, err := parseStats(strings.NewReader(log))
	require.NoError(t, err)
	require.Len(t, stats.samples, 8)
	assert.Equal(t, 7, stats.samples[7].T)
	assert.Equal(t, &Total{MsgPerSec: 75, MBPerSec: 0.75}, stats.total)
	assert.Equal(t, 2, stats.trailingIdle())

	// The two warm-up samples and the two idle ones at the end are left out,
	// the stall in the middle is not.
	s := summarise(stats.samples)
	assert.Equal(t, 4, s.Samples)
	assert.InDelta(t, 150, s.MedianMsgPerSec, 1e-9)
	assert.InDelta(t, 150, s.MeanMsgPerSec, 1e-9)
	assert.InDelta(t, 300, s.PeakMsgPerSec, 1e-9)
	assert.InDelta(t, 0, s.P5MsgPerSec, 1e-9)
	assert.InDelta(t, 1.5, s.MedianMBPerSec, 1e-9)
	assert.InDelta(t, 3, s.PeakMBPerSec, 1e-9)
}

func TestSummariseWithoutMessages(t *testing.T) {
	var c statsCollector
	assert.True(t, c.add("rolling stats: 0 msg/sec, 0 B/sec"))
	assert.True(t, c.add("rolling stats: 0 msg/sec, 0 B/sec"))
	assert.False(t, c.add("something else"))

	// A suite that has not seen a message yet is starting up, not idle.
	assert.Zero(t, c.trailingIdle())
	assert.Equal(t, Summary{}, summarise(c.samples))
	assert.Equal(t, Summary{}, summarise(nil))
}
