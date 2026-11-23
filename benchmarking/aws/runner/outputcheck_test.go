// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// promFrame is one framed /metrics snapshot in the shape Connect's
// prometheus exporter writes: counters carry label and path labels.
func promFrame(ts int, sent, errs, bench float64) string {
	return fmt.Sprintf(`###timestamp=%d
# HELP output_sent Benthos Counter metric
# TYPE output_sent counter
output_sent{label="",path="root.output"} %g
# HELP output_error Benthos Counter metric
# TYPE output_error counter
output_error{label="",path="root.output"} %g
# HELP benchmark_messages_total Benthos Counter metric
# TYPE benchmark_messages_total counter
benchmark_messages_total{label="",path="root.output.processors.1"} %g
benchmark_messages_total{label="",path="root.output.processors.0"} 1
benchmark_messages_total{label="",path="root.pipeline.processors.0"} 99999999
go_goroutines 312
process_resident_memory_bytes 1.45e+08
`, ts, sent, errs, bench)
}

func TestParseOutputCounters(t *testing.T) {
	raw := promFrame(100, 10, 0, 12) + promFrame(110, 5000, 3, 5100) +
		// A truncated final frame (no RSS) and an errored one are ignored.
		"###timestamp=120\noutput_sent{label=\"\",path=\"root.output\"} 9\n" +
		"###timestamp=130\n###scrape_error\n"
	c := parseOutputCounters([]byte(raw))
	require.Equal(t, outputCounters{Sent: 5000, HasSent: true, Errors: 3, HasErrors: true, Processed: 5100, HasProcessed: true}, c)
}

func TestParseOutputCountersAbsent(t *testing.T) {
	raw, err := os.ReadFile("testdata/prom-sample.txt")
	require.NoError(t, err)
	c := parseOutputCounters(append([]byte("###timestamp=1\n"), raw...))
	require.False(t, c.HasSent)
	require.False(t, c.HasErrors)
	require.True(t, c.HasProcessed, "the fixture's unlabelled benchmark counter is the harness one")
	require.InDelta(t, 3.25e6, c.Processed, 1)
	require.Equal(t, outputCounters{}, parseOutputCounters(nil))
}

func TestParseOutputCountersFanOutBroker(t *testing.T) {
	raw := `###timestamp=1
output_sent{label="",path="root.output.broker.outputs.0"} 100
output_sent{label="",path="root.output.broker.outputs.1"} 100
output_error{label="",path="root.output.broker.outputs.0"} 1
output_error{label="",path="root.output.broker.outputs.1"} 2
process_resident_memory_bytes 1
`
	c := parseOutputCounters([]byte(raw))
	require.False(t, c.HasSent, "children of a broker: no single output count")
	require.True(t, c.HasErrors)
	require.InDelta(t, 3, c.Errors, 0)
}

func kinds(as []Anomaly) []string {
	var out []string
	for _, a := range as {
		out = append(out, a.Kind)
	}
	return out
}

func TestE2EPointAnomalies(t *testing.T) {
	ok := outputCounters{Sent: 100000, HasSent: true, HasErrors: true, Processed: 100500, HasProcessed: true}
	cases := []struct {
		name     string
		p        e2ePoint
		want     []string
		wantFail bool
	}{
		{"clean", e2ePoint{Samples: 600, Counters: ok, MedianMsgPerSec: 1000}, nil, false},
		{"no samples", e2ePoint{Samples: 0, Counters: ok}, []string{anomalyNoSamples}, true},
		// 1% of 100000 sent is 1000 errors: at or under it passes with a caveat.
		{
			"few errors are retries",
			e2ePoint{Samples: 600, Counters: outputCounters{Sent: 100000, HasSent: true, Errors: 4, HasErrors: true, Processed: 100000, HasProcessed: true}, MedianMsgPerSec: 1000},
			[]string{anomalySinkRetried},
			false,
		},
		{
			"errors at the limit",
			e2ePoint{Samples: 600, Counters: outputCounters{Sent: 100000, HasSent: true, Errors: 1000, HasErrors: true, Processed: 100000, HasProcessed: true}, MedianMsgPerSec: 1000},
			[]string{anomalySinkRetried},
			false,
		},
		{
			"errors over the limit",
			e2ePoint{Samples: 600, Counters: outputCounters{Sent: 100000, HasSent: true, Errors: 1001, HasErrors: true, Processed: 100000, HasProcessed: true}, MedianMsgPerSec: 1000},
			[]string{anomalySinkErrors},
			true,
		},
		{
			"errors judged against processed without output_sent",
			e2ePoint{Samples: 600, SinkCounted: true, Counters: outputCounters{Errors: 5, HasErrors: true, Processed: 100000, HasProcessed: true}, MedianMsgPerSec: 1000},
			[]string{anomalySinkRetried},
			false,
		},
		{
			"errors with nothing to compare fail",
			e2ePoint{Samples: 600, SinkCounted: true, Counters: outputCounters{Errors: 5, HasErrors: true}, MedianMsgPerSec: 1000},
			[]string{anomalySinkErrors},
			true,
		},
		{
			"sent short of processed",
			e2ePoint{Samples: 600, Counters: outputCounters{Sent: 50000, HasSent: true, HasErrors: true, Processed: 100000, HasProcessed: true}, MedianMsgPerSec: 1000},
			[]string{anomalySinkSendMismatch},
			true,
		},
		// 2% of 100000 is 2000; 3 s of 1000 msg/s is 3000: 2900 is inside.
		{"within floor tolerance", e2ePoint{Samples: 600, Counters: outputCounters{Sent: 97100, HasSent: true, HasErrors: true, Processed: 100000, HasProcessed: true}, MedianMsgPerSec: 1000}, nil, false},
		{
			"just over tolerance",
			e2ePoint{Samples: 600, Counters: outputCounters{Sent: 96900, HasSent: true, HasErrors: true, Processed: 100000, HasProcessed: true}, MedianMsgPerSec: 1000},
			[]string{anomalySinkSendMismatch},
			true,
		},
		{"unverified", e2ePoint{Samples: 600, MedianMsgPerSec: 1000}, []string{anomalySinkUnverified}, false},
		{"sink count covers missing output_sent", e2ePoint{Samples: 600, SinkCounted: true, MedianMsgPerSec: 1000}, nil, false},
		{"drop sink needs no verification", e2ePoint{Samples: 600, SinkDrop: true, MedianMsgPerSec: 1000}, nil, false},
		{"workload bound", e2ePoint{Samples: 600, Counters: ok, MedianMsgPerSec: 960, WorkloadRatePerSec: 1000}, []string{anomalyWorkloadBound}, false},
		{"under workload", e2ePoint{Samples: 600, Counters: ok, MedianMsgPerSec: 940, WorkloadRatePerSec: 1000}, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, fail := e2ePointAnomalies(tc.p)
			require.Equal(t, tc.want, kinds(got))
			require.Equal(t, tc.wantFail, fail)
			for _, a := range got {
				require.True(t, strings.HasPrefix(a.Note, a.Kind+": "), a.Note)
			}
		})
	}
}

func TestE2EWorkloadRate(t *testing.T) {
	w := &WorkloadSpec{WriteRatePerSec: 5000}
	require.InDelta(t, 5000, e2eWorkloadRate(&Scenario{Direction: DirectionE2E, Stack: "postgres", Workload: w}), 0)
	require.InDelta(t, 5000, e2eWorkloadRate(&Scenario{Direction: DirectionE2E, Stack: "mysql", Workload: w}), 0)
	require.Zero(t, e2eWorkloadRate(&Scenario{Direction: DirectionE2E, Stack: stackNone, Workload: w}))
	require.Zero(t, e2eWorkloadRate(&Scenario{Stack: "postgres", Workload: w}), "source scenarios are not judged")
	require.Zero(t, e2eWorkloadRate(&Scenario{Direction: DirectionE2E, Stack: "postgres"}))
}
