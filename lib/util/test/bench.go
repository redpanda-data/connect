// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package test

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

// Bench is a struct carrying message specific benchmarking statistics.
type Bench struct {
	Latency int    // Time taken (ns) for a message to be received by a consumer
	NBytes  int    // Number of bytes carried in the message
	Index   uint64 // The index carried by a message, can be used to detect loss
}

// BenchFromMessage returns the benchmarking stats from a message received.
func BenchFromMessage(msg types.Message) (Bench, error) {
	var b Bench
	for _, part := range msg.GetAll() {
		b.NBytes = b.NBytes + int(len(part))
	}

	var err error

	// If more than one part we assume the first part is a timestamp.
	if msg.Len() > 1 {
		var tsNano int64
		if tsNano, err = strconv.ParseInt(string(msg.Get(0)), 10, 64); err != nil {
			return b, err
		}
		t := time.Unix(0, tsNano)
		b.Latency = int(time.Since(t))
	}

	// If more than two parts we assume the second part is a message index.
	if msg.Len() > 2 {
		if b.Index, err = strconv.ParseUint(string(msg.Get(1)), 10, 64); err != nil {
			return b, err
		}
	}

	return b, nil
}

//------------------------------------------------------------------------------

// StartPrintingBenchmarks starts a goroutine that will periodically print any
// statistics/benchmarks that are accumulated through messages. You must provide
// the messages via the write channel returned by the function.
func StartPrintingBenchmarks(period time.Duration) chan<- Bench {
	c := make(chan Bench, 100)

	var currentIndex uint64 = 1 // First message is expected to be 1.
	dataMissing := 0

	type statTally struct {
		startedAt    time.Time
		latencies    []int
		totalLatency int
		totalBytes   int

		Tally                int     `json:"total"`
		MeanLatency          int     `json:"mean_latency_ns"`
		MeanLatencyStr       string  `json:"mean_latency"`
		PercentileLatency    int     `json:"99%_latency_ns"`
		PercentileLatencyStr string  `json:"99%_latency"`
		ByteRate             float64 `json:"bytes_per_s"`
		MessageRate          float64 `json:"msgs_per_s"`
	}

	pStats := statTally{startedAt: time.Now()}

	updateStats := func(bench Bench) {
		pStats.Tally++
		pStats.latencies = append(pStats.latencies, bench.Latency)
		pStats.totalLatency = pStats.totalLatency + bench.Latency
		pStats.totalBytes = pStats.totalBytes + bench.NBytes
	}

	refreshPStats := func() {
		pStats = statTally{startedAt: time.Now()}
	}

	calcStats := func() {
		if pStats.Tally > 0 {
			pStats.MeanLatency = pStats.totalLatency / pStats.Tally
			pStats.MeanLatencyStr = time.Duration(pStats.MeanLatency).String()
			pStats.ByteRate = float64(pStats.totalBytes) / time.Since(pStats.startedAt).Seconds()
			pStats.MessageRate = float64(pStats.Tally) / time.Since(pStats.startedAt).Seconds()

			// Calc 99th percentile
			index := int(math.Ceil(0.99 * float64(pStats.Tally)))
			sort.Ints(pStats.latencies)
			if index < len(pStats.latencies) {
				pStats.PercentileLatency = pStats.latencies[index]
				pStats.PercentileLatencyStr = time.Duration(pStats.PercentileLatency).String()
			}
		}
	}

	go func() {
		timer := time.NewTicker(period)
		defer timer.Stop()
		for {
			select {
			case bench, open := <-c:
				if !open {
					return
				}
				if bench.Index > 0 {
					if bench.Index == currentIndex {
						currentIndex = bench.Index + 1
					} else if bench.Index < currentIndex {
						dataMissing--
					} else {
						for i := currentIndex; i < bench.Index; i++ {
							dataMissing++
						}
						currentIndex = bench.Index + 1
					}
				}
				updateStats(bench)
			case <-timer.C:
				calcStats()
				blob, err := json.Marshal(pStats)
				if err != nil {
					fmt.Println(err)
				}
				fmt.Println(string(blob))
				if dataMissing > 0 {
					fmt.Printf("{\"data_missing\":%v}\n", dataMissing)
				}
				refreshPStats()
			}
		}
	}()

	return c
}

//------------------------------------------------------------------------------
