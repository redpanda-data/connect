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
	"time"

	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func bytesToIndex(b []byte) (index int32) {
	if len(b) <= 3 {
		return index
	}
	index = int32(b[0])<<24 |
		int32(b[1])<<16 |
		int32(b[2])<<8 |
		int32(b[3])
	return index
}

func indexToBytes(index int32) (b [4]byte) {
	b[0] = byte(index >> 24)
	b[1] = byte(index >> 16)
	b[2] = byte(index >> 8)
	b[3] = byte(index)

	return b
}

//------------------------------------------------------------------------------

// Bench is a struct carrying message specific benchmarking statistics.
type Bench struct {
	Latency int   // Time taken (ns) for a message to be received by a consumer.
	NBytes  int   // Number of bytes carried in the message.
	Index   int32 // The index carried by a message, can be used to detect loss.
}

// NewBenchMessage creates a message carrying information used to calc
// benchmarks on the other end of a transport.
func NewBenchMessage(index int32, dataBlob []byte) types.Message {
	msg := types.Message{
		Parts: make([][]byte, 3),
	}

	indexBytes := indexToBytes(index)

	msg.Parts[1] = indexBytes[0:4]
	msg.Parts[2] = dataBlob

	var err error
	msg.Parts[0], err = time.Now().MarshalBinary()
	if err != nil {
		panic(err)
	}

	return msg
}

// BenchFromMessage returns the benchmarking stats from a message received.
func BenchFromMessage(msg types.Message) (Bench, error) {
	var b Bench
	if len(msg.Parts) < 2 {
		return b, fmt.Errorf("Benchmark requires at least 2 message parts, received: %v", len(msg.Parts))
	}

	t := time.Time{}
	if err := t.UnmarshalBinary(msg.Parts[0]); err != nil {
		return b, err
	}

	b.Latency = int(time.Since(t))
	b.Index = bytesToIndex(msg.Parts[1])
	for _, part := range msg.Parts {
		b.NBytes = b.NBytes + int(len(part))
	}

	return b, nil
}

//------------------------------------------------------------------------------

// StartPrintingBenchmarks starts a goroutine that will periodically print any
// statistics/benchmarks that are accumulated through messages, and also any
// lost interactions as per the startIndex. If you want to disable data loss
// detection then set the startIndex to -1. You must provide the messages via
// the write channel returned by the function.
func StartPrintingBenchmarks(period time.Duration, startIndex int32) chan<- Bench {
	c := make(chan Bench, 100)

	currentIndex := startIndex
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
				if startIndex != -1 {
					if startIndex == bench.Index {
						// Indicates that the producer index has been restarted.
						dataMissing = 0
						currentIndex = bench.Index
					}
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
