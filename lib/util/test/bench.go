/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package test

import (
	"fmt"
	"time"

	"github.com/jeffail/benthos/lib/types"
)

//--------------------------------------------------------------------------------------------------

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

//--------------------------------------------------------------------------------------------------

// Bench - A struct carrying message specific benchmarking statistics.
type Bench struct {
	Latency int64 // Time taken (ns) for a message to be received by the consumer.
	NBytes  int64 // Number of bytes carried in the message.
	Index   int32 // The index carried by the message, can be used to detect loss.
}

/*
NewBenchMessage - Create a message carrying information used to calc benchmarks on the other end of
a transport.
*/
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

// BenchFromMessage - Returns the benchmarking stats from a message received.
func BenchFromMessage(msg types.Message) (Bench, error) {
	var b Bench
	if len(msg.Parts) != 3 {
		return b, fmt.Errorf("incorrect # of message parts: %v != %v", len(msg.Parts), 3)
	}

	t := time.Time{}
	if err := t.UnmarshalBinary(msg.Parts[0]); err != nil {
		return b, err
	}

	b.Latency = int64(time.Since(t))
	b.Index = bytesToIndex(msg.Parts[1])
	for _, part := range msg.Parts {
		b.NBytes = b.NBytes + int64(len(part))
	}

	return b, nil
}

//--------------------------------------------------------------------------------------------------

/*
StartPrintingBenchmarks - Starts a goroutine that will periodically print any statistics/benchmarks
that are accumulated through messages, and also any lost interactions as per the startIndex. If you
want to disable data loss detection then set the startIndex to -1. You must provide the messages via
the write channel returned by the function.
*/
func StartPrintingBenchmarks(period time.Duration, startIndex int32) chan<- Bench {
	c := make(chan Bench, 100)

	currentIndex := startIndex
	dataLoss := []int32{}

	type statTally struct {
		Tally        int64
		StartedAt    time.Time
		TotalLatency int64   // Nanoseconds
		TotalBytes   int64   // Bytes
		AvgLatency   int64   // Nanoseconds per message
		ByteRate     float64 // Bytes per second
		MessageRate  float64 // Messages per second
	}

	tStats := statTally{StartedAt: time.Now()}
	pStats := statTally{StartedAt: time.Now()}

	updateStats := func(bench Bench) {
		pStats.Tally++
		pStats.TotalLatency = pStats.TotalLatency + bench.Latency
		pStats.TotalBytes = pStats.TotalBytes + bench.NBytes

		tStats.Tally++
		tStats.TotalLatency = tStats.TotalLatency + bench.Latency
		tStats.TotalBytes = tStats.TotalBytes + bench.NBytes
	}

	refreshPStats := func() {
		pStats = statTally{StartedAt: time.Now()}
	}

	calcStats := func() {
		if tStats.Tally > 0 {
			if tStats.Tally == 1 {
				tStats.StartedAt = time.Now()
			}
			tStats.AvgLatency = tStats.TotalLatency / tStats.Tally
			tStats.ByteRate = float64(tStats.TotalBytes) / time.Since(tStats.StartedAt).Seconds()
			tStats.MessageRate = float64(tStats.Tally) / time.Since(tStats.StartedAt).Seconds()
		}

		if pStats.Tally > 0 {
			pStats.AvgLatency = pStats.TotalLatency / pStats.Tally
			pStats.ByteRate = float64(pStats.TotalBytes) / time.Since(pStats.StartedAt).Seconds()
			pStats.MessageRate = float64(pStats.Tally) / time.Since(pStats.StartedAt).Seconds()
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
						currentIndex = bench.Index
					}
					if currentIndex != bench.Index {
						dataLoss = append(dataLoss, currentIndex)
						currentIndex = bench.Index
					}
					currentIndex++
				}
				updateStats(bench)
				calcStats()
			case <-timer.C:
				fmt.Printf("+\n")
				fmt.Printf("| THIS TICK\n")
				fmt.Printf("| Avg Latency : %v\n", time.Duration(pStats.AvgLatency))
				fmt.Printf("| Byte Rate   : %.6f MB/s\n", pStats.ByteRate/1048576)
				fmt.Printf("| Msg Rate    : %.6f msg/s\n", pStats.MessageRate)
				fmt.Printf("| Msg Count   : %v\n", pStats.Tally)
				fmt.Printf("|\n")
				fmt.Printf("| TOTAL\n")
				fmt.Printf("| Avg Latency : %v\n", time.Duration(tStats.AvgLatency))
				fmt.Printf("| Byte Rate   : %.6f MB/s\n", tStats.ByteRate/1048576)
				fmt.Printf("| Msg Rate    : %.6f msg/s\n", tStats.MessageRate)
				fmt.Printf("| Msg Count   : %v\n", tStats.Tally)
				if len(dataLoss) > 0 {
					if len(dataLoss) > 100 {
						fmt.Printf("|\n")
						fmt.Printf("| DATA LOST   : %v msgs\n", len(dataLoss))
					} else {
						fmt.Printf("|\n")
						fmt.Printf("| DATA LOST   : %v\n", dataLoss)
					}
				}
				fmt.Printf("+\n\n")
				refreshPStats()
			}
		}
	}()

	return c
}

//--------------------------------------------------------------------------------------------------
