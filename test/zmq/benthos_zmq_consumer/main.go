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

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pebbe/zmq4"
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

type msgStat struct {
	latency int64 // Time taken for a message to leave the producer and be received by the consumer.
	nBytes  int64 // Number of bytes carried in the message.
}

func main() {
	var address string
	flag.StringVar(&address, "addr", "tcp://localhost:1235", "Address of the benthos server")

	flag.Parse()

	fmt.Println("This is a benchmarking utility for benthos.")
	fmt.Println("Make sure you are running benthos with the ./test/zmq.yaml config.")

	ctx, err := zmq4.NewContext()
	if nil != err {
		panic(err)
	}

	pullSocket, err := ctx.NewSocket(zmq4.PULL)
	if nil != err {
		panic(err)
	}
	pullSocket.Connect(address)

	poller := zmq4.NewPoller()
	poller.Add(pullSocket, zmq4.POLLIN)

	wg := sync.WaitGroup{}
	wg.Add(2)

	readChan := make(chan msgStat)

	var running, index, dataLost int32 = 1, 0, 0

	go func() {
		timer := time.NewTicker(time.Second)
		defer timer.Stop()

		var total, tally, avg, secTotal, secTally, secNBytes, secAvg int64

		for atomic.LoadInt32(&running) == 1 {
			select {
			case stat := <-readChan:
				// Total stats
				total = total + stat.latency
				tally = tally + 1
				avg = total / tally

				// Per second stats
				secTotal = secTotal + stat.latency
				secTally = secTally + 1
				secNBytes = secNBytes + stat.nBytes
				secAvg = secTotal / secTally
			case <-timer.C:
				fmt.Printf("+\n")
				fmt.Printf("| THIS SECOND\n")
				fmt.Printf("| Avg Latency : %v\n", time.Duration(secAvg))
				fmt.Printf("| Byte Rate   : %.6f MB/s\n", float64(secNBytes)/1048576)
				fmt.Printf("| Msg Count   : %v\n", secTally)
				fmt.Printf("|\n")
				fmt.Printf("| TOTAL\n")
				fmt.Printf("| Avg Latency : %v\n", time.Duration(avg))
				fmt.Printf("| Msg Count   : %v\n", tally)
				if lost := atomic.LoadInt32(&dataLost); lost != 0 {
					fmt.Printf("|\n")
					fmt.Printf("| DATA LOST   : %v\n", lost)
				}
				fmt.Printf("+\n\n")
				secTotal = 0
				secTally = 0
				secNBytes = 0
				secAvg = 0
			}
		}
		wg.Done()
	}()

	go func() {
		for atomic.LoadInt32(&running) == 1 {
			polled, err := poller.Poll(time.Second)
			if err == nil && len(polled) == 1 {
				if msgParts, err := pullSocket.RecvMessageBytes(0); err == nil {
					if len(msgParts) < 2 {
						panic(fmt.Errorf("Expected message of at least two parts: %v", len(msgParts)))
					}
					i := bytesToIndex(msgParts[1])
					if i != 1 && index != 0 && i != index+1 {
						fmt.Printf("| DATA LOSS   : rcvd %v, exp %v, actual %v\n", i, index, msgParts[1])
						atomic.AddInt32(&dataLost, 1)
					}
					index = i
					t := time.Now()
					if err = t.UnmarshalBinary(msgParts[0]); err == nil {
						var totalBytes int64
						for _, part := range msgParts {
							totalBytes = totalBytes + int64(len(part))
						}
						readChan <- msgStat{
							latency: int64(time.Since(t)),
							nBytes:  totalBytes,
						}
					} else {
						panic(err)
					}
				} else {
					panic(err)
				}
			}
		}
		close(readChan)
		wg.Done()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case <-sigChan:
	}

	atomic.StoreInt32(&running, 0)
	go func() {
		<-time.After(time.Second * 10)
		panic(errors.New("Timed out waiting for processes to end"))
	}()
	wg.Wait()
}

//--------------------------------------------------------------------------------------------------
