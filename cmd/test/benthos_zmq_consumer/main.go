// +build ZMQ4

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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jeffail/benthos/types"
	"github.com/jeffail/benthos/util/test"
	"github.com/pebbe/zmq4"
)

//--------------------------------------------------------------------------------------------------

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

	pullSocket.SetRcvhwm(1)

	if strings.Contains(address, "*") {
		err = pullSocket.Bind(address)
	} else {
		err = pullSocket.Connect(address)
	}
	if nil != err {
		panic(err)
	}

	poller := zmq4.NewPoller()
	poller.Add(pullSocket, zmq4.POLLIN)

	wg := sync.WaitGroup{}
	wg.Add(1)

	benchChan := test.StartPrintingBenchmarks(time.Second, 0)
	<-time.After(time.Second)

	var running int32 = 1
	go func() {
		for atomic.LoadInt32(&running) == 1 {
			polled, err := poller.Poll(time.Second)
			if err == nil && len(polled) == 1 {
				if msgParts, err := pullSocket.RecvMessageBytes(0); err == nil {
					if bench, err := test.BenchFromMessage(types.Message{Parts: msgParts}); err == nil {
						benchChan <- bench
					} else {
						fmt.Printf("| Error: Wrong message format: %v\n", err)
					}
				} else {
					panic(err)
				}
			}
		}
		close(benchChan)
		<-time.After(time.Second) // Wait for one final tick.
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
