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

	var avg, total, tally int64
	readChan := make(chan int64)

	var running int32 = 1

	go func() {
		timer := time.NewTicker(time.Second)
		defer timer.Stop()

		secTally := 0

		for atomic.LoadInt32(&running) == 1 {
			select {
			case t := <-readChan:
				total = total + t
				tally = tally + 1
				secTally = secTally + 1
				avg = total / tally
			case <-timer.C:
				fmt.Printf("+\n")
				fmt.Printf("| Average    : %v\n", time.Duration(avg))
				fmt.Printf("| Tally      : %v\n", tally)
				fmt.Printf("| Per second : %v\n", secTally)
				fmt.Printf("+\n\n")
				secTally = 0
			}
		}
		wg.Done()
	}()

	go func() {
		for atomic.LoadInt32(&running) == 1 {
			polled, err := poller.Poll(time.Second)
			if err == nil && len(polled) == 1 {
				if bytes, err := pullSocket.RecvMessageBytes(0); err == nil {
					t := time.Now()
					if err = t.UnmarshalBinary(bytes[0]); err == nil {
						readChan <- int64(time.Since(t))
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
