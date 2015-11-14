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
	interval := time.Millisecond * 1

	ctx, err := zmq4.NewContext()
	if nil != err {
		panic(err)
	}

	pushSocket, err := ctx.NewSocket(zmq4.PUSH)
	if nil != err {
		panic(err)
	}
	pushSocket.Connect("tcp://localhost:1234")

	pullSocket, err := ctx.NewSocket(zmq4.PULL)
	if nil != err {
		panic(err)
	}
	pullSocket.Connect("tcp://localhost:1235")

	poller := zmq4.NewPoller()
	poller.Add(pullSocket, zmq4.POLLIN)

	wg := sync.WaitGroup{}
	wg.Add(3)

	var avg, total, tally int64
	readChan := make(chan int64)

	var running int32 = 1

	go func() {
		for atomic.LoadInt32(&running) == 1 {
			<-time.After(interval)
			nowBytes, err := time.Now().MarshalBinary()
			if err != nil {
				panic(err)
			}

			_, err = pushSocket.SendBytes(nowBytes, 0)
			if err != nil {
				panic(err)
			}
		}
		wg.Done()
	}()

	go func() {
		timer := time.NewTicker(time.Second * 5)
		defer timer.Stop()

		for atomic.LoadInt32(&running) == 1 {
			select {
			case t := <-readChan:
				total = total + t
				tally = tally + 1
				avg = total / tally
			case <-timer.C:
				fmt.Printf("+\n")
				fmt.Printf("| Average : %v\n", time.Duration(avg))
				fmt.Printf("| Tally   : %v\n", tally)
				fmt.Printf("+\n\n")
			}
		}
		wg.Done()
	}()

	go func() {
		for atomic.LoadInt32(&running) == 1 {
			polled, err := poller.Poll(time.Second * 5)
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
		wg.Done()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case <-sigChan:
	}

	atomic.StoreInt32(&running, 0)
	wg.Wait()
}

//--------------------------------------------------------------------------------------------------
