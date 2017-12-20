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
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/pull"
	"github.com/go-mangos/mangos/protocol/rep"
	"github.com/go-mangos/mangos/transport/ipc"
	"github.com/go-mangos/mangos/transport/tcp"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/test"
)

//--------------------------------------------------------------------------------------------------

func main() {
	runtime.GOMAXPROCS(1)

	var address, period string
	var reqRep bool
	flag.StringVar(&address, "addr", "tcp://localhost:1235", "Address of the benthos server")
	flag.StringVar(&period, "period", "10s", "Time period between benchmark measurements")
	flag.BoolVar(&reqRep, "reqrep", false, "Use request/reply sockets instead of push/pull")

	flag.Parse()

	fmt.Fprintln(os.Stderr, "This is a benchmarking utility for benthos.")
	fmt.Fprintln(
		os.Stderr, "Make sure you are running benthos with the ./test/scale_proto.yaml"+
			" or ./test/scale_proto_reqrep.yaml config.",
	)

	duration, err := time.ParseDuration(period)
	if err != nil {
		panic(err)
	}

	var socket mangos.Socket
	if reqRep {
		socket, err = rep.NewSocket()
	} else {
		socket, err = pull.NewSocket()
	}
	if err != nil {
		panic(err)
	}

	socket.AddTransport(tcp.NewTransport())
	socket.AddTransport(ipc.NewTransport())
	socket.SetOption(mangos.OptionRecvDeadline, time.Second)

	if err = socket.Dial(address); err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	benchChan := test.StartPrintingBenchmarks(duration, 0)
	<-time.After(time.Second)

	var running int32 = 1
	go func() {
		for atomic.LoadInt32(&running) == 1 {
			data, err := socket.Recv()
			if err != nil && err != mangos.ErrRecvTimeout {
				fmt.Printf("| Error: Failed to read socket: %v\n", err)
			} else if err == nil {
				msg, err := types.FromBytes(data)
				if err != nil {
					fmt.Printf("| Error: Wrong message format: %v\n", err)
				} else {
					if bench, err := test.BenchFromMessage(msg); err == nil {
						benchChan <- bench
					} else {
						fmt.Printf("| Error: Wrong message format: %v\n", err)
					}
				}
			}
			if reqRep {
				if err == nil {
					if err = socket.Send([]byte("SUCCESS")); err != nil {
						fmt.Printf("| Error: Failed to send response: %v\n", err)
					}
				} else if err != mangos.ErrRecvTimeout {
					if err = socket.Send([]byte("ERROR")); err != nil {
						fmt.Printf("| Error: Failed to send response: %v\n", err)
					}
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
