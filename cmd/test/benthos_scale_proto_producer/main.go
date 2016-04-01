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
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/push"
	"github.com/go-mangos/mangos/transport/ipc"
	"github.com/go-mangos/mangos/transport/tcp"

	"github.com/jeffail/benthos/types"
	"github.com/jeffail/benthos/util/test"
)

//--------------------------------------------------------------------------------------------------

func main() {
	var address string
	flag.StringVar(&address, "addr", "tcp://localhost:1234", "Address of the benthos server")

	flag.Parse()

	fmt.Println("This is a benchmarking utility for benthos.")
	fmt.Println("Make sure you are running benthos with the ./test/scale_proto.yaml config.")

	if len(flag.Args()) != 2 {
		fmt.Printf("\nUsage: %v <interval> <blob_size>\n", os.Args[0])
		fmt.Printf("e.g.: %v 10us 1024\n", os.Args[0])
		return
	}

	interval, err := time.ParseDuration(flag.Args()[0])
	if err != nil {
		panic(err)
	}

	blobSize, err := strconv.Atoi(flag.Args()[1])
	if err != nil {
		panic(err)
	}

	socket, err := push.NewSocket()
	if err != nil {
		panic(err)
	}

	socket.AddTransport(tcp.NewTransport())
	socket.AddTransport(ipc.NewTransport())
	socket.SetOption(mangos.OptionSendDeadline, time.Second)

	if err = socket.Dial(address); err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	dataBlob := make([]byte, blobSize)
	for i := range dataBlob {
		dataBlob[i] = byte(rand.Int())
	}

	var running, index int32 = 1, 0

	go func() {
		var msg *types.Message
		for atomic.LoadInt32(&running) == 1 {
			if msg == nil {
				<-time.After(interval)
				tmpMsg := test.NewBenchMessage(index, dataBlob)
				msg = &tmpMsg
			}
			err = socket.Send(msg.Bytes())
			if err != nil && err != mangos.ErrSendTimeout {
				panic(err)
			} else {
				msg = nil
			}
			index++
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
	go func() {
		<-time.After(time.Second * 10)
		panic(errors.New("Timed out waiting for processes to end"))
	}()
	wg.Wait()
}

//--------------------------------------------------------------------------------------------------
