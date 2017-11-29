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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jeffail/benthos/lib/buffer"
	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/benthos/lib/util/test"
	"github.com/jeffail/benthos/lib/util/service/log"
	"github.com/jeffail/benthos/lib/util/service/metrics"
)

//--------------------------------------------------------------------------------------------------

func main() {
	var period, messageDelay, filesDir string
	var blobSize int

	flag.StringVar(&period, "period", "60s", "Time period between benchmark measurements")
	flag.StringVar(&filesDir, "mmap_dir", "/tmp/benthos", "Location for storing mmap files")
	flag.StringVar(&messageDelay, "interval", "10us",
		"Interval between messages sent, use this to test latency at different stress levels")
	flag.IntVar(&blobSize, "blob_size", 1024, "Size of each message to be sent")

	flag.Parse()

	fmt.Fprintln(os.Stderr, "Launching in-process benchmarking using mmap persistence.")

	duration, err := time.ParseDuration(period)
	if err != nil {
		panic(err)
	}

	interval, err := time.ParseDuration(messageDelay)
	if err != nil {
		panic(err)
	}

	bufferConf := buffer.NewConfig()
	bufferConf.Type = "mmap_file"
	bufferConf.Mmap.Path = filesDir

	var logConfig = log.LoggerConfig{
		LogLevel: "NONE",
	}

	msgChan, resChan := make(chan types.Message), make(chan types.Response)

	buf, err := buffer.New(bufferConf, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		panic(err)
	}
	buf.StartListening(resChan)
	buf.StartReceiving(msgChan)

	benchChan := test.StartPrintingBenchmarks(duration, 0)
	<-time.After(time.Second)

	wg := sync.WaitGroup{}
	wg.Add(2)

	var running int32 = 1

	// Writer routine
	go func() {
		defer func() {
			close(msgChan)
			wg.Done()
		}()

		var index int32

		dataBlob := make([]byte, blobSize)
		for i := range dataBlob {
			dataBlob[i] = byte(rand.Int())
		}

		for atomic.LoadInt32(&running) == 1 {
			<-time.After(interval)
			msgChan <- test.NewBenchMessage(index, dataBlob)
			_, open := <-buf.ResponseChan()
			if !open {
				return
			}
			index++
		}
	}()

	// Reader routine
	go func() {
		defer func() {
			close(resChan)
			wg.Done()
		}()

		for {
			msg, open := <-buf.MessageChan()
			if !open {
				return
			}
			if bench, err := test.BenchFromMessage(msg); err == nil {
				benchChan <- bench
			} else {
				fmt.Printf("Error: Wrong message format: %v\n", err)
			}
			resChan <- types.NewSimpleResponse(nil)
		}
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
