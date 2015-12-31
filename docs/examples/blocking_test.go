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

package examples

import (
	"fmt"
	"sync"
	"testing"
)

//--------------------------------------------------------------------------------------------------

// blocker - A type for buffering strings, which blocks respective read or write calls.
type blocker interface {
	Write(data string)
	Read() string
}

//--------------------------------------------------------------------------------------------------

type condBlocker struct {
	limit int
	buf   []string
	cond  *sync.Cond
}

func (c *condBlocker) Write(data string) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	defer c.cond.Broadcast()

	for len(c.buf) > c.limit {
		c.cond.Wait()
	}

	c.buf = append(c.buf, data)
}

func (c *condBlocker) Read() string {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	defer c.cond.Broadcast()

	for len(c.buf) == 0 {
		c.cond.Wait()
	}

	data := c.buf[0]
	c.buf = c.buf[1:]

	return data
}

//--------------------------------------------------------------------------------------------------

type selectBlocker struct {
	limit   int
	inChan  chan string
	outChan chan string
}

func (s *selectBlocker) Write(data string) {
	s.inChan <- data
}

func (s *selectBlocker) Read() string {
	return <-s.outChan
}

func (s *selectBlocker) loop() {
	var inChan, outChan chan string

	buf := []string{}
	for {
		inChan = s.inChan
		outChan = s.outChan

		nextData := ""

		if len(buf) == 0 {
			outChan = nil
		} else {
			nextData = buf[0]
		}
		if len(buf) >= s.limit {
			inChan = nil
		}

		select {
		case d := <-inChan:
			buf = append(buf, d)
		case outChan <- nextData:
			buf = buf[1:]
		}
	}
}

//--------------------------------------------------------------------------------------------------

var (
	blockerLayers = 20
	blockerLoops  = 1000
)

func runBlockerAsyncBenchmark(layers []blocker, b *testing.B) {
	if len(layers) != blockerLayers {
		b.Errorf("Test did not provide correct # of blocker layers, %v != %v", len(layers), blockerLayers)
		return
	}

	b.StopTimer()

	readyWg, doneWg := sync.WaitGroup{}, sync.WaitGroup{}
	startChan := make(chan struct{})

	readyWg.Add(blockerLayers + 1)
	doneWg.Add(blockerLayers + 1)

	for i := 0; i < blockerLayers-1; i++ {
		go func(bOut, bIn blocker) {
			defer doneWg.Done()
			readyWg.Done()

			<-startChan

			for j := 0; j < blockerLoops; j++ {
				res, exp := bOut.Read(), fmt.Sprintf("loop%v", j)
				if res != exp {
					b.Errorf("Unexpected data: %v != %v", res, exp)
				}
				bIn.Write(exp)
			}
		}(layers[i], layers[i+1])
	}

	go func() {
		defer doneWg.Done()
		readyWg.Done()

		<-startChan

		for i := 0; i < blockerLoops; i++ {
			layers[0].Write(fmt.Sprintf("loop%v", i))
		}
	}()

	go func() {
		defer doneWg.Done()
		readyWg.Done()

		<-startChan

		for i := 0; i < blockerLoops; i++ {
			if res, exp := layers[blockerLayers-1].Read(), fmt.Sprintf("loop%v", i); res != exp {
				b.Errorf("Unexpected data: %v != %v", res, exp)
			}
		}
	}()

	readyWg.Wait()

	b.StartTimer()
	close(startChan)

	doneWg.Wait()
}

func runBlockerSyncBenchmark(layers []blocker, b *testing.B) {
	if len(layers) != blockerLayers {
		b.Errorf("Test did not provide correct # of blocker layers, %v != %v", len(layers), blockerLayers)
		return
	}

	for i := 0; i < blockerLoops; i++ {
		layers[0].Write(fmt.Sprintf("loop%v", i))

		for j := 0; j < blockerLayers-1; j++ {
			res, exp := layers[j].Read(), fmt.Sprintf("loop%v", i)
			if res != exp {
				b.Errorf("Unexpected data: %v != %v", res, exp)
			}
			layers[j+1].Write(exp)
		}

		if res, exp := layers[blockerLayers-1].Read(), fmt.Sprintf("loop%v", i); res != exp {
			b.Errorf("Unexpected data: %v != %v", res, exp)
		}
	}
}

func BenchmarkSyncCondBlocking(b *testing.B) {
	for j := 0; j < b.N; j++ {
		b.StopTimer()

		blockers := []blocker{}

		for i := 0; i < blockerLayers; i++ {
			blockers = append(blockers, &condBlocker{
				limit: 10,
				buf:   []string{},
				cond:  sync.NewCond(&sync.Mutex{}),
			})
		}

		b.StartTimer()
		runBlockerSyncBenchmark(blockers, b)
	}
}

func BenchmarkSyncSelectBlocking(b *testing.B) {
	for j := 0; j < b.N; j++ {
		b.StopTimer()

		blockers := []blocker{}

		for i := 0; i < blockerLayers; i++ {
			selBlocker := &selectBlocker{
				limit:   10,
				inChan:  make(chan string),
				outChan: make(chan string),
			}
			go selBlocker.loop()

			blockers = append(blockers, selBlocker)
		}

		b.StartTimer()
		runBlockerSyncBenchmark(blockers, b)
	}
}

func BenchmarkAsyncCondBlocking(b *testing.B) {
	for j := 0; j < b.N; j++ {
		b.StopTimer()

		blockers := []blocker{}

		for i := 0; i < blockerLayers; i++ {
			blockers = append(blockers, &condBlocker{
				limit: 10,
				buf:   []string{},
				cond:  sync.NewCond(&sync.Mutex{}),
			})
		}

		b.StartTimer()
		runBlockerAsyncBenchmark(blockers, b)
	}
}

func BenchmarkAsyncSelectBlocking(b *testing.B) {
	for j := 0; j < b.N; j++ {
		b.StopTimer()

		blockers := []blocker{}

		for i := 0; i < blockerLayers; i++ {
			selBlocker := &selectBlocker{
				limit:   10,
				inChan:  make(chan string),
				outChan: make(chan string),
			}
			go selBlocker.loop()

			blockers = append(blockers, selBlocker)
		}

		b.StartTimer()
		runBlockerAsyncBenchmark(blockers, b)
	}
}

//--------------------------------------------------------------------------------------------------
