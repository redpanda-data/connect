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

package buffer

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestNoneBufferClose(t *testing.T) {
	empty, err := NewEmpty(NewConfig(), nil, nil, nil)
	if err != nil {
		t.Error(err)
		return
	}

	tChan := make(chan types.Transaction)

	if err = empty.Consume(tChan); err != nil {
		t.Error(err)
		return
	}
	if err = empty.Consume(tChan); err == nil {
		t.Error("received nil, expected error from double msg assignment")
		return
	}

	empty.CloseAsync()
	if err = empty.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestNoneBufferBasic(t *testing.T) {
	nThreads, nMessages := 5, 100

	conf := NewConfig()
	empty, err := NewEmpty(conf, nil, nil, nil)
	if err != nil {
		t.Error(err)
		return
	}

	tChan := make(chan types.Transaction)

	if err = empty.Consume(tChan); err != nil {
		t.Error(err)
		return
	}

	go func() {
		for tr := range empty.TransactionChan() {
			tr.ResponseChan <- response.NewError(errors.New(string(tr.Payload.Get(0).Get())))
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(nThreads)

	for i := 0; i < nThreads; i++ {
		go func(nThread int) {
			for j := 0; j < nMessages; j++ {
				resChan := make(chan types.Response)
				msg := fmt.Sprintf("Hello World %v %v", nThread, j)
				tChan <- types.NewTransaction(message.New(
					[][]byte{[]byte(msg)},
				), resChan)
				select {
				case res := <-resChan:
					if actual := res.Error().Error(); msg != actual {
						t.Errorf("Wrong result: %v != %v", msg, actual)
					}
				case <-time.After(time.Second):
					t.Error("Timed out waiting for response")
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	close(tChan)
	if err = empty.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------
