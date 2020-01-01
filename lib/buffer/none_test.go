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
