package output

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestHTTPBasic(t *testing.T) {
	nTestLoops := 10

	conf := NewConfig()
	conf.HTTPServer.Address = "localhost:1237"
	conf.HTTPServer.Path = "/testpost"

	h, err := NewHTTPServer(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)
	resChan := make(chan response.Error)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}
	if err = h.Consume(msgChan); err == nil {
		t.Error("Expected error from double listen")
	}

	<-time.After(time.Millisecond * 100)

	// Test both single and multipart messages.
	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)

		go func() {
			testMsg := message.QuickBatch([][]byte{[]byte(testStr)})
			select {
			case msgChan <- types.NewTransaction(testMsg, resChan):
			case <-time.After(time.Second):
				t.Error("Timed out waiting for message")
				return
			}
			select {
			case resMsg := <-resChan:
				if resMsg.AckError() != nil {
					t.Error(resMsg.AckError())
				}
			case <-time.After(time.Second):
				t.Error("Timed out waiting for response")
			}
		}()

		res, err := http.Get("http://localhost:1237/testpost")
		if err != nil {
			t.Error(err)
			return
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Errorf("Wrong error code returned: %v", res.StatusCode)
			return
		}
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestHTTPBadRequests(t *testing.T) {
	conf := NewConfig()
	conf.HTTPServer.Address = "localhost:1236"
	conf.HTTPServer.Path = "/testpost"

	h, err := NewHTTPServer(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 100)

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}

	_, err = http.Get("http://localhost:1236/testpost")
	if err == nil {
		t.Error("request success when service should be closed")
	}
}

func TestHTTPTimeout(t *testing.T) {
	conf := NewConfig()
	conf.HTTPServer.Address = "localhost:1235"
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.Timeout = "1ms"

	h, err := NewHTTPServer(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 100)

	var res *http.Response
	res, err = http.Get("http://localhost:1235/testpost")
	if err != nil {
		t.Error(err)
		return
	}
	if exp, act := http.StatusRequestTimeout, res.StatusCode; exp != act {
		t.Errorf("Unexpected status code: %v != %v", exp, act)
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}
