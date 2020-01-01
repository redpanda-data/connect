package buffer

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestMemoryBuffer(t *testing.T) {
	conf := NewConfig()
	conf.Type = "memory"

	buf, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	tChan, resChan := make(chan types.Transaction), make(chan types.Response)

	if err = buf.Consume(tChan); err != nil {
		t.Error(err)
	}

	msg := message.New([][]byte{
		[]byte(`one`),
		[]byte(`two`),
	})

	select {
	case tChan <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("buffer closed early")
		}
		if res.Error() != nil {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	var outTr types.Transaction
	var open bool
	select {
	case outTr, open = <-buf.TransactionChan():
		if !open {
			t.Error("buffer closed early")
		}
		if exp, act := 2, outTr.Payload.Len(); exp != act {
			t.Errorf("Wrong message length: %v != %v", exp, act)
		} else {
			if exp, act := `one`, string(outTr.Payload.Get(0).Get()); exp != act {
				t.Errorf("Wrong message length: %s != %s", exp, act)
			}
			if exp, act := `two`, string(outTr.Payload.Get(1).Get()); exp != act {
				t.Errorf("Wrong message length: %s != %s", exp, act)
			}
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case outTr.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	buf.CloseAsync()
	if err := buf.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}
