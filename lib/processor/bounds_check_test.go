package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
)

func TestBoundsCheck(t *testing.T) {
	conf := NewConfig()
	conf.BoundsCheck.MinParts = 2
	conf.BoundsCheck.MaxParts = 3
	conf.BoundsCheck.MaxPartSize = 10
	conf.BoundsCheck.MinPartSize = 1

	testLog := log.Noop()
	proc, err := NewBoundsCheck(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	goodParts := [][][]byte{
		{
			[]byte("hello"),
			[]byte("world"),
		},
		{
			[]byte("helloworld"),
			[]byte("helloworld"),
		},
		{
			[]byte("hello"),
			[]byte("world"),
			[]byte("!"),
		},
		{
			[]byte("helloworld"),
			[]byte("helloworld"),
			[]byte("helloworld"),
		},
	}

	badParts := [][][]byte{
		{
			[]byte("hello world"),
		},
		{
			[]byte("hello world"),
			[]byte("hello world this exceeds max part size"),
		},
		{
			[]byte("hello"),
			[]byte("world"),
			[]byte("this"),
			[]byte("exceeds"),
			[]byte("max"),
			[]byte("num"),
			[]byte("parts"),
		},
		{
			[]byte("hello"),
			[]byte(""),
		},
	}

	for _, parts := range goodParts {
		msg := message.QuickBatch(parts)
		if msgs, _ := proc.ProcessMessage(msg); len(msgs) == 0 {
			t.Errorf("Bounds check failed on: %s", parts)
		} else if !reflect.DeepEqual(msgs[0], msg) {
			t.Error("Wrong message returned (expected same)")
		}
	}

	for _, parts := range badParts {
		if msgs, res := proc.ProcessMessage(message.QuickBatch(parts)); len(msgs) > 0 {
			t.Errorf("Bounds check didnt fail on: %s", parts)
		} else if _, ok := res.(response.Ack); !ok {
			t.Error("Expected simple response from bad message")
		}
	}
}
