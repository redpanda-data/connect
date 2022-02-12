package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func TestGroupByValueBasic(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeGroupByValue
	conf.GroupByValue.Value = "${!json(\"foo\")}"

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][][]byte{
		{
			[]byte(`{"foo":0,"bar":0}`),
			[]byte(`{"foo":0,"bar":7}`),
		},
		{
			[]byte(`{"foo":3,"bar":1}`),
		},
		{
			[]byte(`{"bar":2}`),
		},
		{
			[]byte(`{"foo":2,"bar":3}`),
		},
		{
			[]byte(`{"foo":4,"bar":4}`),
		},
		{
			[]byte(`{"foo":1,"bar":5}`),
			[]byte(`{"foo":1,"bar":6}`),
			[]byte(`{"foo":1,"bar":8}`),
		},
	}
	act := [][][]byte{}

	input := message.QuickBatch([][]byte{
		[]byte(`{"foo":0,"bar":0}`),
		[]byte(`{"foo":3,"bar":1}`),
		[]byte(`{"bar":2}`),
		[]byte(`{"foo":2,"bar":3}`),
		[]byte(`{"foo":4,"bar":4}`),
		[]byte(`{"foo":1,"bar":5}`),
		[]byte(`{"foo":1,"bar":6}`),
		[]byte(`{"foo":0,"bar":7}`),
		[]byte(`{"foo":1,"bar":8}`),
	})
	msgs, res := proc.ProcessMessage(input)
	if res != nil {
		t.Fatal(res)
	}

	for _, msg := range msgs {
		act = append(act, message.GetAllBytes(msg))
	}
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

//------------------------------------------------------------------------------
