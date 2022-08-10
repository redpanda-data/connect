package pure_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func TestGroupByValueBasic(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "group_by_value"
	conf.GroupByValue.Value = "${!json(\"foo\")}"

	proc, err := mock.NewManager().NewProcessor(conf)
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
	msgs, res := proc.ProcessBatch(context.Background(), input)
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
