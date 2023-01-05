package pure_test

import (
	"context"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestSplitToSingleParts(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "split"

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Error(err)
		return
	}

	tests := [][][]byte{
		{},
		{
			[]byte("foo"),
		},
		{
			[]byte("foo"),
			[]byte("bar"),
		},
		{
			[]byte("foo"),
			[]byte("bar"),
			[]byte("baz"),
		},
	}

	for _, tIn := range tests {
		inMsg := message.QuickBatch(tIn)
		_ = inMsg.Iter(func(i int, p *message.Part) error {
			p.MetaSetMut("foo", "bar")
			return nil
		})
		msgs, _ := proc.ProcessBatch(context.Background(), inMsg)
		if exp, act := len(tIn), len(msgs); exp != act {
			t.Errorf("Wrong count of messages: %v != %v", act, exp)
			continue
		}
		for i, expBytes := range tIn {
			if act, exp := string(msgs[i].Get(0).AsBytes()), string(expBytes); act != exp {
				t.Errorf("Wrong contents: %v != %v", act, exp)
			}
			if act, exp := msgs[i].Get(0).MetaGetStr("foo"), "bar"; act != exp {
				t.Errorf("Wrong metadata: %v != %v", act, exp)
			}
		}
	}
}

func TestSplitToMultipleParts(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "split"
	conf.Split.Size = 2

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Error(err)
		return
	}

	inMsg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	})
	msgs, _ := proc.ProcessBatch(context.Background(), inMsg)
	if exp, act := 2, len(msgs); exp != act {
		t.Fatalf("Wrong message count: %v != %v", act, exp)
	}
	if exp, act := 2, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong message count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[1].Len(); exp != act {
		t.Fatalf("Wrong message count: %v != %v", act, exp)
	}
	if exp, act := "foo", string(msgs[0].Get(0).AsBytes()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(msgs[0].Get(1).AsBytes()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "baz", string(msgs[1].Get(0).AsBytes()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
}

func TestSplitByBytes(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "split"
	conf.Split.Size = 0
	conf.Split.ByteSize = 6

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	inMsg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	})
	msgs, _ := proc.ProcessBatch(context.Background(), inMsg)
	if exp, act := 2, len(msgs); exp != act {
		t.Fatalf("Wrong batch count: %v != %v", act, exp)
	}
	if exp, act := 2, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong message 1 count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[1].Len(); exp != act {
		t.Fatalf("Wrong message 2 count: %v != %v", act, exp)
	}
	if exp, act := "foo", string(msgs[0].Get(0).AsBytes()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(msgs[0].Get(1).AsBytes()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "baz", string(msgs[1].Get(0).AsBytes()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
}

func TestSplitByBytesTooLarge(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "split"
	conf.Split.Size = 0
	conf.Split.ByteSize = 2

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	inMsg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	})
	msgs, _ := proc.ProcessBatch(context.Background(), inMsg)
	if exp, act := 3, len(msgs); exp != act {
		t.Fatalf("Wrong batch count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong message 1 count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[1].Len(); exp != act {
		t.Fatalf("Wrong message 2 count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[2].Len(); exp != act {
		t.Fatalf("Wrong message 3 count: %v != %v", act, exp)
	}
	if exp, act := "foo", string(msgs[0].Get(0).AsBytes()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(msgs[1].Get(0).AsBytes()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "baz", string(msgs[2].Get(0).AsBytes()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
}
