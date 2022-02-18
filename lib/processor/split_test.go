package processor

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestSplitToSingleParts(t *testing.T) {
	conf := NewConfig()
	conf.Type = "split"

	testLog := log.Noop()
	proc, err := New(conf, mock.NewManager(), testLog, metrics.Noop())
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
			p.MetaSet("foo", "bar")
			return nil
		})
		msgs, _ := proc.ProcessMessage(inMsg)
		if exp, act := len(tIn), len(msgs); exp != act {
			t.Errorf("Wrong count of messages: %v != %v", act, exp)
			continue
		}
		for i, expBytes := range tIn {
			if act, exp := string(msgs[i].Get(0).Get()), string(expBytes); act != exp {
				t.Errorf("Wrong contents: %v != %v", act, exp)
			}
			if act, exp := msgs[i].Get(0).MetaGet("foo"), "bar"; act != exp {
				t.Errorf("Wrong metadata: %v != %v", act, exp)
			}
		}
	}
}

func TestSplitToMultipleParts(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeSplit
	conf.Split.Size = 2

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	inMsg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	})
	msgs, _ := proc.ProcessMessage(inMsg)
	if exp, act := 2, len(msgs); exp != act {
		t.Fatalf("Wrong message count: %v != %v", act, exp)
	}
	if exp, act := 2, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong message count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[1].Len(); exp != act {
		t.Fatalf("Wrong message count: %v != %v", act, exp)
	}
	if exp, act := "foo", string(msgs[0].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(msgs[0].Get(1).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "baz", string(msgs[1].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
}

func TestSplitByBytes(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeSplit
	conf.Split.Size = 0
	conf.Split.ByteSize = 6

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	inMsg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	})
	msgs, _ := proc.ProcessMessage(inMsg)
	if exp, act := 2, len(msgs); exp != act {
		t.Fatalf("Wrong batch count: %v != %v", act, exp)
	}
	if exp, act := 2, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong message 1 count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[1].Len(); exp != act {
		t.Fatalf("Wrong message 2 count: %v != %v", act, exp)
	}
	if exp, act := "foo", string(msgs[0].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(msgs[0].Get(1).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "baz", string(msgs[1].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
}

func TestSplitByBytesTooLarge(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeSplit
	conf.Split.Size = 0
	conf.Split.ByteSize = 2

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	inMsg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	})
	msgs, _ := proc.ProcessMessage(inMsg)
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
	if exp, act := "foo", string(msgs[0].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(msgs[1].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "baz", string(msgs[2].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
}
