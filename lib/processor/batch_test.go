package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestBatchBasic(t *testing.T) {
	conf := NewConfig()
	conf.Batch.Count = 2
	conf.Batch.ByteSize = 0

	testLog := log.Noop()
	proc, err := NewBatch(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	msgs, res := proc.ProcessMessage(message.New(exp))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, message.GetAllBytes(msgs[0])) {
		t.Errorf("Wrong result: %s != %s", message.GetAllBytes(msgs[0]), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func TestBatchTwoParts(t *testing.T) {
	conf := NewConfig()
	conf.Batch.ByteSize = 6

	testLog := log.Noop()
	proc, err := NewBatch(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	msgs, res := proc.ProcessMessage(message.New(exp))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, message.GetAllBytes(msgs[0])) {
		t.Errorf("Wrong result: %s != %s", message.GetAllBytes(msgs[0]), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func TestBatchLotsOfParts(t *testing.T) {
	conf := NewConfig()
	conf.Batch.ByteSize = 1

	testLog := log.Noop()
	proc, err := NewBatch(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	input := [][]byte{
		[]byte("foo"), []byte("bar"), []byte("bar2"),
		[]byte("bar3"), []byte("bar4"), []byte("bar5"),
	}

	msgs, res := proc.ProcessMessage(message.New(input))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(input, message.GetAllBytes(msgs[0])) {
		t.Errorf("Wrong result: %s != %s", message.GetAllBytes(msgs[0]), input)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func TestBatchTwoSingleParts(t *testing.T) {
	conf := NewConfig()
	conf.Batch.ByteSize = 5

	testLog := log.Noop()
	proc, err := NewBatch(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{[]byte("foo1"), []byte("bar1")}

	inMsg := message.New([][]byte{exp[0]})
	inMsg.Get(0).Metadata().Set("foo", "bar1")

	msgs, res := proc.ProcessMessage(inMsg)
	if len(msgs) != 0 {
		t.Error("Expected fail on one part")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	inMsg = message.New([][]byte{exp[1]})
	inMsg.Get(0).Metadata().Set("foo", "bar2")

	msgs, res = proc.ProcessMessage(inMsg)
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, message.GetAllBytes(msgs[0])) {
		t.Errorf("Wrong result: %s != %s", message.GetAllBytes(msgs[0]), exp)
	}
	if exp, act := "bar1", msgs[0].Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong metadata: %v != %v", act, exp)
	}
	if exp, act := "bar2", msgs[0].Get(1).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong metadata: %v != %v", act, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	exp = [][]byte{[]byte("foo2"), []byte("bar2")}

	msgs, res = proc.ProcessMessage(message.New([][]byte{exp[0]}))
	if len(msgs) != 0 {
		t.Error("Expected fail on one part")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, res = proc.ProcessMessage(message.New([][]byte{exp[1]}))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, message.GetAllBytes(msgs[0])) {
		t.Errorf("Wrong result: %s != %s", message.GetAllBytes(msgs[0]), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func TestBatchTwoDiffParts(t *testing.T) {
	conf := NewConfig()
	conf.Batch.ByteSize = 5

	testLog := log.Noop()
	proc, err := NewBatch(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	input := [][]byte{[]byte("foo1"), []byte("bar1")}
	exp := [][]byte{[]byte("foo1"), []byte("bar1"), []byte("foo1")}

	msgs, res := proc.ProcessMessage(message.New([][]byte{input[0]}))
	if len(msgs) != 0 {
		t.Error("Expected fail on one part")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, res = proc.ProcessMessage(message.New([][]byte{input[1], input[0]}))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, message.GetAllBytes(msgs[0])) {
		t.Errorf("Wrong result: %s != %s", message.GetAllBytes(msgs[0]), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	exp = [][]byte{[]byte("bar1"), []byte("foo1")}

	msgs, res = proc.ProcessMessage(message.New([][]byte{input[1], input[0]}))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, message.GetAllBytes(msgs[0])) {
		t.Errorf("Wrong result: %s != %s", message.GetAllBytes(msgs[0]), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	exp = [][]byte{[]byte("bar1"), []byte("foo1"), []byte("bar1")}

	msgs, res = proc.ProcessMessage(message.New([][]byte{input[1], input[0], input[1]}))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, message.GetAllBytes(msgs[0])) {
		t.Errorf("Wrong result: %s != %s", message.GetAllBytes(msgs[0]), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func TestBatchCondition(t *testing.T) {
	condConf := condition.NewConfig()
	condConf.Type = "text"
	condConf.Text.Operator = "contains"
	condConf.Text.Arg = "end_batch"

	conf := NewConfig()
	conf.Batch.ByteSize = 1000
	conf.Batch.Condition = condConf

	testLog := log.Noop()
	proc, err := NewBatch(conf, nil, testLog, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	msgs, res := proc.ProcessMessage(message.New([][]byte{[]byte("foo")}))
	if len(msgs) != 0 {
		t.Error("Expected no batch")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, res = proc.ProcessMessage(message.New([][]byte{[]byte("bar")}))
	if len(msgs) != 0 {
		t.Error("Expected no batch")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, _ = proc.ProcessMessage(message.New([][]byte{[]byte("baz: end_batch")}))
	if len(msgs) != 1 {
		t.Fatal("Expected batch")
	}

	exp := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz: end_batch"),
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong batch contents: %s != %s", act, exp)
	}
}
