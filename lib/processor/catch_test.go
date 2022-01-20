package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestCatchEmpty(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeCatch

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte("foo bar baz"),
	}
	msgs, res := proc.ProcessMessage(message.New(exp))
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	msgs[0].Iter(func(i int, p types.Part) error {
		if HasFailed(p) {
			t.Errorf("Unexpected part %v failed flag", i)
		}
		return nil
	})
}

func TestCatchBasic(t *testing.T) {
	encodeConf := NewConfig()
	encodeConf.Type = TypeBloblang
	encodeConf.Bloblang = `root = if batch_index() == 0 { content().encode("base64") }`

	conf := NewConfig()
	conf.Type = TypeCatch
	conf.Catch = append(conf.Catch, encodeConf)

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("Zm9vIGJhciBiYXo="),
		[]byte("MSAyIDMgNA=="),
		[]byte("aGVsbG8gZm9vIHdvcmxk"),
	}

	msg := message.New(parts)
	msg.Iter(func(i int, p types.Part) error {
		FlagFail(p)
		return nil
	})
	msgs, res := proc.ProcessMessage(msg)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	msgs[0].Iter(func(i int, p types.Part) error {
		if HasFailed(p) {
			t.Errorf("Unexpected part %v failed flag", i)
		}
		return nil
	})
}

func TestCatchFilterSome(t *testing.T) {
	filterConf := NewConfig()
	filterConf.Type = TypeBloblang
	filterConf.Bloblang = `root = if !content().contains("foo") { deleted() }`

	conf := NewConfig()
	conf.Type = TypeCatch
	conf.Catch = append(conf.Catch, filterConf)

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("foo bar baz"),
		[]byte("hello foo world"),
	}
	msg := message.New(parts)
	msg.Iter(func(i int, p types.Part) error {
		FlagFail(p)
		return nil
	})
	msgs, res := proc.ProcessMessage(msg)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	msgs[0].Iter(func(i int, p types.Part) error {
		if HasFailed(p) {
			t.Errorf("Unexpected part %v failed flag", i)
		}
		return nil
	})
}

func TestCatchMultiProcs(t *testing.T) {
	encodeConf := NewConfig()
	encodeConf.Type = TypeBloblang
	encodeConf.Bloblang = `root = if batch_index() == 0 { content().encode("base64") }`

	filterConf := NewConfig()
	filterConf.Type = TypeBloblang
	filterConf.Bloblang = `root = if !content().contains("foo") { deleted() }`

	conf := NewConfig()
	conf.Type = TypeCatch
	conf.Catch = append(conf.Catch, filterConf, encodeConf)

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("Zm9vIGJhciBiYXo="),
		[]byte("aGVsbG8gZm9vIHdvcmxk"),
	}
	msg := message.New(parts)
	msg.Iter(func(i int, p types.Part) error {
		FlagFail(p)
		return nil
	})
	msgs, res := proc.ProcessMessage(msg)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	msgs[0].Iter(func(i int, p types.Part) error {
		if HasFailed(p) {
			t.Errorf("Unexpected part %v failed flag", i)
		}
		return nil
	})
}

func TestCatchNotFails(t *testing.T) {
	encodeConf := NewConfig()
	encodeConf.Type = TypeBloblang
	encodeConf.Bloblang = `root = if batch_index() == 0 { content().encode("base64") }`

	conf := NewConfig()
	conf.Type = TypeCatch
	conf.Catch = append(conf.Catch, encodeConf)

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte(`FAILED ENCODE ME PLEASE`),
		[]byte("NOT FAILED, DO NOT ENCODE"),
		[]byte(`FAILED ENCODE ME PLEASE 2`),
	}
	exp := [][]byte{
		[]byte("RkFJTEVEIEVOQ09ERSBNRSBQTEVBU0U="),
		[]byte("NOT FAILED, DO NOT ENCODE"),
		[]byte("RkFJTEVEIEVOQ09ERSBNRSBQTEVBU0UgMg=="),
	}
	msg := message.New(parts)
	FlagFail(msg.Get(0))
	FlagFail(msg.Get(2))
	msgs, res := proc.ProcessMessage(msg)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	msgs[0].Iter(func(i int, p types.Part) error {
		if HasFailed(p) {
			t.Errorf("Unexpected part %v failed flag", i)
		}
		return nil
	})
}

func TestCatchFilterAll(t *testing.T) {
	filterConf := NewConfig()
	filterConf.Type = TypeBloblang
	filterConf.Bloblang = `root = if !content().contains("foo") { deleted() }`

	conf := NewConfig()
	conf.Type = TypeCatch
	conf.Catch = append(conf.Catch, filterConf)

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello world"),
	}
	msg := message.New(parts)
	msg.Iter(func(i int, p types.Part) error {
		FlagFail(p)
		return nil
	})
	msgs, res := proc.ProcessMessage(msg)
	if res == nil {
		t.Fatal("expected empty response")
	}
	if err = res.Error(); err != nil {
		t.Error(err)
	}
	if len(msgs) != 0 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
}

//------------------------------------------------------------------------------
