package processor

import (
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestSubprocessWithSed(t *testing.T) {
	t.Skip("disabled for now")

	conf := NewConfig()
	conf.Type = TypeSubprocess
	conf.Subprocess.Name = "sed"
	conf.Subprocess.Args = []string{"s/foo/bar/g", "-u"}

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Skipf("Not sure if this is due to missing executable: %v", err)
	}

	exp := [][]byte{
		[]byte(`hello bar world`),
		[]byte(`hello baz world`),
		[]byte(`bar`),
	}
	msgIn := message.New([][]byte{
		[]byte(`hello foo world`),
		[]byte(`hello baz world`),
		[]byte(`foo`),
	})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("Wrong count of messages")
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res.Error())
	}

	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestSubprocessWithCat(t *testing.T) {
	t.Skip("disabled for now")

	conf := NewConfig()
	conf.Type = TypeSubprocess
	conf.Subprocess.Name = "cat"

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Skipf("Not sure if this is due to missing executable: %v", err)
	}

	exp := [][]byte{
		[]byte(`hello bar world`),
		[]byte(`hello baz world`),
		[]byte(`bar`),
	}
	msgIn := message.New([][]byte{
		[]byte(`hello bar world`),
		[]byte(`hello baz world`),
		[]byte(`bar`),
	})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("Wrong count of messages")
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res.Error())
	}

	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestSubprocessLineBreaks(t *testing.T) {
	t.Skip("disabled for now")

	conf := NewConfig()
	conf.Type = TypeSubprocess
	conf.Subprocess.Name = "sed"
	conf.Subprocess.Args = []string{`s/\(^$\)\|\(foo\)/bar/`, "-u"}

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Skipf("Not sure if this is due to missing executable: %v", err)
	}

	exp := [][]byte{
		[]byte("hello bar\nbar world"),
		[]byte("hello bar bar world"),
		[]byte("hello bar\nbar world\n"),
		[]byte("bar"),
		[]byte("hello bar\nbar\nbar world\n"),
	}
	msgIn := message.New([][]byte{
		[]byte("hello foo\nfoo world"),
		[]byte("hello foo bar world"),
		[]byte("hello foo\nfoo world\n"),
		[]byte(""),
		[]byte("hello foo\n\nfoo world\n"),
	})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatalf("Wrong count of messages %d", len(msgs))
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res.Error())
	}

	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
