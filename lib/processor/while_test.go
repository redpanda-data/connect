package processor

import (
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWhileErrs(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeWhile

	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	require.EqualError(t, err, "a check query is required")
}

func TestWhileWithCount(t *testing.T) {
	conf := NewConfig()
	conf.Type = "while"
	conf.While.Check = `count("while_test_1") < 3`

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	conf.While.Processors = append(conf.While.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`bar`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{[]byte("bar")}))
	require.Nil(t, res)

	assert.Equal(t, exp, message.GetAllBytes(msg[0]))
}

func TestWhileWithContentCheck(t *testing.T) {
	conf := NewConfig()
	conf.Type = "while"
	conf.While.Check = "batch_size() <= 3"

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	conf.While.Processors = append(conf.While.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`bar`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestWhileWithCountALO(t *testing.T) {
	conf := NewConfig()
	conf.Type = "while"
	conf.While.Check = `count("while_test_2") < 3`
	conf.While.AtLeastOnce = true

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	conf.While.Processors = append(conf.While.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`bar`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestWhileMaxLoops(t *testing.T) {
	conf := NewConfig()
	conf.Type = "while"
	conf.While.MaxLoops = 3
	conf.While.Check = `true`

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	conf.While.Processors = append(conf.While.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`bar`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestWhileWithStaticTrue(t *testing.T) {
	conf := NewConfig()
	conf.Type = "while"
	conf.While.Check = `true`

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	conf.While.Processors = append(conf.While.Processors, procConf)

	procConf = NewConfig()
	procConf.Type = "sleep"
	procConf.Sleep.Duration = "100ms"

	conf.While.Processors = append(conf.While.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		<-time.After(time.Millisecond * 100)
		c.CloseAsync()
	}()

	_, res := c.ProcessMessage(message.New([][]byte{[]byte("bar")}))
	if res == nil {
		t.Error("Expected error response due to shut down")
	}

	if err := c.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
