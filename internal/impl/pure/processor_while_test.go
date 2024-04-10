package pure_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func TestWhileErrs(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "while"

	_, err := mock.NewManager().NewProcessor(conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "a check query is required")
}

func TestWhileWithCount(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
while:
  check: 'count("while_test_1") < 3'
  processors:
    - insert_part:
        content: foo
        index: 0
`)
	require.NoError(t, err)

	c, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`bar`),
	}

	msg, res := c.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("bar")}))
	require.NoError(t, res)

	assert.Equal(t, exp, message.GetAllBytes(msg[0]))
}

func TestWhileWithContentCheck(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
while:
  check: 'batch_size() <= 3'
  processors:
    - insert_part:
        content: foo
        index: 0
`)
	require.NoError(t, err)

	c, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`bar`),
	}

	msg, res := c.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res)
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestWhileWithCountALO(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
while:
  check: 'count("while_test_2") < 3'
  at_least_once: true
  processors:
    - insert_part:
        content: foo
        index: 0
`)
	require.NoError(t, err)

	c, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`bar`),
	}

	msg, res := c.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res)
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestWhileMaxLoops(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
while:
  check: 'true'
  max_loops: 3
  processors:
    - insert_part:
        content: foo
        index: 0
`)
	require.NoError(t, err)

	c, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`foo`),
		[]byte(`bar`),
	}

	msg, res := c.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res)
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestWhileWithStaticTrue(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
while:
  check: 'true'
  processors:
    - insert_part:
        content: 'foo'
        index: 0
    - sleep:
        duration: 100ms
`)
	require.NoError(t, err)

	c, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	go func() {
		<-time.After(time.Millisecond * 100)
		assert.NoError(t, c.Close(ctx))
	}()

	_, err = c.ProcessBatch(ctx, message.QuickBatch([][]byte{[]byte("bar")}))
	assert.NoError(t, err)
}
