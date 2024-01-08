package pure_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	bmock "github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestReadUntilErrs(t *testing.T) {
	conf, err := input.FromYAML(`
read_until:
  input:
    stdin: {}
`)
	require.NoError(t, err)

	_, err = bmock.NewManager().NewInput(conf)
	assert.EqualError(t, err, "failed to init input <no label>: it is required to set either check or idle_timeout")
}

func TestReadUntilInput(t *testing.T) {
	content := []byte(`foo
bar
baz`)

	tmpfile, err := os.CreateTemp("", "benthos_read_until_test")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	inConfStr := fmt.Sprintf(`
read_until:
  input:
    file:
      paths: [ "%v" ]
`, tmpfile.Name())

	t.Run("ReadUntilBasic", func(te *testing.T) {
		testReadUntilBasic(inConfStr, te)
	})
	t.Run("ReadUntilRestart", func(te *testing.T) {
		testReadUntilRestart(inConfStr, te)
	})
	t.Run("ReadUntilRetry", func(te *testing.T) {
		testReadUntilRetry(inConfStr, te)
	})
}

func testReadUntilBasic(inConf string, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	rConf, err := input.FromYAML(inConf + `
  check: 'content() == "bar"'
`)
	require.NoError(t, err)

	in, err := bmock.NewManager().NewInput(rConf)
	if err != nil {
		t.Fatal(err)
	}

	expMsgs := []string{
		"foo",
		"bar",
	}

	for i, expMsg := range expMsgs {
		var tran message.Transaction
		var open bool
		select {
		case tran, open = <-in.TransactionChan():
			if !open {
				t.Fatal("transaction chan closed")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		if exp, act := expMsg, string(tran.Payload.Get(0).AsBytes()); exp != act {
			t.Errorf("Wrong message contents: %v != %v", act, exp)
		}
		if i == len(expMsgs)-1 {
			if exp, act := "final", tran.Payload.Get(0).MetaGetStr("benthos_read_until"); exp != act {
				t.Errorf("Metadata missing from final message: %v != %v", act, exp)
			}
		} else if exp, act := "", tran.Payload.Get(0).MetaGetStr("benthos_read_until"); exp != act {
			t.Errorf("Metadata final message metadata added to non-final message: %v", act)
		}
		require.NoError(t, tran.Ack(ctx, nil))
	}

	// Should close automatically now
	select {
	case _, open := <-in.TransactionChan():
		if open {
			t.Fatal("transaction chan not closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if err = in.WaitForClose(ctx); err != nil {
		t.Fatal(err)
	}
}

func testReadUntilRestart(inConf string, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	rConf, err := input.FromYAML(inConf + `
  check: 'false'
  restart_input: true
`)
	require.NoError(t, err)

	in, err := bmock.NewManager().NewInput(rConf)
	require.NoError(t, err)

	expMsgs := []string{
		"foo",
		"bar",
		"baz",
	}

	for i := 0; i < 3; i++ {
		for _, expMsg := range expMsgs {
			var tran message.Transaction
			var open bool
			select {
			case tran, open = <-in.TransactionChan():
				require.True(t, open)
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}

			require.Len(t, tran.Payload, 1)
			assert.Equal(t, expMsg, string(tran.Payload[0].AsBytes()))
			require.NoError(t, tran.Ack(ctx, nil))
		}
	}

	in.TriggerStopConsuming()
	require.NoError(t, in.WaitForClose(ctx))
}

func testReadUntilRetry(inConf string, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	rConf, err := input.FromYAML(inConf + `
  check: 'content() == "bar"'
`)
	require.NoError(t, err)

	in, err := bmock.NewManager().NewInput(rConf)
	if err != nil {
		t.Fatal(err)
	}

	expMsgs := map[string]struct{}{
		"foo": {},
		"bar": {},
	}

	var tran message.Transaction
	var open bool

	resFns := []func(context.Context, error) error{}
	i := 0
	for len(expMsgs) > 0 && i < 10 {
		// First try
		select {
		case tran, open = <-in.TransactionChan():
			if !open {
				t.Fatalf("transaction chan closed at %v", i)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		i++
		act := string(tran.Payload.Get(0).AsBytes())
		if _, exists := expMsgs[act]; !exists {
			t.Errorf("Unexpected message contents '%v': %v", i, act)
		} else {
			delete(expMsgs, act)
		}
		{
			tmpTran := tran
			resFns = append(resFns, tmpTran.Ack)
		}
	}

	select {
	case <-in.TransactionChan():
		t.Error("Unexpected transaction")
		return
	case <-time.After(time.Millisecond * 500):
	}

	for _, rFn := range resFns {
		require.NoError(t, rFn(ctx, errors.New("failed")))
	}

	expMsgs = map[string]struct{}{
		"foo": {},
		"bar": {},
		"baz": {},
	}

remainingLoop:
	for len(expMsgs) > 0 {
		// Second try
		select {
		case tran, open = <-in.TransactionChan():
			if !open {
				break remainingLoop
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		act := string(tran.Payload.Get(0).AsBytes())
		if _, exists := expMsgs[act]; !exists {
			t.Errorf("Unexpected message contents '%v': %v", i, act)
		} else {
			delete(expMsgs, act)
		}
		require.NoError(t, tran.Ack(ctx, nil))
	}
	if len(expMsgs) == 3 {
		t.Error("Expected at least one extra message")
	}

	// Should close automatically now
	select {
	case _, open := <-in.TransactionChan():
		if open {
			t.Fatal("transaction chan not closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if err = in.WaitForClose(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestReadUntilTimeout(t *testing.T) {
	conf, err := input.FromYAML(`
read_until:
  idle_timeout: 100ms
  input:
    generate:
      count: 1000
      interval: 1s
      mapping: 'root.id = counter()'
`)
	require.NoError(t, err)

	strm, err := bmock.NewManager().NewInput(conf)
	require.NoError(t, err)

	tran, open := <-strm.TransactionChan()
	require.True(t, open)
	require.Len(t, tran.Payload, 1)
	assert.Equal(t, `{"id":1}`, string(tran.Payload[0].AsBytes()))
	require.NoError(t, tran.Ack(context.Background(), nil))

	_, open = <-strm.TransactionChan()
	require.False(t, open)
}
