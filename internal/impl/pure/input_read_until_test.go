package pure_test

import (
	"context"
	"errors"
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
	conf := input.NewConfig()
	conf.Type = "read_until"

	inConf := input.NewConfig()
	conf.ReadUntil.Input = &inConf

	_, err := bmock.NewManager().NewInput(conf)
	assert.EqualError(t, err, "failed to init input <no label>: a check query is required")
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

	inconf := input.NewConfig()
	inconf.Type = "file"
	inconf.File.Paths = []string{tmpfile.Name()}

	t.Run("ReadUntilBasic", func(te *testing.T) {
		testReadUntilBasic(inconf, te)
	})
	t.Run("ReadUntilRestart", func(te *testing.T) {
		testReadUntilRestart(inconf, te)
	})
	t.Run("ReadUntilRetry", func(te *testing.T) {
		testReadUntilRetry(inconf, te)
	})
}

func testReadUntilBasic(inConf input.Config, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	rConf := input.NewConfig()
	rConf.Type = "read_until"
	rConf.ReadUntil.Input = &inConf
	rConf.ReadUntil.Check = `content() == "bar"`

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

func testReadUntilRestart(inConf input.Config, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	rConf := input.NewConfig()
	rConf.Type = "read_until"
	rConf.ReadUntil.Input = &inConf
	rConf.ReadUntil.Check = `false`
	rConf.ReadUntil.Restart = true

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

func testReadUntilRetry(inConf input.Config, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	rConf := input.NewConfig()
	rConf.Type = "read_until"
	rConf.ReadUntil.Input = &inConf
	rConf.ReadUntil.Check = `content() == "bar"`

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
