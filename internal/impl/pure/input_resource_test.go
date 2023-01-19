package pure_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestResourceInput(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	mgr := mock.NewManager()
	mgr.Inputs["foo"] = mock.NewInput([]message.Batch{
		{message.NewPart([]byte("hello world 1"))},
		{message.NewPart([]byte("hello world 2"))},
		{message.NewPart([]byte("hello world 3"))},
	})

	nConf := input.NewConfig()
	nConf.Type = "resource"
	nConf.Resource = "foo"

	p, err := mgr.NewInput(nConf)
	require.NoError(t, err)

	tChan := p.TransactionChan()
	readTran := func() message.Transaction {
		select {
		case tran, open := <-tChan:
			require.True(t, open)
			return tran
		case <-ctx.Done():
			t.Fatal("timed out")
		}
		return message.Transaction{}
	}

	tr := readTran()
	require.Len(t, tr.Payload, 1)
	assert.Equal(t, "hello world 1", string(tr.Payload[0].AsBytes()))
	require.NoError(t, tr.Ack(ctx, nil))

	tr = readTran()
	require.Len(t, tr.Payload, 1)
	assert.Equal(t, "hello world 2", string(tr.Payload[0].AsBytes()))
	require.NoError(t, tr.Ack(ctx, nil))

	tr = readTran()
	require.Len(t, tr.Payload, 1)
	assert.Equal(t, "hello world 3", string(tr.Payload[0].AsBytes()))
	require.NoError(t, tr.Ack(ctx, nil))

	select {
	case _, open := <-tChan:
		assert.False(t, open)
	case <-ctx.Done():
		t.Error("timed out")
	}
	assert.NoError(t, p.WaitForClose(ctx))
}

func TestResourceInputEarlyTermination(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	mgr := mock.NewManager()
	mgr.Inputs["foo"] = mock.NewInput([]message.Batch{
		{message.NewPart([]byte("hello world 1"))},
		{message.NewPart([]byte("hello world 2"))},
		{message.NewPart([]byte("hello world 3"))},
	})

	nConf := input.NewConfig()
	nConf.Type = "resource"
	nConf.Resource = "foo"

	p, err := mgr.NewInput(nConf)
	require.NoError(t, err)

	tChan := p.TransactionChan()
	readTran := func() message.Transaction {
		select {
		case tran, open := <-tChan:
			require.True(t, open)
			return tran
		case <-ctx.Done():
			t.Fatal("timed out")
		}
		return message.Transaction{}
	}

	tr := readTran()
	require.Len(t, tr.Payload, 1)
	assert.Equal(t, "hello world 1", string(tr.Payload[0].AsBytes()))
	require.NoError(t, tr.Ack(ctx, nil))

	p.TriggerStopConsuming()

	assert.Eventually(t, func() bool {
		select {
		case _, open := <-tChan:
			return !open
		case <-ctx.Done():
			return false
		}
	}, time.Second, time.Millisecond)

	assert.NoError(t, p.WaitForClose(ctx))
}

func TestResourceInputBadName(t *testing.T) {
	conf := input.NewConfig()
	conf.Type = "resource"
	conf.Resource = "foo"

	_, err := mock.NewManager().NewInput(conf)
	if err == nil {
		t.Error("expected error from bad resource")
	}
}
