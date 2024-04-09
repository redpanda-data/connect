package pure

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestRetryHappy(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
retry:
  processors:
    - resource: foo
`)
	require.NoError(t, err)

	mockMgr := mock.NewManager()
	mockMgr.Processors["foo"] = func(b message.Batch) ([]message.Batch, error) {
		b[0].SetBytes([]byte(string(b[0].AsBytes()) + " updated"))
		return []message.Batch{
			{b[0]},
		}, nil
	}

	p, err := mockMgr.NewProcessor(conf)
	require.NoError(t, err)

	resBatches, err := p.ProcessBatch(context.Background(), message.Batch{
		message.NewPart([]byte("hello world a")),
		message.NewPart([]byte("hello world b")),
		message.NewPart([]byte("hello world c")),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 3)

	var resMsgs []string
	for _, m := range resBatches[0] {
		resMsgs = append(resMsgs, string(m.AsBytes()))
	}
	assert.Equal(t, []string{
		"hello world a updated",
		"hello world b updated",
		"hello world c updated",
	}, resMsgs)

	require.NoError(t, p.Close(context.Background()))
}

func TestRetryVerySad(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
retry:
  backoff:
    initial_interval: 1ms
    max_interval: 10ms
    max_elapsed_time: 100ms
  processors:
    - resource: foo
`)
	require.NoError(t, err)

	mockMgr := mock.NewManager()

	var fooCalls uint32
	mockMgr.Processors["foo"] = func(b message.Batch) ([]message.Batch, error) {
		b[0].SetBytes([]byte(string(b[0].AsBytes()) + " updated"))
		b[0].ErrorSet(errors.New("nope"))
		atomic.AddUint32(&fooCalls, 1)
		return []message.Batch{
			{b[0]},
		}, nil
	}

	p, err := mockMgr.NewProcessor(conf)
	require.NoError(t, err)

	resBatches, err := p.ProcessBatch(context.Background(), message.Batch{
		message.NewPart([]byte("hello world a")),
		message.NewPart([]byte("hello world b")),
		message.NewPart([]byte("hello world c")),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 3)

	var resMsgs []string
	for _, m := range resBatches[0] {
		resMsgs = append(resMsgs, string(m.AsBytes()))
	}
	assert.Equal(t, []string{
		"hello world a updated",
		"hello world b updated",
		"hello world c updated",
	}, resMsgs)

	assert.Greater(t, fooCalls, uint32(6))

	require.NoError(t, p.Close(context.Background()))
}

func TestRetryOneFailure(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
retry:
  backoff:
    initial_interval: 1ms
    max_interval: 10ms
  processors:
    - resource: foo
`)
	require.NoError(t, err)

	mockMgr := mock.NewManager()

	var fooCalls uint32
	mockMgr.Processors["foo"] = func(b message.Batch) ([]message.Batch, error) {
		b[0].SetBytes([]byte(string(b[0].AsBytes()) + " updated"))
		if atomic.AddUint32(&fooCalls, 1) == 1 {
			b[0].ErrorSet(errors.New("nope"))
		}
		return []message.Batch{
			{b[0]},
		}, nil
	}

	p, err := mockMgr.NewProcessor(conf)
	require.NoError(t, err)

	resBatches, err := p.ProcessBatch(context.Background(), message.Batch{
		message.NewPart([]byte("hello world a")),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 1)

	var resMsgs []string
	for _, m := range resBatches[0] {
		resMsgs = append(resMsgs, string(m.AsBytes()))
	}
	assert.Equal(t, []string{
		"hello world a updated",
	}, resMsgs)

	assert.Greater(t, fooCalls, uint32(1))

	require.NoError(t, p.Close(context.Background()))
}

func TestRetryParallelErrors(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
retry:
  backoff:
    initial_interval: 100ms
    max_interval: 100ms
    max_elapsed_time: 1s
  parallel: true
  processors:
    - resource: foo
`)
	require.NoError(t, err)

	mockMgr := mock.NewManager()

	var fooCalls, barCalls, bazCalls uint32
	mockMgr.Processors["foo"] = func(b message.Batch) ([]message.Batch, error) {
		mContent := string(b[0].AsBytes())
		b[0].SetBytes([]byte(mContent + " updated"))
		var calls uint32
		switch mContent {
		case "foo":
			calls = atomic.AddUint32(&fooCalls, 1)
		case "bar":
			calls = atomic.AddUint32(&barCalls, 1)
		case "baz":
			calls = atomic.AddUint32(&bazCalls, 1)
		}
		if calls == 1 {
			b[0].ErrorSet(errors.New("nope"))
		}
		return []message.Batch{
			{b[0]},
		}, nil
	}

	p, err := mockMgr.NewProcessor(conf)
	require.NoError(t, err)

	tBefore := time.Now()
	resBatches, err := p.ProcessBatch(context.Background(), message.Batch{
		message.NewPart([]byte("foo")),
		message.NewPart([]byte("bar")),
		message.NewPart([]byte("baz")),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 3)

	tTaken := time.Since(tBefore)
	assert.Less(t, tTaken, time.Millisecond*200)

	var resMsgs []string
	for _, m := range resBatches[0] {
		resMsgs = append(resMsgs, string(m.AsBytes()))
	}
	assert.Equal(t, []string{
		"foo updated",
		"bar updated",
		"baz updated",
	}, resMsgs)

	assert.Equal(t, uint32(2), fooCalls)
	assert.Equal(t, uint32(2), barCalls)
	assert.Equal(t, uint32(2), bazCalls)

	require.NoError(t, p.Close(context.Background()))
}
