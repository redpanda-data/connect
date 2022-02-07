package output

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockNBWriter struct {
	t           *testing.T
	written     []string
	errorOn     []string
	closeCalled bool
	closeChan   chan error
	mut         sync.Mutex
}

func (m *mockNBWriter) ConnectWithContext(context.Context) error {
	return nil
}

func (m *mockNBWriter) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	m.t.Helper()
	assert.Equal(m.t, 1, msg.Len())
	return msg.Iter(func(i int, p *message.Part) error {
		for _, eOn := range m.errorOn {
			if eOn == string(p.Get()) {
				return errors.New("test err")
			}
		}
		m.written = append(m.written, string(p.Get()))
		return nil
	})
}

func (m *mockNBWriter) CloseAsync() {
	m.mut.Lock()
	m.closeCalled = true
	m.mut.Unlock()
}

func (m *mockNBWriter) WaitForClose(time.Duration) error {
	if m.closeChan == nil {
		return nil
	}
	return <-m.closeChan
}

func TestNotBatchedSingleMessages(t *testing.T) {
	msg := func(c string) *message.Batch {
		p := message.NewPart([]byte(c))
		msg := message.QuickBatch(nil)
		msg.Append(p)
		return msg
	}

	w := &mockNBWriter{t: t}
	out, err := NewAsyncWriter("foo", 1, w, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	nbOut := OnlySinglePayloads(out)

	resChan := make(chan types.Response)
	tChan := make(chan types.Transaction)
	require.NoError(t, nbOut.Consume(tChan))

	for i := 0; i < 5; i++ {
		select {
		case tChan <- types.NewTransaction(msg(fmt.Sprintf("foo%v", i)), resChan):
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
		select {
		case res := <-resChan:
			assert.NoError(t, res.Error())
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	nbOut.CloseAsync()
	assert.NoError(t, nbOut.WaitForClose(time.Second))
	assert.Equal(t, []string{
		"foo0", "foo1", "foo2", "foo3", "foo4",
	}, w.written)
}

func TestShutdown(t *testing.T) {
	msg := func(c string) *message.Batch {
		p := message.NewPart([]byte(c))
		msg := message.QuickBatch(nil)
		msg.Append(p)
		return msg
	}

	w := &mockNBWriter{t: t, closeChan: make(chan error)}
	out, err := NewAsyncWriter("foo", 1, w, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	nbOut := OnlySinglePayloads(out)

	resChan := make(chan types.Response)
	tChan := make(chan types.Transaction)
	require.NoError(t, nbOut.Consume(tChan))

	select {
	case tChan <- types.NewTransaction(msg("foo"), resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	select {
	case res := <-resChan:
		assert.NoError(t, res.Error())
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	nbOut.CloseAsync()
	assert.EqualError(t, nbOut.WaitForClose(time.Millisecond*100), "action timed out")

	select {
	case w.closeChan <- errors.New("custom err"):
	case <-time.After(time.Second):
		t.Error("timed out")
	}

	assert.NoError(t, nbOut.WaitForClose(time.Millisecond*100))
	assert.Equal(t, []string{"foo"}, w.written)
	w.mut.Lock()
	assert.True(t, w.closeCalled)
	w.mut.Unlock()
}

func TestNotBatchedBreakOutMessages(t *testing.T) {
	msg := func(c ...string) *message.Batch {
		msg := message.QuickBatch(nil)
		for _, str := range c {
			p := message.NewPart([]byte(str))
			msg.Append(p)
		}
		return msg
	}

	w := &mockNBWriter{t: t}
	out, err := NewAsyncWriter("foo", 1, w, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	nbOut := OnlySinglePayloads(out)

	resChan := make(chan types.Response)
	tChan := make(chan types.Transaction)
	require.NoError(t, nbOut.Consume(tChan))

	select {
	case tChan <- types.NewTransaction(msg(
		"foo0", "foo1", "foo2", "foo3", "foo4",
	), resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	select {
	case res := <-resChan:
		assert.NoError(t, res.Error())
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	nbOut.CloseAsync()
	assert.NoError(t, nbOut.WaitForClose(time.Second))
	assert.Equal(t, []string{
		"foo0", "foo1", "foo2", "foo3", "foo4",
	}, w.written)
}

func TestNotBatchedBreakOutMessagesErrors(t *testing.T) {
	msg := func(c ...string) *message.Batch {
		msg := message.QuickBatch(nil)
		for _, str := range c {
			p := message.NewPart([]byte(str))
			msg.Append(p)
		}
		return msg
	}

	w := &mockNBWriter{t: t, errorOn: []string{"foo1", "foo3"}}
	out, err := NewAsyncWriter("foo", 1, w, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	nbOut := OnlySinglePayloads(out)

	resChan := make(chan types.Response)
	tChan := make(chan types.Transaction)
	require.NoError(t, nbOut.Consume(tChan))

	select {
	case tChan <- types.NewTransaction(msg(
		"foo0", "foo1", "foo2", "foo3", "foo4",
	), resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	select {
	case res := <-resChan:
		err := res.Error()
		require.Error(t, err)

		walkable, ok := err.(batch.WalkableError)
		require.True(t, ok)

		errs := map[int]string{}
		walkable.WalkParts(func(i int, _ *message.Part, err error) bool {
			if err != nil {
				errs[i] = err.Error()
			}
			return true
		})
		assert.Equal(t, map[int]string{
			1: "test err",
			3: "test err",
		}, errs)
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	nbOut.CloseAsync()
	assert.NoError(t, nbOut.WaitForClose(time.Second))
	assert.Equal(t, []string{
		"foo0", "foo2", "foo4",
	}, w.written)
}

func TestNotBatchedBreakOutMessagesErrorsAsync(t *testing.T) {
	msg := func(c ...string) *message.Batch {
		msg := message.QuickBatch(nil)
		for _, str := range c {
			p := message.NewPart([]byte(str))
			msg.Append(p)
		}
		return msg
	}

	w := &mockNBWriter{t: t, errorOn: []string{"foo1", "foo3"}}
	out, err := NewAsyncWriter("foo", 5, w, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	nbOut := OnlySinglePayloads(out)

	resChan := make(chan types.Response)
	tChan := make(chan types.Transaction)
	require.NoError(t, nbOut.Consume(tChan))

	select {
	case tChan <- types.NewTransaction(msg(
		"foo0", "foo1", "foo2", "foo3", "foo4",
	), resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	select {
	case res := <-resChan:
		err := res.Error()
		require.Error(t, err)

		walkable, ok := err.(batch.WalkableError)
		require.True(t, ok)

		errs := map[int]string{}
		walkable.WalkParts(func(i int, _ *message.Part, err error) bool {
			if err != nil {
				errs[i] = err.Error()
			}
			return true
		})
		assert.Equal(t, map[int]string{
			1: "test err",
			3: "test err",
		}, errs)
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	nbOut.CloseAsync()
	assert.NoError(t, nbOut.WaitForClose(time.Second))
	sort.Strings(w.written)
	assert.Equal(t, []string{
		"foo0", "foo2", "foo4",
	}, w.written)
}
