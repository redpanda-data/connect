package output

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type mockNBWriter struct {
	t           *testing.T
	written     []string
	errorOn     []string
	closeCalled bool
	closeChan   chan error
	mut         sync.Mutex
}

func (m *mockNBWriter) Connect(context.Context) error {
	return nil
}

func (m *mockNBWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	m.t.Helper()
	assert.Equal(m.t, 1, msg.Len())
	return msg.Iter(func(i int, p *message.Part) error {
		for _, eOn := range m.errorOn {
			if eOn == string(p.AsBytes()) {
				return errors.New("test err")
			}
		}
		m.written = append(m.written, string(p.AsBytes()))
		return nil
	})
}

func (m *mockNBWriter) Close(ctx context.Context) error {
	m.mut.Lock()
	m.closeCalled = true
	m.mut.Unlock()
	if m.closeChan == nil {
		return nil
	}
	return <-m.closeChan
}

func TestNotBatchedSingleMessages(t *testing.T) {
	msg := func(c string) message.Batch {
		p := message.NewPart([]byte(c))
		msg := message.Batch{p}
		return msg
	}

	w := &mockNBWriter{t: t}
	out, err := NewAsyncWriter("foo", 1, w, component.NoopObservability())
	require.NoError(t, err)

	nbOut := OnlySinglePayloads(out)

	resChan := make(chan error)
	tChan := make(chan message.Transaction)
	require.NoError(t, nbOut.Consume(tChan))

	for i := 0; i < 5; i++ {
		select {
		case tChan <- message.NewTransaction(msg(fmt.Sprintf("foo%v", i)), resChan):
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
		select {
		case res := <-resChan:
			assert.NoError(t, res)
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nbOut.TriggerCloseNow()
	assert.NoError(t, nbOut.WaitForClose(ctx))
	assert.Equal(t, []string{
		"foo0", "foo1", "foo2", "foo3", "foo4",
	}, w.written)
}

func TestShutdown(t *testing.T) {
	msg := func(c string) message.Batch {
		p := message.NewPart([]byte(c))
		msg := message.Batch{p}
		return msg
	}

	w := &mockNBWriter{t: t, closeChan: make(chan error)}
	out, err := NewAsyncWriter("foo", 1, w, component.NoopObservability())
	require.NoError(t, err)

	nbOut := OnlySinglePayloads(out)

	resChan := make(chan error)
	tChan := make(chan message.Transaction)
	require.NoError(t, nbOut.Consume(tChan))

	select {
	case tChan <- message.NewTransaction(msg("foo"), resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	select {
	case res := <-resChan:
		assert.NoError(t, res)
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	done()

	nbOut.TriggerCloseNow()
	assert.EqualError(t, nbOut.WaitForClose(ctx), "context canceled")

	select {
	case w.closeChan <- errors.New("custom err"):
	case <-time.After(time.Second):
		t.Error("timed out")
	}

	ctx, done = context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	assert.NoError(t, nbOut.WaitForClose(ctx))
	assert.Equal(t, []string{"foo"}, w.written)
	w.mut.Lock()
	assert.True(t, w.closeCalled)
	w.mut.Unlock()
}

func TestNotBatchedBreakOutMessages(t *testing.T) {
	msg := func(c ...string) message.Batch {
		msg := message.QuickBatch(nil)
		for _, str := range c {
			msg = append(msg, message.NewPart([]byte(str)))
		}
		return msg
	}

	w := &mockNBWriter{t: t}
	out, err := NewAsyncWriter("foo", 1, w, component.NoopObservability())
	require.NoError(t, err)

	nbOut := OnlySinglePayloads(out)

	resChan := make(chan error)
	tChan := make(chan message.Transaction)
	require.NoError(t, nbOut.Consume(tChan))

	select {
	case tChan <- message.NewTransaction(msg(
		"foo0", "foo1", "foo2", "foo3", "foo4",
	), resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	select {
	case res := <-resChan:
		assert.NoError(t, res)
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nbOut.TriggerCloseNow()
	assert.NoError(t, nbOut.WaitForClose(ctx))
	assert.Equal(t, []string{
		"foo0", "foo1", "foo2", "foo3", "foo4",
	}, w.written)
}

func TestNotBatchedBreakOutMessagesErrors(t *testing.T) {
	msg := func(c ...string) message.Batch {
		msg := message.QuickBatch(nil)
		for _, str := range c {
			msg = append(msg, message.NewPart([]byte(str)))
		}
		return msg
	}

	w := &mockNBWriter{t: t, errorOn: []string{"foo1", "foo3"}}
	out, err := NewAsyncWriter("foo", 1, w, component.NoopObservability())
	require.NoError(t, err)

	nbOut := OnlySinglePayloads(out)

	resChan := make(chan error)
	tChan := make(chan message.Transaction)
	require.NoError(t, nbOut.Consume(tChan))

	sourceMessage := msg("foo0", "foo1", "foo2", "foo3", "foo4")
	sortGroup, sourceMessage := message.NewSortGroup(sourceMessage)

	select {
	case tChan <- message.NewTransaction(sourceMessage, resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	select {
	case res := <-resChan:
		err := res
		require.Error(t, err)

		walkable, ok := err.(*batch.Error)
		require.True(t, ok)

		errs := map[int]string{}
		walkable.WalkParts(sortGroup, sourceMessage, func(i int, _ *message.Part, err error) bool {
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

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nbOut.TriggerCloseNow()
	assert.NoError(t, nbOut.WaitForClose(ctx))
	assert.Equal(t, []string{
		"foo0", "foo2", "foo4",
	}, w.written)
}

func TestNotBatchedBreakOutMessagesErrorsAsync(t *testing.T) {
	msg := func(c ...string) message.Batch {
		msg := message.QuickBatch(nil)
		for _, str := range c {
			msg = append(msg, message.NewPart([]byte(str)))
		}
		return msg
	}

	w := &mockNBWriter{t: t, errorOn: []string{"foo1", "foo3"}}
	out, err := NewAsyncWriter("foo", 5, w, component.NoopObservability())
	require.NoError(t, err)

	nbOut := OnlySinglePayloads(out)

	resChan := make(chan error)
	tChan := make(chan message.Transaction)
	require.NoError(t, nbOut.Consume(tChan))

	sourceMessage := msg("foo0", "foo1", "foo2", "foo3", "foo4")
	sortGroup, sourceMessage := message.NewSortGroup(sourceMessage)

	select {
	case tChan <- message.NewTransaction(sourceMessage, resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	select {
	case res := <-resChan:
		err := res
		require.Error(t, err)

		walkable, ok := err.(*batch.Error)
		require.True(t, ok)

		errs := map[int]string{}
		walkable.WalkParts(sortGroup, sourceMessage, func(i int, _ *message.Part, err error) bool {
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

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nbOut.TriggerCloseNow()
	assert.NoError(t, nbOut.WaitForClose(ctx))
	sort.Strings(w.written)
	assert.Equal(t, []string{
		"foo0", "foo2", "foo4",
	}, w.written)
}
