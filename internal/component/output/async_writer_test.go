package output

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type mockAsyncWriter struct {
	msgsTotal uint64
	msgsRcvd  sync.Map
	connChan  chan error
	writeChan chan error
}

func newAsyncMockWriter() *mockAsyncWriter {
	return &mockAsyncWriter{
		connChan:  make(chan error),
		writeChan: make(chan error),
	}
}

func (w *mockAsyncWriter) Connect(ctx context.Context) error {
	return <-w.connChan
}

func (w *mockAsyncWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	w.msgsRcvd.Store(atomic.AddUint64(&w.msgsTotal, 1), msg)
	return <-w.writeChan
}
func (w *mockAsyncWriter) Close(context.Context) error { return nil }

type writerCantConnect struct{}

func (w writerCantConnect) Connect(ctx context.Context) error {
	return component.ErrNotConnected
}

func (w writerCantConnect) WriteBatch(ctx context.Context, msg message.Batch) error {
	return component.ErrNotConnected
}
func (w writerCantConnect) Close(context.Context) error { return nil }

type writerCantSend struct {
	connected int
}

func (w *writerCantSend) Connect(ctx context.Context) error {
	w.connected++
	return nil
}

func (w *writerCantSend) WriteBatch(ctx context.Context, msg message.Batch) error {
	return component.ErrNotConnected
}
func (w *writerCantSend) Close(context.Context) error { return nil }

//------------------------------------------------------------------------------

func TestAsyncWriterCantConnect(t *testing.T) {
	t.Parallel()

	w, err := NewAsyncWriter("foo", 1, writerCantConnect{}, component.NoopObservability())
	if err != nil {
		t.Fatal(err)
	}

	if err = w.Consume(make(chan message.Transaction)); err != nil {
		t.Error(err)
	}
	if err = w.Consume(nil); err == nil {
		t.Error("Expected error from duplicate receiver call")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	// We will fail to connect but should still exit immediately.
	w.TriggerCloseNow()
	require.NoError(t, w.WaitForClose(ctx))
}

//------------------------------------------------------------------------------

func TestAsyncWriterCantSendClosed(t *testing.T) {
	t.Parallel()

	writerImpl := &writerCantSend{}

	w, err := NewAsyncWriter("foo", 1, writerImpl, component.NoopObservability())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan message.Transaction)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	w.TriggerCloseNow()
	require.NoError(t, w.WaitForClose(ctx))
}

func TestAsyncWriterCantSendClosedChan(t *testing.T) {
	t.Parallel()

	writerImpl := &writerCantSend{}

	w, err := NewAsyncWriter("foo", 1, writerImpl, component.NoopObservability())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan message.Transaction)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	close(msgChan)

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, w.WaitForClose(ctx))
}

//------------------------------------------------------------------------------

func TestAsyncWriterStartClosed(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter("foo", 1, writerImpl, component.NoopObservability())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan message.Transaction)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	select {
	case writerImpl.connChan <- component.ErrTypeClosed:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, w.WaitForClose(ctx))
}

func TestAsyncWriterClosesOnReconn(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter("foo", 1, writerImpl, component.NoopObservability())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan message.Transaction)
	resChan := make(chan error)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	select {
	case writerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	go func() {
		select {
		case writerImpl.writeChan <- component.ErrNotConnected:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case writerImpl.connChan <- component.ErrTypeClosed:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	select {
	case msgChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, w.WaitForClose(ctx))
}

func TestAsyncWriterClosesOnResend(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter("foo", 1, writerImpl, component.NoopObservability())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan message.Transaction)
	resChan := make(chan error)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	select {
	case writerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	go func() {
		select {
		case writerImpl.writeChan <- component.ErrNotConnected:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case writerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case writerImpl.writeChan <- component.ErrTypeClosed:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	select {
	case msgChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, w.WaitForClose(ctx))
}

//------------------------------------------------------------------------------

func TestAsyncWriterCanReconnect(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter("foo", 1, writerImpl, component.NoopObservability())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan message.Transaction)
	resChan := make(chan error)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	select {
	case writerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	go func() {
		select {
		case writerImpl.writeChan <- component.ErrNotConnected:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case writerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case writerImpl.writeChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	select {
	case msgChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Res chan closed")
		}
		if err := res; err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	w.TriggerCloseNow()
	require.NoError(t, w.WaitForClose(ctx))
}

func TestAsyncWriterCanReconnectAsync(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter("foo", 2, writerImpl, component.NoopObservability())
	if err != nil {
		t.Fatal(err)
	}

	msgChan := make(chan message.Transaction)
	resChan := make(chan error)
	resChan2 := make(chan error)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	select {
	case writerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	doneChan := make(chan struct{})
	go func() {
		defer close(doneChan)
		select {
		case writerImpl.writeChan <- component.ErrNotConnected:
		case <-time.After(time.Second * 5):
			t.Error("Timed out")
			return
		}
		select {
		case writerImpl.writeChan <- component.ErrNotConnected:
		case <-time.After(time.Second * 5):
			t.Error("Timed out")
			return
		}
		select {
		case writerImpl.connChan <- nil:
		case <-time.After(time.Second * 5):
			t.Error("Timed out")
			return
		}
		go func() {
			select {
			case writerImpl.connChan <- nil:
			case <-time.After(time.Second * 5):
			}
		}()
		select {
		case writerImpl.writeChan <- nil:
		case <-time.After(time.Second * 5):
			t.Error("Timed out")
			return
		}
		select {
		case writerImpl.writeChan <- nil:
		case <-time.After(time.Second * 5):
			t.Error("Timed out")
		}
	}()

	select {
	case msgChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case msgChan <- message.NewTransaction(message.QuickBatch(nil), resChan2):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Res chan closed")
		}
		if err := res; err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second * 5):
		t.Error("Timed out")
	}
	select {
	case res, open := <-resChan2:
		if !open {
			t.Error("Res chan closed")
		}
		if err := res; err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second * 5):
		t.Error("Timed out")
	}
	<-doneChan

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	w.TriggerCloseNow()
	require.NoError(t, w.WaitForClose(ctx))
}

func TestAsyncWriterCantReconnect(t *testing.T) {
	t.Skip("Takes too long!")
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter("foo", 1, writerImpl, component.NoopObservability())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan message.Transaction)
	resChan := make(chan error)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	go func() {
		select {
		case msgChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	select {
	case writerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}
	select {
	case writerImpl.writeChan <- component.ErrNotConnected:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	select {
	case writerImpl.connChan <- component.ErrNotConnected:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	w.TriggerCloseNow()

	go func() {
		select {
		case writerImpl.connChan <- component.ErrNotConnected:
		case <-time.After(time.Second):
		}
	}()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, w.WaitForClose(ctx))
}

func TestAsyncWriterHappyPath(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	w, err := NewAsyncWriter("foo", 1, writerImpl, component.NoopObservability())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan message.Transaction)
	resChan := make(chan error)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	go func() {
		select {
		case msgChan <- message.NewTransaction(message.QuickBatch(exp), resChan):
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	select {
	case writerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}
	select {
	case writerImpl.writeChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	select {
	case res, open := <-resChan:
		require.True(t, open)
		require.NoError(t, res)
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	// We will be failing to send but should still exit immediately.
	w.TriggerCloseNow()
	require.NoError(t, w.WaitForClose(ctx))

	msgRcvd, exists := writerImpl.msgsRcvd.Load(uint64(1))
	require.True(t, exists)

	if act := message.GetAllBytes(msgRcvd.(message.Batch)); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message sent: %v != %v", act, exp)
	}
}

func TestAsyncWriterSadPath(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	expErr := errors.New("message got lost or something")

	w, err := NewAsyncWriter("foo", 1, writerImpl, component.NoopObservability())
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan message.Transaction)
	resChan := make(chan error)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	go func() {
		select {
		case msgChan <- message.NewTransaction(message.QuickBatch(exp), resChan):
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	select {
	case writerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}
	select {
	case writerImpl.writeChan <- expErr:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	select {
	case res, open := <-resChan:
		if !open {
			t.Fatal("Chan closed")
		}
		if actErr := res; expErr != actErr {
			t.Errorf("Wrong response: %v != %v", actErr, expErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	w.TriggerCloseNow()
	require.NoError(t, w.WaitForClose(ctx))

	msgRcvd, exists := writerImpl.msgsRcvd.Load(uint64(1))
	require.True(t, exists)

	if act := message.GetAllBytes(msgRcvd.(message.Batch)); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message sent: %v != %v", act, exp)
	}
}
