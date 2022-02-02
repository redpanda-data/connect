package output

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/require"
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

func (w *mockAsyncWriter) ConnectWithContext(ctx context.Context) error {
	return <-w.connChan
}
func (w *mockAsyncWriter) WriteWithContext(ctx context.Context, msg types.Message) error {
	w.msgsRcvd.Store(atomic.AddUint64(&w.msgsTotal, 1), msg)
	return <-w.writeChan
}
func (w *mockAsyncWriter) CloseAsync() {}
func (w *mockAsyncWriter) WaitForClose(time.Duration) error {
	return nil
}

type writerCantConnect struct{}

func (w writerCantConnect) ConnectWithContext(ctx context.Context) error {
	return types.ErrNotConnected
}
func (w writerCantConnect) WriteWithContext(ctx context.Context, msg types.Message) error {
	return types.ErrNotConnected
}
func (w writerCantConnect) CloseAsync() {}
func (w writerCantConnect) WaitForClose(time.Duration) error {
	return nil
}

type writerCantSend struct {
	connected int
}

func (w *writerCantSend) ConnectWithContext(ctx context.Context) error {
	w.connected++
	return nil
}
func (w *writerCantSend) WriteWithContext(ctx context.Context, msg types.Message) error {
	return types.ErrNotConnected
}
func (w *writerCantSend) CloseAsync() {}
func (w *writerCantSend) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------

func TestAsyncWriterCantConnect(t *testing.T) {
	t.Parallel()

	w, err := NewAsyncWriter(
		"foo", 1, writerCantConnect{},
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err = w.Consume(make(chan types.Transaction)); err != nil {
		t.Error(err)
	}
	if err = w.Consume(nil); err == nil {
		t.Error("Expected error from duplicate receiver call")
	}

	// We will fail to connect but should still exit immediately.
	w.CloseAsync()
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------

func TestAsyncWriterCantSendClosed(t *testing.T) {
	t.Parallel()

	writerImpl := &writerCantSend{}

	w, err := NewAsyncWriter(
		"foo", 1, writerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	w.CloseAsync()
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestAsyncWriterCantSendClosedChan(t *testing.T) {
	t.Parallel()

	writerImpl := &writerCantSend{}

	w, err := NewAsyncWriter(
		"foo", 1, writerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	close(msgChan)
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------

func TestAsyncWriterStartClosed(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter(
		"foo", 1, writerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	select {
	case writerImpl.connChan <- types.ErrTypeClosed:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestAsyncWriterClosesOnReconn(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter(
		"foo", 1, writerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

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
		case writerImpl.writeChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case writerImpl.connChan <- types.ErrTypeClosed:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	select {
	case msgChan <- types.NewTransaction(message.New(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	if err = w.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestAsyncWriterClosesOnResend(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter(
		"foo", 1, writerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

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
		case writerImpl.writeChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case writerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case writerImpl.writeChan <- types.ErrTypeClosed:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	select {
	case msgChan <- types.NewTransaction(message.New(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	if err = w.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------

func TestAsyncWriterCanReconnect(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter(
		"foo", 1, writerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

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
		case writerImpl.writeChan <- types.ErrNotConnected:
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
	case msgChan <- types.NewTransaction(message.New(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Res chan closed")
		}
		if err := res.Error(); err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	w.CloseAsync()
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestAsyncWriterCanReconnectAsync(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter(
		"foo", 2, writerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Fatal(err)
	}

	msgChan := make(chan types.Transaction)
	resChan := make(chan types.Response)
	resChan2 := make(chan types.Response)

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
		case writerImpl.writeChan <- types.ErrNotConnected:
		case <-time.After(time.Second * 5):
			t.Error("Timed out")
			return
		}
		select {
		case writerImpl.writeChan <- types.ErrNotConnected:
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
	case msgChan <- types.NewTransaction(message.New(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case msgChan <- types.NewTransaction(message.New(nil), resChan2):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Res chan closed")
		}
		if err := res.Error(); err != nil {
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
		if err := res.Error(); err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second * 5):
		t.Error("Timed out")
	}
	<-doneChan

	w.CloseAsync()
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestAsyncWriterCantReconnect(t *testing.T) {
	t.Skip("Takes too long!")
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	w, err := NewAsyncWriter(
		"foo", 1, writerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	go func() {
		select {
		case msgChan <- types.NewTransaction(message.New(nil), resChan):
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
	case writerImpl.writeChan <- types.ErrNotConnected:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	select {
	case writerImpl.connChan <- types.ErrNotConnected:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	w.CloseAsync()

	go func() {
		select {
		case writerImpl.connChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
		}
	}()

	if err = w.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestAsyncWriterHappyPath(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	w, err := NewAsyncWriter(
		"foo", 1, writerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	go func() {
		select {
		case msgChan <- types.NewTransaction(message.New(exp), resChan):
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
		require.NoError(t, res.Error())
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	w.CloseAsync()
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	msgRcvd, exists := writerImpl.msgsRcvd.Load(uint64(1))
	require.True(t, exists)

	if act := message.GetAllBytes(msgRcvd.(types.Message)); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message sent: %v != %v", act, exp)
	}
}

func TestAsyncWriterSadPath(t *testing.T) {
	t.Parallel()

	writerImpl := newAsyncMockWriter()

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	expErr := errors.New("message got lost or something")

	w, err := NewAsyncWriter(
		"foo", 1, writerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = w.Consume(msgChan); err != nil {
		t.Error(err)
	}

	go func() {
		select {
		case msgChan <- types.NewTransaction(message.New(exp), resChan):
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
		if actErr := res.Error(); expErr != actErr {
			t.Errorf("Wrong response: %v != %v", actErr, expErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	w.CloseAsync()
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	msgRcvd, exists := writerImpl.msgsRcvd.Load(uint64(1))
	require.True(t, exists)

	if act := message.GetAllBytes(msgRcvd.(types.Message)); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message sent: %v != %v", act, exp)
	}
}
