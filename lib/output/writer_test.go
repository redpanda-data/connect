package output

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type mockWriter struct {
	resToSnd error
	msgRcvd  types.Message

	connChan  chan error
	writeChan chan error
}

func newMockWriter() *mockWriter {
	return &mockWriter{
		connChan:  make(chan error),
		writeChan: make(chan error),
	}
}

func (w *mockWriter) ConnectWithContext(ctx context.Context) error {
	return w.Connect()
}
func (w *mockWriter) Connect() error {
	return <-w.connChan
}
func (w *mockWriter) WriteWithContext(ctx context.Context, msg types.Message) error {
	return w.Write(msg)
}
func (w *mockWriter) Write(msg types.Message) error {
	w.msgRcvd = msg
	return <-w.writeChan
}
func (w *mockWriter) CloseAsync() {}
func (w *mockWriter) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------

type writerCantConnect struct{}

func (w writerCantConnect) ConnectWithContext(ctx context.Context) error {
	return w.Connect()
}
func (w writerCantConnect) Connect() error { return types.ErrNotConnected }
func (w writerCantConnect) WriteWithContext(ctx context.Context, msg types.Message) error {
	return w.Write(msg)
}
func (w writerCantConnect) Write(msg types.Message) error {
	return types.ErrNotConnected
}
func (w writerCantConnect) CloseAsync() {}
func (w writerCantConnect) WaitForClose(time.Duration) error {
	return nil
}

func TestWriterCantConnect(t *testing.T) {
	t.Parallel()

	w, err := NewWriter(
		"foo", writerCantConnect{},
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
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

type writerCantSend struct {
	connected int
}

func (w *writerCantSend) ConnectWithContext(ctx context.Context) error {
	return w.Connect()
}
func (w *writerCantSend) Connect() error {
	w.connected++
	return nil
}
func (w *writerCantSend) WriteWithContext(ctx context.Context, msg types.Message) error {
	return w.Write(msg)
}
func (w *writerCantSend) Write(msg types.Message) error {
	return types.ErrNotConnected
}
func (w *writerCantSend) CloseAsync() {}
func (w *writerCantSend) WaitForClose(time.Duration) error {
	return nil
}

func TestWriterCantSendClosed(t *testing.T) {
	t.Parallel()

	writerImpl := &writerCantSend{}

	w, err := NewWriter(
		"foo", writerImpl,
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

func TestWriterCantSendClosedChan(t *testing.T) {
	t.Parallel()

	writerImpl := &writerCantSend{}

	w, err := NewWriter(
		"foo", writerImpl,
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

func TestWriterStartClosed(t *testing.T) {
	t.Parallel()

	writerImpl := newMockWriter()

	w, err := NewWriter(
		"foo", writerImpl,
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

func TestWriterClosesOnReconn(t *testing.T) {
	t.Parallel()

	writerImpl := newMockWriter()

	w, err := NewWriter(
		"foo", writerImpl,
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

func TestWriterClosesOnResend(t *testing.T) {
	t.Parallel()

	writerImpl := newMockWriter()

	w, err := NewWriter(
		"foo", writerImpl,
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

func TestWriterCanReconnect(t *testing.T) {
	t.Parallel()

	writerImpl := newMockWriter()

	w, err := NewWriter(
		"foo", writerImpl,
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

func TestWriterCantReconnect(t *testing.T) {
	t.Skip("Takes too long!")
	t.Parallel()

	writerImpl := newMockWriter()

	w, err := NewWriter(
		"foo", writerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Fatal(err)
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

func TestWriterHappyPath(t *testing.T) {
	t.Parallel()

	writerImpl := newMockWriter()

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	expErr := error(nil)

	writerImpl.resToSnd = expErr

	w, err := NewWriter(
		"foo", writerImpl,
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

	if act := message.GetAllBytes(writerImpl.msgRcvd); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message sent: %v != %v", act, exp)
	}
}

func TestWriterSadPath(t *testing.T) {
	t.Parallel()

	writerImpl := newMockWriter()

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	expErr := errors.New("message got lost or something")

	writerImpl.resToSnd = expErr

	w, err := NewWriter(
		"foo", writerImpl,
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

	if act := message.GetAllBytes(writerImpl.msgRcvd); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message sent: %v != %v", act, exp)
	}
}

//------------------------------------------------------------------------------
