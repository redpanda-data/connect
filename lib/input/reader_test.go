package input

import (
	"errors"
	"os"
	"reflect"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type mockReader struct {
	msgToSnd types.Message
	ackRcvd  error

	connChan chan error
	readChan chan error
	ackChan  chan error
}

func newMockReader() *mockReader {
	return &mockReader{
		msgToSnd: message.New(nil),
		connChan: make(chan error),
		readChan: make(chan error),
		ackChan:  make(chan error),
	}
}

func (r *mockReader) Connect() error {
	cerr, open := <-r.connChan
	if !open {
		return types.ErrNotConnected
	}
	return cerr
}
func (r *mockReader) Read() (types.Message, error) {
	if err, open := <-r.readChan; err != nil {
		if !open {
			return nil, types.ErrNotConnected
		}
		return nil, err
	}
	return r.msgToSnd.DeepCopy(), nil
}
func (r *mockReader) Acknowledge(err error) error {
	r.ackRcvd = err
	return <-r.ackChan
}
func (r *mockReader) CloseAsync() {}
func (r *mockReader) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------

type readerCantConnect struct{}

func (r readerCantConnect) Connect() error { return types.ErrNotConnected }
func (r readerCantConnect) Read() (types.Message, error) {
	return nil, types.ErrNotConnected
}
func (r readerCantConnect) Acknowledge(err error) error {
	return types.ErrNotConnected
}
func (r readerCantConnect) CloseAsync() {}
func (r readerCantConnect) WaitForClose(time.Duration) error {
	return nil
}

func TestReaderCantConnect(t *testing.T) {
	r, err := NewReader(
		"foo", readerCantConnect{},
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	// We will fail to connect but should still exit immediately.
	r.CloseAsync()
	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------

type readerCantRead struct {
	connected int
}

func (r *readerCantRead) Connect() error {
	r.connected++
	return nil
}
func (r *readerCantRead) Read() (types.Message, error) {
	return nil, types.ErrNotConnected
}
func (r *readerCantRead) Acknowledge(err error) error {
	return types.ErrNotConnected
}
func (r *readerCantRead) CloseAsync() {}
func (r *readerCantRead) WaitForClose(time.Duration) error {
	return nil
}

func TestReaderCantRead(t *testing.T) {
	readerImpl := &readerCantRead{}

	r, err := NewReader(
		"foo", readerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()
	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if readerImpl.connected < 1 {
		t.Errorf("Connected wasn't called enough times: %v", readerImpl.connected)
	}
}

//------------------------------------------------------------------------------

func TestReaderTypeClosedOnConn(t *testing.T) {
	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	go func() {
		select {
		case readerImpl.connChan <- types.ErrTypeClosed:
		case <-time.After(time.Second):
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestReaderTypeClosedOnReconn(t *testing.T) {
	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.readChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.connChan <- types.ErrTypeClosed:
		case <-time.After(time.Second):
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestReaderTypeClosedOnReread(t *testing.T) {
	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.readChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.readChan <- types.ErrTypeClosed:
		case <-time.After(time.Second):
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------

func TestReaderCanReconnect(t *testing.T) {
	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.readChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
		}
	}()

	var ts types.Transaction
	var open bool
	select {
	case ts, open = <-r.TransactionChan():
		if !open {
			t.Fatal("Closed early")
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	select {
	case ts.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()

	go func() {
		select {
		case readerImpl.readChan <- nil:
		case readerImpl.connChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestReaderFailsReconnect(t *testing.T) {
	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.readChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.connChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second * 2):
		}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
		}
	}()

	var ts types.Transaction
	var open bool
	select {
	case ts, open = <-r.TransactionChan():
		if !open {
			t.Fatal("Closed early")
		}
	case <-time.After(time.Second * 2):
		t.Error("Timed out")
	}

	select {
	case ts.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()

	go func() {
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestReaderCloseDuringReconnect(t *testing.T) {
	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case readerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}
	select {
	case readerImpl.readChan <- types.ErrNotConnected:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	go func() {
		select {
		case readerImpl.connChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
		}
		close(readerImpl.connChan)
	}()

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()
	close(readerImpl.readChan)

	if err = r.WaitForClose(time.Second); err != nil {
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		t.Error(err)
	}
}

func TestReaderHappyPath(t *testing.T) {
	exp := [][]byte{[]byte("foo"), []byte("bar")}

	readerImpl := newMockReader()
	readerImpl.msgToSnd = message.New(exp)
	readerImpl.ackRcvd = errors.New("ack not received")

	r, err := NewReader(
		"foo", readerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	select {
	case readerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	go func() {
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
		}
	}()

	var ts types.Transaction
	var open bool

	select {
	case ts, open = <-r.TransactionChan():
		if !open {
			t.Fatal("Chan closed")
		}
		if act := message.GetAllBytes(ts.Payload); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message returned: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	select {
	case ts.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()
	close(readerImpl.readChan)
	close(readerImpl.connChan)

	if err = r.WaitForClose(time.Second); err != nil {
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		t.Fatal(err)
	}

	if readerImpl.ackRcvd != nil {
		t.Error(readerImpl.ackRcvd)
	}
}

func TestReaderSadPath(t *testing.T) {
	exp := [][]byte{[]byte("foo"), []byte("bar")}
	expErr := errors.New("test error")

	readerImpl := newMockReader()
	readerImpl.msgToSnd = message.New(exp)
	readerImpl.ackRcvd = errors.New("ack not received")

	r, err := NewReader(
		"foo", readerImpl,
		log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}

	select {
	case readerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	go func() {
		for {
			select {
			case readerImpl.readChan <- nil:
				select {
				case readerImpl.ackChan <- nil:
				case <-time.After(time.Second):
				}
				return
			case readerImpl.connChan <- nil:
			case <-time.After(time.Second):
			}
		}
	}()

	var ts types.Transaction
	var open bool

	select {
	case ts, open = <-r.TransactionChan():
		if !open {
			t.Fatal("Chan closed")
		}
		if act := message.GetAllBytes(ts.Payload); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message returned: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	select {
	case ts.ResponseChan <- response.NewError(expErr):
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()
	close(readerImpl.readChan)
	close(readerImpl.connChan)

	if err = r.WaitForClose(time.Second); err != nil {
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		t.Fatal(err)
	}

	if actErr := readerImpl.ackRcvd; expErr != actErr {
		t.Errorf("Wrong response received: %v != %v", actErr, expErr)
	}
}
