package input

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//------------------------------------------------------------------------------

type mockAsyncReader struct {
	msgsToSnd []types.Message
	ackRcvd   []error
	ackMut    sync.Mutex

	connChan       chan error
	readChan       chan error
	ackChan        chan error
	closeAsyncChan chan struct{}
	closeAsyncOnce sync.Once
}

func newMockAsyncReader() *mockAsyncReader {
	return &mockAsyncReader{
		connChan:       make(chan error),
		readChan:       make(chan error),
		ackChan:        make(chan error),
		closeAsyncChan: make(chan struct{}),
	}
}

func (r *mockAsyncReader) ConnectWithContext(ctx context.Context) error {
	cerr, open := <-r.connChan
	if !open {
		return types.ErrNotConnected
	}
	return cerr
}

func (r *mockAsyncReader) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	select {
	case <-ctx.Done():
		return nil, nil, types.ErrTimeout
	case err, open := <-r.readChan:
		if !open {
			return nil, nil, types.ErrNotConnected
		}
		if err != nil {
			return nil, nil, err
		}
	}
	r.ackMut.Lock()
	r.ackRcvd = append(r.ackRcvd, errors.New("ack not received"))
	i := len(r.ackRcvd) - 1
	r.ackMut.Unlock()

	var nextMsg types.Message = message.New(nil)
	if len(r.msgsToSnd) > 0 {
		nextMsg = r.msgsToSnd[0]
		r.msgsToSnd = r.msgsToSnd[1:]
	}

	return nextMsg.DeepCopy(), func(ctx context.Context, res types.Response) error {
		r.ackMut.Lock()
		r.ackRcvd[i] = res.Error()
		r.ackMut.Unlock()
		select {
		case err := <-r.ackChan:
			return err
		case <-ctx.Done():
		}
		return nil
	}, nil
}

func (r *mockAsyncReader) CloseAsync() {
	r.closeAsyncOnce.Do(func() {
		close(r.closeAsyncChan)
	})
}

func (r *mockAsyncReader) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------

type asyncReaderCantConnect struct{}

func (r asyncReaderCantConnect) ConnectWithContext(ctx context.Context) error {
	return types.ErrNotConnected
}
func (r asyncReaderCantConnect) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	return nil, nil, types.ErrNotConnected
}
func (r asyncReaderCantConnect) CloseAsync() {}
func (r asyncReaderCantConnect) WaitForClose(time.Duration) error {
	return nil
}

func TestAsyncReaderCantConnect(t *testing.T) {
	r, err := NewAsyncReader(
		"foo", true, asyncReaderCantConnect{},
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

type asyncReaderCantRead struct {
	connected int
}

func (r *asyncReaderCantRead) ConnectWithContext(ctx context.Context) error {
	r.connected++
	return nil
}
func (r *asyncReaderCantRead) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	return nil, nil, types.ErrNotConnected
}
func (r *asyncReaderCantRead) CloseAsync() {}
func (r *asyncReaderCantRead) WaitForClose(time.Duration) error {
	return nil
}

func TestAsyncReaderCantRead(t *testing.T) {
	readerImpl := &asyncReaderCantRead{}

	r, err := NewAsyncReader(
		"foo", true, readerImpl,
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

func TestAsyncReaderTypeClosedOnConn(t *testing.T) {
	readerImpl := newMockAsyncReader()

	r, err := NewAsyncReader(
		"foo", true, readerImpl,
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

func TestAsyncReaderTypeClosedOnReconn(t *testing.T) {
	readerImpl := newMockAsyncReader()

	r, err := NewAsyncReader(
		"foo", true, readerImpl,
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

func TestAsyncReaderTypeClosedOnReread(t *testing.T) {
	readerImpl := newMockAsyncReader()

	r, err := NewAsyncReader(
		"foo", true, readerImpl,
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

func TestAsyncReaderCanReconnect(t *testing.T) {
	readerImpl := newMockAsyncReader()

	r, err := NewAsyncReader(
		"foo", true, readerImpl,
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

func TestAsyncReaderFailsReconnect(t *testing.T) {
	readerImpl := newMockAsyncReader()

	r, err := NewAsyncReader(
		"foo", true, readerImpl,
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

func TestAsyncReaderCloseDuringReconnect(t *testing.T) {
	readerImpl := newMockAsyncReader()

	r, err := NewAsyncReader(
		"foo", true, readerImpl,
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

func TestAsyncReaderHappyPath(t *testing.T) {
	exp := [][]byte{[]byte("foo"), []byte("bar")}

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []types.Message{message.New(exp)}

	r, err := NewAsyncReader(
		"foo", true, readerImpl,
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

	if readerImpl.ackRcvd[0] != nil {
		t.Error(readerImpl.ackRcvd[0])
	}
}

func TestAsyncReaderCloseWithPendingAcks(t *testing.T) {
	exp := [][]byte{[]byte("hello world")}

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []types.Message{message.New(exp)}

	r, err := NewAsyncReader("foo", true, readerImpl, log.Noop(), metrics.Noop())
	require.NoError(t, err)

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
	}()

	var ts types.Transaction
	var open bool

	select {
	case ts, open = <-r.TransactionChan():
		require.True(t, open)
		assert.Equal(t, message.GetAllBytes(ts.Payload), exp)
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	select {
	case ts.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	// Blocking the reader ack for now
	r.CloseAsync()

	select {
	case <-readerImpl.closeAsyncChan:
		t.Fatal("reader closed early")
	// case <-time.After(time.Millisecond * 100):
	case <-time.After(time.Second):
	}

	select {
	case readerImpl.ackChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	select {
	case <-readerImpl.closeAsyncChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	if readerImpl.ackRcvd[0] != nil {
		t.Error(readerImpl.ackRcvd[0])
	}
}

func TestAsyncReaderSadPath(t *testing.T) {
	exp := [][]byte{[]byte("foo"), []byte("bar")}
	expErr := errors.New("test error")

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []types.Message{message.New(exp)}

	r, err := NewAsyncReader(
		"foo", true, readerImpl,
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

	if actErr := readerImpl.ackRcvd[0]; expErr != actErr {
		t.Errorf("Wrong response received: %v != %v", actErr, expErr)
	}
}

func TestAsyncReaderParallel(t *testing.T) {
	expMsgs := []string{}
	for i := 0; i < 10; i++ {
		expMsgs = append(expMsgs, fmt.Sprintf("message: %v", i))
	}
	readerImpl := newMockAsyncReader()
	for _, str := range expMsgs {
		readerImpl.msgsToSnd = append(readerImpl.msgsToSnd, message.New([][]byte{[]byte(str)}))
	}

	r, err := NewAsyncReader(
		"foo", true, readerImpl,
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

	go func() {
		for range expMsgs {
			select {
			case readerImpl.readChan <- nil:
			case <-time.After(time.Second):
			}
		}
	}()

	expErrs := []error{}
	for i := range expMsgs {
		expErrs = append(expErrs, fmt.Errorf("err %v", i))
	}

	resChans := make([]chan<- types.Response, len(expMsgs))
	for i, mStr := range expMsgs {
		var ts types.Transaction
		var open bool
		select {
		case ts, open = <-r.TransactionChan():
			if !open {
				t.Fatal("Chan closed")
			}
			if act, exp := string(ts.Payload.Get(0).Get()), mStr; exp != act {
				t.Errorf("Wrong message returned: %v != %v", act, exp)
			}
			resChans[i] = ts.ResponseChan
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
	}

	go func() {
		for range expErrs {
			select {
			case readerImpl.ackChan <- nil:
			case <-time.After(time.Second):
			}
		}
	}()

	for i, e := range expErrs {
		select {
		case resChans[i] <- response.NewError(e):
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()
	close(readerImpl.readChan)
	close(readerImpl.connChan)

	if err = r.WaitForClose(time.Second); err != nil {
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		t.Fatal(err)
	}

	if exp, act := expErrs, readerImpl.ackRcvd; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected errors returned: %v != %v", act, exp)
	}
}

//------------------------------------------------------------------------------

func BenchmarkAsyncReaderGenerateN1(b *testing.B) {
	benchmarkAsyncReaderGenerateN(b, 1)
}

func BenchmarkAsyncReaderGenerateN10(b *testing.B) {
	benchmarkAsyncReaderGenerateN(b, 10)
}

func BenchmarkAsyncReaderGenerateN100(b *testing.B) {
	benchmarkAsyncReaderGenerateN(b, 100)
}

func BenchmarkAsyncReaderGenerateN1000(b *testing.B) {
	benchmarkAsyncReaderGenerateN(b, 1000)
}

func benchmarkAsyncReaderGenerateN(b *testing.B, capacity int) {
	bloblConf := NewBloblangConfig()
	bloblConf.Count = 0
	bloblConf.Interval = ""
	bloblConf.Mapping = `root = "hello world"`

	readerImpl, err := newBloblang(types.NoopMgr(), bloblConf)
	require.NoError(b, err)

	r, err := NewAsyncReader("foo", true, readerImpl, log.Noop(), metrics.Noop())
	require.NoError(b, err)

	b.Cleanup(func() {
		r.CloseAsync()
		if err = r.WaitForClose(time.Second); err != nil {
			pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			b.Fatal(err)
		}
	})

	resChans := make([]chan<- types.Response, capacity)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N/capacity; i++ {
		for j := 0; j < capacity; j++ {
			select {
			case ts, open := <-r.TransactionChan():
				require.True(b, open)
				resChans[j] = ts.ResponseChan
			case <-time.After(time.Second):
				b.Fatal("Timed out")
			}
		}

		for j := 0; j < capacity; j++ {
			select {
			case resChans[j] <- response.NewAck():
			case <-time.After(time.Second):
				b.Fatal("Timed out")
			}
		}
	}
}
