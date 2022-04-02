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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/input/reader"
)

//------------------------------------------------------------------------------

type mockAsyncReader struct {
	msgsToSnd []*message.Batch
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
		return component.ErrNotConnected
	}
	return cerr
}

func (r *mockAsyncReader) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
	select {
	case <-ctx.Done():
		return nil, nil, component.ErrTimeout
	case err, open := <-r.readChan:
		if !open {
			return nil, nil, component.ErrNotConnected
		}
		if err != nil {
			return nil, nil, err
		}
	}
	r.ackMut.Lock()
	r.ackRcvd = append(r.ackRcvd, errors.New("ack not received"))
	i := len(r.ackRcvd) - 1
	r.ackMut.Unlock()

	nextMsg := message.QuickBatch(nil)
	if len(r.msgsToSnd) > 0 {
		nextMsg = r.msgsToSnd[0]
		r.msgsToSnd = r.msgsToSnd[1:]
	}

	return nextMsg.DeepCopy(), func(ctx context.Context, res error) error {
		r.ackMut.Lock()
		r.ackRcvd[i] = res
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
	return component.ErrNotConnected
}
func (r asyncReaderCantConnect) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
	return nil, nil, component.ErrNotConnected
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
func (r *asyncReaderCantRead) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
	return nil, nil, component.ErrNotConnected
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
		case readerImpl.connChan <- component.ErrTypeClosed:
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
		case readerImpl.readChan <- component.ErrNotConnected:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.connChan <- component.ErrTypeClosed:
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
		case readerImpl.readChan <- component.ErrNotConnected:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.readChan <- component.ErrTypeClosed:
		case <-time.After(time.Second):
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------

func TestAsyncReaderCanReconnect(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

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
		case readerImpl.readChan <- component.ErrNotConnected:
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

	var ts message.Transaction
	var open bool
	select {
	case ts, open = <-r.TransactionChan():
		if !open {
			t.Fatal("Closed early")
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	require.NoError(t, ts.Ack(tCtx, nil))

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()

	go func() {
		select {
		case readerImpl.readChan <- nil:
		case readerImpl.connChan <- component.ErrNotConnected:
		case <-time.After(time.Second):
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestAsyncReaderFailsReconnect(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

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
		case readerImpl.readChan <- component.ErrNotConnected:
		case <-time.After(time.Second):
		}
		select {
		case readerImpl.connChan <- component.ErrNotConnected:
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

	var ts message.Transaction
	var open bool
	select {
	case ts, open = <-r.TransactionChan():
		if !open {
			t.Fatal("Closed early")
		}
	case <-time.After(time.Second * 2):
		t.Error("Timed out")
	}
	require.NoError(t, ts.Ack(tCtx, nil))

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
	case readerImpl.readChan <- component.ErrNotConnected:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	go func() {
		select {
		case readerImpl.connChan <- component.ErrNotConnected:
		case <-time.After(time.Second):
		}
		close(readerImpl.connChan)
	}()

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()
	close(readerImpl.readChan)

	if err = r.WaitForClose(time.Second); err != nil {
		_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		t.Error(err)
	}
}

func TestAsyncReaderHappyPath(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []*message.Batch{message.QuickBatch(exp)}

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

	var ts message.Transaction
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
	require.NoError(t, ts.Ack(tCtx, nil))

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()
	close(readerImpl.readChan)
	close(readerImpl.connChan)

	if err = r.WaitForClose(time.Second); err != nil {
		_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		t.Fatal(err)
	}

	if readerImpl.ackRcvd[0] != nil {
		t.Error(readerImpl.ackRcvd[0])
	}
}

func TestAsyncReaderCloseWithPendingAcks(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	exp := [][]byte{[]byte("hello world")}

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []*message.Batch{message.QuickBatch(exp)}

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

	var ts message.Transaction
	var open bool

	select {
	case ts, open = <-r.TransactionChan():
		require.True(t, open)
		assert.Equal(t, message.GetAllBytes(ts.Payload), exp)
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}
	require.NoError(t, ts.Ack(tCtx, nil))

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
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	expErr := errors.New("test error")

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []*message.Batch{message.QuickBatch(exp)}

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

	var ts message.Transaction
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
	require.NoError(t, ts.Ack(tCtx, expErr))

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()
	close(readerImpl.readChan)
	close(readerImpl.connChan)

	if err = r.WaitForClose(time.Second); err != nil {
		_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		t.Fatal(err)
	}

	if actErr := readerImpl.ackRcvd[0]; expErr != actErr {
		t.Errorf("Wrong response received: %v != %v", actErr, expErr)
	}
}

func TestAsyncReaderParallel(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	expMsgs := []string{}
	for i := 0; i < 10; i++ {
		expMsgs = append(expMsgs, fmt.Sprintf("message: %v", i))
	}
	readerImpl := newMockAsyncReader()
	for _, str := range expMsgs {
		readerImpl.msgsToSnd = append(readerImpl.msgsToSnd, message.QuickBatch([][]byte{[]byte(str)}))
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

	resFns := make([]func(context.Context, error) error, len(expMsgs))
	for i, mStr := range expMsgs {
		var ts message.Transaction
		var open bool
		select {
		case ts, open = <-r.TransactionChan():
			if !open {
				t.Fatal("Chan closed")
			}
			if act, exp := string(ts.Payload.Get(0).Get()), mStr; exp != act {
				t.Errorf("Wrong message returned: %v != %v", act, exp)
			}
			resFns[i] = ts.Ack
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
		require.NoError(t, resFns[i](tCtx, e))
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()
	close(readerImpl.readChan)
	close(readerImpl.connChan)

	if err = r.WaitForClose(time.Second); err != nil {
		_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
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
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	bloblConf := NewBloblangConfig()
	bloblConf.Count = 0
	bloblConf.Interval = ""
	bloblConf.Mapping = `root = "hello world"`

	readerImpl, err := newBloblang(mock.NewManager(), bloblConf)
	require.NoError(b, err)

	r, err := NewAsyncReader("foo", true, readerImpl, log.Noop(), metrics.Noop())
	require.NoError(b, err)

	b.Cleanup(func() {
		r.CloseAsync()
		if err = r.WaitForClose(time.Second); err != nil {
			_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			b.Fatal(err)
		}
	})

	resFns := make([]func(context.Context, error) error, capacity)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N/capacity; i++ {
		for j := 0; j < capacity; j++ {
			select {
			case ts, open := <-r.TransactionChan():
				require.True(b, open)
				resFns[j] = ts.Ack
			case <-time.After(time.Second):
				b.Fatal("Timed out")
			}
		}

		for j := 0; j < capacity; j++ {
			require.NoError(b, resFns[j](tCtx, nil))
		}
	}
}
