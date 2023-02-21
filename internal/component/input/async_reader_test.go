package input_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type mockAsyncReader struct {
	msgsToSnd []message.Batch
	ackRcvd   []error
	ackMut    sync.Mutex

	connChan       chan error
	readChan       chan error
	ackChan        chan error
	closeAsyncChan chan struct{}
	closeAsyncOnce sync.Once

	unblockCloseAsyncChan chan struct{}
	waitForCloseChan      chan error
}

func newMockAsyncReader() *mockAsyncReader {
	unblockCloseAsyncChan := make(chan struct{})
	close(unblockCloseAsyncChan)

	waitForCloseChan := make(chan error, 1)
	waitForCloseChan <- nil

	return &mockAsyncReader{
		connChan:              make(chan error),
		readChan:              make(chan error),
		ackChan:               make(chan error),
		closeAsyncChan:        make(chan struct{}),
		unblockCloseAsyncChan: unblockCloseAsyncChan,
		waitForCloseChan:      waitForCloseChan,
	}
}

func (r *mockAsyncReader) Connect(ctx context.Context) error {
	cerr, open := <-r.connChan
	if !open {
		return component.ErrNotConnected
	}
	return cerr
}

func (r *mockAsyncReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
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

	nextMsg := message.Batch{message.NewPart(nil)}
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

func (r *mockAsyncReader) Close(ctx context.Context) error {
	<-r.unblockCloseAsyncChan
	r.closeAsyncOnce.Do(func() {
		close(r.closeAsyncChan)
	})
	return <-r.waitForCloseChan
}

//------------------------------------------------------------------------------

type asyncReaderCantConnect struct{}

func (r asyncReaderCantConnect) Connect(ctx context.Context) error {
	return component.ErrNotConnected
}

func (r asyncReaderCantConnect) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	return nil, nil, component.ErrNotConnected
}
func (r asyncReaderCantConnect) Close(ctx context.Context) error { return nil }

func TestAsyncReaderCantConnect(t *testing.T) {
	r, err := input.NewAsyncReader("foo", asyncReaderCantConnect{}, mock.NewManager())
	if err != nil {
		t.Error(err)
		return
	}

	// We will fail to connect but should still exit immediately.
	r.TriggerStopConsuming()
	require.NoError(t, r.WaitForClose(context.Background()))
}

//------------------------------------------------------------------------------

type asyncReaderCantRead struct {
	connected int
}

func (r *asyncReaderCantRead) Connect(ctx context.Context) error {
	r.connected++
	return nil
}

func (r *asyncReaderCantRead) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	return nil, nil, component.ErrNotConnected
}
func (r *asyncReaderCantRead) Close(ctx context.Context) error { return nil }

func TestAsyncReaderCantRead(t *testing.T) {
	readerImpl := &asyncReaderCantRead{}

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
	if err != nil {
		t.Error(err)
		return
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	r.TriggerStopConsuming()
	require.NoError(t, r.WaitForClose(ctx))

	if readerImpl.connected < 1 {
		t.Errorf("Connected wasn't called enough times: %v", readerImpl.connected)
	}
}

//------------------------------------------------------------------------------

func TestAsyncReaderTypeClosedOnConn(t *testing.T) {
	readerImpl := newMockAsyncReader()

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
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

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, r.WaitForClose(ctx))
}

func TestAsyncReaderTypeClosedOnReconn(t *testing.T) {
	readerImpl := newMockAsyncReader()

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
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

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, r.WaitForClose(ctx))
}

func TestAsyncReaderTypeClosedOnReread(t *testing.T) {
	readerImpl := newMockAsyncReader()

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
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

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, r.WaitForClose(ctx))
}

//------------------------------------------------------------------------------

func TestAsyncReaderCanReconnect(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	readerImpl := newMockAsyncReader()

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
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
	r.TriggerStopConsuming()

	go func() {
		select {
		case readerImpl.readChan <- nil:
		case readerImpl.connChan <- component.ErrNotConnected:
		case <-time.After(time.Second):
		}
	}()

	require.NoError(t, r.WaitForClose(tCtx))
}

func TestAsyncReaderFailsReconnect(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	readerImpl := newMockAsyncReader()

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
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
	r.TriggerStopConsuming()

	go func() {
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
		}
	}()

	require.NoError(t, r.WaitForClose(tCtx))
}

func TestAsyncReaderCloseDuringReconnect(t *testing.T) {
	readerImpl := newMockAsyncReader()

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
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
	r.TriggerStopConsuming()
	close(readerImpl.readChan)

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, r.WaitForClose(ctx))
}

func TestAsyncReaderHappyPath(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []message.Batch{message.QuickBatch(exp)}

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
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
	r.TriggerStopConsuming()
	close(readerImpl.readChan)
	close(readerImpl.connChan)

	require.NoError(t, r.WaitForClose(tCtx))

	if readerImpl.ackRcvd[0] != nil {
		t.Error(readerImpl.ackRcvd[0])
	}
}

func TestAsyncReaderCloseWithPendingAcks(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	exp := [][]byte{[]byte("hello world")}

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []message.Batch{message.QuickBatch(exp)}

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
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
	r.TriggerStopConsuming()

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
	readerImpl.msgsToSnd = []message.Batch{message.QuickBatch(exp)}

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
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
	r.TriggerStopConsuming()
	close(readerImpl.readChan)
	close(readerImpl.connChan)

	require.NoError(t, r.WaitForClose(tCtx))

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

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
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
			if act, exp := string(ts.Payload.Get(0).AsBytes()), mStr; exp != act {
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
	r.TriggerStopConsuming()
	close(readerImpl.readChan)
	close(readerImpl.connChan)

	require.NoError(t, r.WaitForClose(tCtx))

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

type mockStaticReader struct {
	d []byte
}

func (r *mockStaticReader) Connect(ctx context.Context) error {
	return nil
}

func (r *mockStaticReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	nextMsg := message.QuickBatch([][]byte{r.d})
	return nextMsg, func(ctx context.Context, res error) error {
		return nil
	}, nil
}

func (r *mockStaticReader) Close(ctx context.Context) error {
	return nil
}

func benchmarkAsyncReaderGenerateN(b *testing.B, capacity int) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	readerImpl := &mockStaticReader{
		d: []byte(`root = "hello world"`),
	}

	r, err := input.NewAsyncReader("foo", readerImpl, mock.NewManager())
	require.NoError(b, err)

	b.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		defer done()

		r.TriggerStopConsuming()
		require.NoError(b, r.WaitForClose(ctx))
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
