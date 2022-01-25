package reader

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockAsyncReader struct {
	msgsToSnd []types.Message
	ackRcvd   []error

	connChan         chan error
	readChan         chan error
	ackChan          chan error
	closeAsyncChan   chan struct{}
	waitForCloseChan chan error
}

func newMockAsyncReader() *mockAsyncReader {
	return &mockAsyncReader{
		connChan:         make(chan error),
		readChan:         make(chan error),
		ackChan:          make(chan error),
		closeAsyncChan:   make(chan struct{}),
		waitForCloseChan: make(chan error),
	}
}

func (r *mockAsyncReader) ConnectWithContext(ctx context.Context) error {
	cerr, open := <-r.connChan
	if !open {
		return types.ErrNotConnected
	}
	return cerr
}

func (r *mockAsyncReader) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
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
	r.ackRcvd = append(r.ackRcvd, errors.New("ack not received"))
	i := len(r.ackRcvd) - 1

	var nextMsg types.Message = message.New(nil)
	if len(r.msgsToSnd) > 0 {
		nextMsg = r.msgsToSnd[0]
		r.msgsToSnd = r.msgsToSnd[1:]
	}

	return nextMsg.DeepCopy(), func(ctx context.Context, res types.Response) error {
		r.ackRcvd[i] = res.Error()
		return <-r.ackChan
	}, nil
}

func (r *mockAsyncReader) CloseAsync() {
	<-r.closeAsyncChan
}

func (r *mockAsyncReader) WaitForClose(time.Duration) error {
	return <-r.waitForCloseChan
}

//------------------------------------------------------------------------------

func TestAsyncPreserverClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncPreserver(readerImpl)

	exp := errors.New("foo error")

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		if err := pres.ConnectWithContext(ctx); err != nil {
			t.Error(err)
		}
		pres.CloseAsync()
		if act := pres.WaitForClose(time.Second); act != exp {
			t.Errorf("Wrong error returned: %v != %v", act, exp)
		}
		wg.Done()
	}()

	select {
	case readerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	select {
	case readerImpl.closeAsyncChan <- struct{}{}:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	select {
	case readerImpl.waitForCloseChan <- exp:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	wg.Wait()
}

func TestAsyncPreserverNackThenClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []types.Message{
		message.New([][]byte{[]byte("hello world")}),
	}
	pres := NewAsyncPreserver(readerImpl)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		select {
		case readerImpl.connChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.readChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.readChan <- types.ErrTypeClosed:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.ackChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.readChan <- types.ErrTypeClosed:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.closeAsyncChan <- struct{}{}:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.waitForCloseChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}
	}()

	err := pres.ConnectWithContext(ctx)
	assert.NoError(t, err)

	_, ackFn1, err := pres.ReadWithContext(ctx)
	assert.NoError(t, err)

	go func() {
		time.Sleep(time.Millisecond * 10)
		assert.NoError(t, ackFn1(ctx, response.NewError(errors.New("rejected"))))
	}()

	_, _, err = pres.ReadWithContext(ctx)
	assert.Equal(t, types.ErrTimeout, err)

	_, ackFn2, err := pres.ReadWithContext(ctx)
	assert.NoError(t, err)
	assert.NoError(t, ackFn2(ctx, response.NewAck()))

	_, _, err = pres.ReadWithContext(ctx)
	assert.Equal(t, types.ErrTypeClosed, err)

	pres.CloseAsync()
	err = pres.WaitForClose(time.Second)
	assert.NoError(t, err)

	wg.Wait()
}

func TestAsyncPreserverCloseThenAck(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []types.Message{
		message.New([][]byte{[]byte("hello world")}),
	}
	pres := NewAsyncPreserver(readerImpl)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		select {
		case readerImpl.connChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.readChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.readChan <- types.ErrTypeClosed:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.ackChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.closeAsyncChan <- struct{}{}:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.waitForCloseChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}
	}()

	err := pres.ConnectWithContext(ctx)
	assert.NoError(t, err)

	_, ackFn1, err := pres.ReadWithContext(ctx)
	assert.NoError(t, err)

	go func() {
		time.Sleep(time.Millisecond * 10)
		assert.NoError(t, ackFn1(ctx, response.NewAck()))
	}()

	_, _, err = pres.ReadWithContext(ctx)
	assert.Equal(t, types.ErrTypeClosed, err)

	pres.CloseAsync()
	err = pres.WaitForClose(time.Second)
	assert.NoError(t, err)

	wg.Wait()
}

func TestAsyncPreserverCloseThenNackThenAck(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []types.Message{
		message.New([][]byte{[]byte("hello world")}),
	}
	pres := NewAsyncPreserver(readerImpl)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		select {
		case readerImpl.connChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.readChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.readChan <- types.ErrTypeClosed:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.readChan <- types.ErrTypeClosed:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.ackChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.closeAsyncChan <- struct{}{}:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.waitForCloseChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}
	}()

	err := pres.ConnectWithContext(ctx)
	assert.NoError(t, err)

	_, ackFn1, err := pres.ReadWithContext(ctx)
	assert.NoError(t, err)

	go func() {
		time.Sleep(time.Millisecond * 100)
		assert.NoError(t, ackFn1(ctx, response.NewError(errors.New("huh"))))
	}()

	_, _, err = pres.ReadWithContext(ctx)
	assert.Equal(t, types.ErrTimeout, err)

	_, ackFn2, err := pres.ReadWithContext(ctx)
	require.NoError(t, err)

	go func() {
		time.Sleep(time.Millisecond * 100)
		assert.NoError(t, ackFn2(ctx, response.NewAck()))
	}()

	_, _, err = pres.ReadWithContext(ctx)
	assert.Equal(t, types.ErrTypeClosed, err)

	pres.CloseAsync()
	err = pres.WaitForClose(time.Second)
	assert.NoError(t, err)

	wg.Wait()
}

func TestAsyncPreserverCloseViaConnectThenAck(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	readerImpl.msgsToSnd = []types.Message{
		message.New([][]byte{[]byte("hello world")}),
	}
	pres := NewAsyncPreserver(readerImpl)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		select {
		case readerImpl.connChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.readChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.readChan <- types.ErrNotConnected:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.connChan <- types.ErrTypeClosed:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.ackChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.closeAsyncChan <- struct{}{}:
		case <-ctx.Done():
			t.Error("Timed out")
		}

		select {
		case readerImpl.waitForCloseChan <- nil:
		case <-ctx.Done():
			t.Error("Timed out")
		}
	}()

	err := pres.ConnectWithContext(ctx)
	assert.NoError(t, err)

	_, ackFn1, err := pres.ReadWithContext(ctx)
	assert.NoError(t, err)

	_, _, err = pres.ReadWithContext(ctx)
	assert.Equal(t, types.ErrNotConnected, err)

	err = pres.ConnectWithContext(ctx)
	assert.NoError(t, err)

	go func() {
		time.Sleep(time.Millisecond * 100)
		assert.NoError(t, ackFn1(ctx, response.NewAck()))
	}()

	_, _, err = pres.ReadWithContext(ctx)
	assert.Equal(t, types.ErrTypeClosed, err)

	pres.CloseAsync()
	err = pres.WaitForClose(time.Second)
	assert.NoError(t, err)

	wg.Wait()
}

func TestAsyncPreserverHappy(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncPreserver(readerImpl)

	expParts := [][]byte{
		[]byte("foo"),
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		for _, p := range expParts {
			readerImpl.msgsToSnd = []types.Message{message.New([][]byte{p})}
			select {
			case readerImpl.readChan <- nil:
			case <-time.After(time.Second):
				t.Error("Timed out")
			}
		}
	}()

	if err := pres.ConnectWithContext(ctx); err != nil {
		t.Error(err)
	}

	for _, exp := range expParts {
		msg, _, err := pres.ReadWithContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if act := msg.Get(0).Get(); !reflect.DeepEqual(act, exp) {
			t.Errorf("Wrong message returned: %v != %v", act, exp)
		}
	}
}

func TestAsyncPreserverErrorProp(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncPreserver(readerImpl)

	expErr := errors.New("foo")

	go func() {
		select {
		case readerImpl.connChan <- expErr:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.readChan <- expErr:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.ackChan <- expErr:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	if actErr := pres.ConnectWithContext(ctx); expErr != actErr {
		t.Errorf("Wrong error returned: %v != %v", actErr, expErr)
	}
	if _, _, actErr := pres.ReadWithContext(ctx); expErr != actErr {
		t.Errorf("Wrong error returned: %v != %v", actErr, expErr)
	}
	if _, aFn, actErr := pres.ReadWithContext(ctx); actErr != nil {
		t.Fatal(actErr)
	} else if actErr = aFn(ctx, response.NewAck()); expErr != actErr {
		t.Errorf("Wrong error returned: %v != %v", actErr, expErr)
	}
}

func TestAsyncPreserverErrorBackoff(t *testing.T) {
	t.Parallel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncPreserver(readerImpl)

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.closeAsyncChan <- struct{}{}:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.waitForCloseChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	require.NoError(t, pres.ConnectWithContext(ctx))

	i := 0
	for {
		_, aFn, actErr := pres.ReadWithContext(ctx)
		if actErr != nil {
			assert.EqualError(t, actErr, "context deadline exceeded")
			break
		}
		require.NoError(t, aFn(ctx, response.NewError(errors.New("no thanks"))))
		i++
		if i == 10 {
			t.Error("Expected backoff to prevent this")
			break
		}
	}

	pres.CloseAsync()
	require.NoError(t, pres.WaitForClose(time.Second))
}

func TestAsyncPreserverBatchError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncPreserver(readerImpl)

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		readerImpl.msgsToSnd = []types.Message{
			message.New([][]byte{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
				[]byte("buz"),
				[]byte("bev"),
			})}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.ackChan <- errors.New("ack propagated"):
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	require.NoError(t, pres.ConnectWithContext(ctx))

	msg, ackFn, err := pres.ReadWithContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("buz"),
		[]byte("bev"),
	}, message.GetAllBytes(msg))

	bErr := batch.NewError(msg, errors.New("first"))
	bErr.Failed(1, errors.New("second"))
	bErr.Failed(3, errors.New("third"))

	require.NoError(t, ackFn(ctx, response.NewError(bErr)))

	msg, ackFn, err = pres.ReadWithContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{
		[]byte("bar"),
		[]byte("buz"),
	}, message.GetAllBytes(msg))

	require.EqualError(t, ackFn(ctx, response.NewAck()), "ack propagated")
}

func TestAsyncPreserverBatchErrorUnordered(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncPreserver(readerImpl)

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		readerImpl.msgsToSnd = []types.Message{
			message.New([][]byte{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
				[]byte("buz"),
				[]byte("bev"),
			})}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.ackChan <- errors.New("ack propagated"):
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	require.NoError(t, pres.ConnectWithContext(ctx))

	msg, ackFn, err := pres.ReadWithContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("buz"),
		[]byte("bev"),
	}, message.GetAllBytes(msg))

	bMsg := message.New(nil)
	bMsg.Append(msg.Get(1))
	bMsg.Append(msg.Get(3))
	bMsg.Append(msg.Get(0))
	bMsg.Append(msg.Get(4))
	bMsg.Append(msg.Get(2))

	bErr := batch.NewError(bMsg, errors.New("first"))
	bErr.Failed(1, errors.New("second"))
	bErr.Failed(2, errors.New("third"))

	require.NoError(t, ackFn(ctx, response.NewError(bErr)))

	msg, ackFn, err = pres.ReadWithContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{
		[]byte("buz"),
		[]byte("foo"),
	}, message.GetAllBytes(msg))

	require.EqualError(t, ackFn(ctx, response.NewAck()), "ack propagated")
}

//------------------------------------------------------------------------------

func TestAsyncPreserverBuffer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncPreserver(readerImpl)

	sendMsg := func(content string) {
		readerImpl.msgsToSnd = []types.Message{message.New(
			[][]byte{[]byte(content)},
		)}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}
	sendAck := func() {
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	// Send message normally.
	exp := "msg 1"
	exp2 := "msg 2"
	exp3 := "msg 3"

	go sendMsg(exp)
	msg, aFn, err := pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp)
	}

	// Prime second message.
	go sendMsg(exp2)

	// Fail previous message, expecting it to be resent.
	_ = aFn(ctx, response.NewError(errors.New("failed")))
	msg, aFn, err = pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp)
	}

	// Read the primed message.
	var aFn2 AsyncAckFn
	msg, aFn2, err = pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp2 != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp2)
	}

	// Fail both messages, expecting them to be resent.
	_ = aFn(ctx, response.NewError(errors.New("failed again")))
	_ = aFn2(ctx, response.NewError(errors.New("failed again")))

	// Read both messages.
	msg, aFn, err = pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp)
	}
	msg, aFn2, err = pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp2 != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp2)
	}

	// Prime a new message and also an acknowledgement.
	go sendMsg(exp3)
	go sendAck()
	go sendAck()

	// Ack all messages.
	_ = aFn(ctx, response.NewAck())
	_ = aFn2(ctx, response.NewAck())

	msg, _, err = pres.ReadWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp3 != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp3)
	}
}

func TestAsyncPreserverBufferBatchedAcks(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	readerImpl := newMockAsyncReader()
	pres := NewAsyncPreserver(readerImpl)

	sendMsg := func(content string) {
		readerImpl.msgsToSnd = []types.Message{message.New(
			[][]byte{[]byte(content)},
		)}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}
	sendAck := func() {
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	messages := []string{
		"msg 1",
		"msg 2",
		"msg 3",
	}

	ackFns := []AsyncAckFn{}
	for _, exp := range messages {
		go sendMsg(exp)
		msg, aFn, err := pres.ReadWithContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ackFns = append(ackFns, aFn)
		if act := string(msg.Get(0).Get()); exp != act {
			t.Errorf("Wrong message returned: %v != %v", act, exp)
		}
	}

	// Fail all messages, expecting them to be resent.
	for _, aFn := range ackFns {
		_ = aFn(ctx, response.NewError(errors.New("failed again")))
	}
	ackFns = []AsyncAckFn{}

	for _, exp := range messages {
		msg, aFn, err := pres.ReadWithContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ackFns = append(ackFns, aFn)
		if act := string(msg.Get(0).Get()); exp != act {
			t.Errorf("Wrong message returned: %v != %v", act, exp)
		}
	}

	// Ack all messages.
	go func() {
		for _, aFn := range ackFns {
			_ = aFn(ctx, response.NewAck())
		}
	}()

	for range ackFns {
		sendAck()
	}
}
