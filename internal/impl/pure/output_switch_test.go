package pure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func newSwitch(t testing.TB, mockOutputs []*mock.OutputChanneled, confStr string, args ...any) *switchOutput {
	t.Helper()

	mgr := mock.NewManager()

	pConf, err := switchOutputSpec().ParseYAML(fmt.Sprintf(confStr, args...), nil)
	require.NoError(t, err)

	s, err := switchOutputFromParsed(pConf, mgr)
	require.NoError(t, err)

	for i := 0; i < len(mockOutputs); i++ {
		close(s.outputTSChans[i])
		s.outputs[i] = mockOutputs[i]
		s.outputTSChans[i] = make(chan message.Transaction)
		_ = mockOutputs[i].Consume(s.outputTSChans[i])
	}
	return s
}

func TestSwitchNoConditions(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nOutputs, nMsgs := 10, 1000

	confStr := `
cases:`

	mockOutputs := []*mock.OutputChanneled{}
	for i := 0; i < nOutputs; i++ {
		confStr += `
  - output:
      drop: {}
    continue: true
`
		mockOutputs = append(mockOutputs, &mock.OutputChanneled{})
	}

	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for broker send")
		}
		resFnSlice := []func(context.Context, error) error{}
		for j := 0; j < nOutputs; j++ {
			select {
			case ts := <-mockOutputs[j].TChan:
				if !bytes.Equal(ts.Payload.Get(0).AsBytes(), content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).AsBytes(), content[0])
				}
				resFnSlice = append(resFnSlice, ts.Ack)
			case <-time.After(time.Second):
				t.Fatal("Timed out waiting for broker propagate")
			}
		}
		for j := 0; j < nOutputs; j++ {
			require.NoError(t, resFnSlice[j](ctx, nil))
		}
		select {
		case res := <-resChan:
			require.NoError(t, res)
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to broker")
		}
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))
}

func TestSwitchNoRetries(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nOutputs, nMsgs := 10, 1000

	confStr := `
retry_until_success: false
cases:`

	mockOutputs := []*mock.OutputChanneled{}
	for i := 0; i < nOutputs; i++ {
		confStr += `
  - output:
      drop: {}
    continue: true
`
		mockOutputs = append(mockOutputs, &mock.OutputChanneled{})
	}

	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for broker send")
		}
		resFnSlice := []func(context.Context, error) error{}
		for j := 0; j < nOutputs; j++ {
			select {
			case ts := <-mockOutputs[j].TChan:
				if !bytes.Equal(ts.Payload.Get(0).AsBytes(), content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).AsBytes(), content[0])
				}
				resFnSlice = append(resFnSlice, ts.Ack)
			case <-time.After(time.Second):
				t.Fatal("Timed out waiting for broker propagate")
			}
		}
		for j := 0; j < nOutputs; j++ {
			var res error
			if j == 1 {
				res = errors.New("test")
			} else {
				res = nil
			}
			require.NoError(t, resFnSlice[j](ctx, res))
		}
		select {
		case res := <-resChan:
			assert.EqualError(t, res, "test")
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to broker")
		}
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))
}

func TestSwitchBatchNoRetries(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	confStr := `
retry_until_success: false
cases:
  - check: 'root = this.id %% 2 == 0'
    output:
      drop: {}
  - check: 'root = true'
    output:
      reject: "meow"
`

	s := newSwitch(t, nil, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error)
	require.NoError(t, s.Consume(readChan))

	msg := message.QuickBatch([][]byte{
		[]byte(`{"content":"hello world","id":0}`),
		[]byte(`{"content":"hello world","id":1}`),
		[]byte(`{"content":"hello world","id":2}`),
		[]byte(`{"content":"hello world","id":3}`),
		[]byte(`{"content":"hello world","id":4}`),
	})
	sortGroup, msg := message.NewSortGroup(msg)

	select {
	case readChan <- message.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for broker send")
	}

	var res error
	select {
	case res = <-resChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out responding to broker")
	}

	err := res
	require.Error(t, err)

	var bOut *batch.Error
	require.ErrorAsf(t, err, &bOut, "should be batch error, got: %v", err)

	assert.Equal(t, 2, bOut.IndexedErrors())

	errContents := []string{}
	bOut.WalkPartsBySource(sortGroup, msg, func(i int, p *message.Part, e error) bool {
		if e != nil {
			errContents = append(errContents, string(p.AsBytes()))
			assert.EqualError(t, e, "meow")
		}
		return true
	})
	assert.Equal(t, []string{
		`{"content":"hello world","id":1}`,
		`{"content":"hello world","id":3}`,
	}, errContents)

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))
}

func TestSwitchBatchNoRetriesBatchErr(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	confStr := `
retry_until_success: false
cases:
  - output:
      drop: {}
    continue: true
  - output:
      drop: {}
    continue: true
`
	mockOutputs := []*mock.OutputChanneled{
		{}, {},
	}

	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)
	require.NoError(t, s.Consume(readChan))

	msg := message.QuickBatch([][]byte{
		[]byte("hello world 0"),
		[]byte("hello world 1"),
		[]byte("hello world 2"),
		[]byte("hello world 3"),
		[]byte("hello world 4"),
	})
	sortGroup, msg := message.NewSortGroup(msg)

	select {
	case readChan <- message.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for broker send")
	}

	transactions := []message.Transaction{}
	for j := 0; j < 2; j++ {
		select {
		case ts := <-mockOutputs[j].TChan:
			transactions = append(transactions, ts)
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for broker propagate")
		}
	}
	for j := 0; j < 2; j++ {
		var res error
		if j == 0 {
			batchErr := batch.NewError(transactions[j].Payload, errors.New("not this"))
			batchErr.Failed(1, errors.New("err 1"))
			batchErr.Failed(3, errors.New("err 3"))
			res = batchErr
		} else {
			res = nil
		}
		require.NoError(t, transactions[j].Ack(ctx, res))
	}

	select {
	case res := <-resChan:
		err := res
		require.Error(t, err)

		var bOut *batch.Error
		require.ErrorAsf(t, err, &bOut, "should be batch error but got %T", err)

		assert.Equal(t, 2, bOut.IndexedErrors())

		errContents := []string{}
		bOut.WalkPartsBySource(sortGroup, msg, func(i int, p *message.Part, e error) bool {
			if e != nil {
				errContents = append(errContents, string(p.AsBytes()))
				assert.EqualError(t, e, fmt.Sprintf("err %v", i))
			}
			return true
		})
		assert.Equal(t, []string{
			"hello world 1",
			"hello world 3",
		}, errContents)
	case <-time.After(time.Second):
		t.Fatal("Timed out responding to broker")
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))
}

func TestSwitchWithConditions(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nMsgs := 100

	mockOutputs := []*mock.OutputChanneled{{}, {}, {}}
	confStr := `
cases:
  - output:
      drop: {}
    check: 'this.foo == "bar"'
  - output:
      drop: {}
    check: 'this.foo == "baz"'
  - output:
      drop: {}
`
	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		closed := 0
		bar := `{"foo":"bar"}`
		baz := `{"foo":"baz"}`

	outputLoop:
		for closed < len(mockOutputs) {
			var ts message.Transaction
			var ok bool

			select {
			case ts, ok = <-mockOutputs[0].TChan:
				if !ok {
					closed++
					continue outputLoop
				}
				if act := string(ts.Payload.Get(0).AsBytes()); act != bar {
					t.Errorf("Expected output 0 msgs to equal %s, got %s", bar, act)
				}
			case ts, ok = <-mockOutputs[1].TChan:
				if !ok {
					closed++
					continue outputLoop
				}
				if act := string(ts.Payload.Get(0).AsBytes()); act != baz {
					t.Errorf("Expected output 1 msgs to equal %s, got %s", baz, act)
				}
			case ts, ok = <-mockOutputs[2].TChan:
				if !ok {
					closed++
					continue outputLoop
				}
				if act := string(ts.Payload.Get(0).AsBytes()); act == bar || act == baz {
					t.Errorf("Expected output 2 msgs to not equal %s or %s, got %s", bar, baz, act)
				}
			case <-time.After(time.Second):
				t.Error("Timed out waiting for output to propagate")
				break outputLoop
			}

			if !assert.NoError(t, ts.Ack(ctx, nil)) {
				break outputLoop
			}
		}
	}()

	for i := 0; i < nMsgs; i++ {
		foo := "bar"
		if i%3 == 0 {
			foo = "qux"
		} else if i%2 == 0 {
			foo = "baz"
		}
		content := [][]byte{[]byte(fmt.Sprintf("{\"foo\":%q}", foo))}
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for output send")
			return
		}

		select {
		case res := <-resChan:
			require.NoError(t, res)
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to output")
		}
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))
	wg.Wait()
}

func TestSwitchError(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockOutputs := []*mock.OutputChanneled{{}, {}, {}}
	confStr := `
cases:
  - output:
      drop: {}
    check: 'this.foo == "bar"'
  - output:
      drop: {}
    check: 'this.foo.not_null() == "baz"'
  - output:
      drop: {}
    check: 'this.foo == "buz"'
`
	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	msg := message.QuickBatch([][]byte{
		[]byte(`{"foo":"bar"}`),
		[]byte(`{"not_foo":"baz"}`),
		[]byte(`{"foo":"baz"}`),
		[]byte(`{"foo":"buz"}`),
		[]byte(`{"foo":"nope"}`),
	})

	select {
	case readChan <- message.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out waiting to send")
	}

	var ts message.Transaction

	for i := 0; i < len(mockOutputs); i++ {
		select {
		case ts = <-mockOutputs[0].TChan:
			assert.Equal(t, 1, ts.Payload.Len())
			assert.Equal(t, `{"foo":"bar"}`, string(ts.Payload.Get(0).AsBytes()))
		case ts = <-mockOutputs[1].TChan:
			assert.Equal(t, 1, ts.Payload.Len())
			assert.Equal(t, `{"foo":"baz"}`, string(ts.Payload.Get(0).AsBytes()))
		case ts = <-mockOutputs[2].TChan:
			assert.Equal(t, 1, ts.Payload.Len())
			assert.Equal(t, `{"foo":"buz"}`, string(ts.Payload.Get(0).AsBytes()))
		case <-time.After(time.Second):
			t.Error("Timed out waiting for output to propagate")
		}
		require.NoError(t, ts.Ack(ctx, nil))
	}

	select {
	case res := <-resChan:
		if res != nil {
			t.Errorf("Received unexpected errors from output: %v", res)
		}
	case <-time.After(time.Second):
		t.Error("Timed out responding to output")
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))
}

func TestSwitchBatchSplit(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockOutputs := []*mock.OutputChanneled{{}, {}, {}}
	confStr := `
cases:
  - output:
      drop: {}
    check: 'this.foo == "bar"'
  - output:
      drop: {}
    check: 'this.foo == "baz"'
  - output:
      drop: {}
    check: 'this.foo == "buz"'
`
	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	msg := message.QuickBatch([][]byte{
		[]byte(`{"foo":"bar"}`),
		[]byte(`{"foo":"baz"}`),
		[]byte(`{"foo":"buz"}`),
		[]byte(`{"foo":"nope"}`),
	})

	select {
	case readChan <- message.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out waiting to send")
	}

	var ts message.Transaction

	for i := 0; i < len(mockOutputs); i++ {
		select {
		case ts = <-mockOutputs[0].TChan:
			assert.Equal(t, 1, ts.Payload.Len())
			assert.Equal(t, `{"foo":"bar"}`, string(ts.Payload.Get(0).AsBytes()))
		case ts = <-mockOutputs[1].TChan:
			assert.Equal(t, 1, ts.Payload.Len())
			assert.Equal(t, `{"foo":"baz"}`, string(ts.Payload.Get(0).AsBytes()))
		case ts = <-mockOutputs[2].TChan:
			assert.Equal(t, 1, ts.Payload.Len())
			assert.Equal(t, `{"foo":"buz"}`, string(ts.Payload.Get(0).AsBytes()))
		case <-time.After(time.Second):
			t.Error("Timed out waiting for output to propagate")
		}
		require.NoError(t, ts.Ack(ctx, nil))
	}

	select {
	case res := <-resChan:
		require.NoError(t, res)
	case <-time.After(time.Second):
		t.Error("Timed out responding to output")
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))
}

func TestSwitchBatchGroup(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockOutputs := []*mock.OutputChanneled{{}, {}, {}}
	confStr := `
cases:
  - output:
      drop: {}
    check: 'json().foo.from(0) == "bar"'
  - output:
      drop: {}
    check: 'json().foo.from(0) == "baz"'
  - output:
      drop: {}
    check: 'json().foo.from(0) == "buz"'
`
	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	msg := message.QuickBatch([][]byte{
		[]byte(`{"foo":"baz"}`),
		[]byte(`{"foo":"bar"}`),
		[]byte(`{"foo":"buz"}`),
		[]byte(`{"foo":"nope"}`),
	})

	select {
	case readChan <- message.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out waiting to send")
	}

	var ts message.Transaction

	select {
	case ts = <-mockOutputs[0].TChan:
		t.Error("did not expect message route to 0")
	case ts = <-mockOutputs[1].TChan:
		assert.Equal(t, 4, ts.Payload.Len())
		assert.Equal(t, `{"foo":"baz"}`, string(ts.Payload.Get(0).AsBytes()))
		assert.Equal(t, `{"foo":"bar"}`, string(ts.Payload.Get(1).AsBytes()))
		assert.Equal(t, `{"foo":"buz"}`, string(ts.Payload.Get(2).AsBytes()))
		assert.Equal(t, `{"foo":"nope"}`, string(ts.Payload.Get(3).AsBytes()))
	case ts = <-mockOutputs[2].TChan:
		t.Error("did not expect message route to 2")
	case <-time.After(time.Second):
		t.Error("Timed out waiting for output to propagate")
	}
	require.NoError(t, ts.Ack(ctx, nil))

	select {
	case <-mockOutputs[0].TChan:
		t.Error("did not expect message route to 0")
	case <-mockOutputs[2].TChan:
		t.Error("did not expect message route to 2")
	case res := <-resChan:
		if res != nil {
			t.Errorf("Received unexpected errors from output: %v", res)
		}
	case <-time.After(time.Second):
		t.Error("Timed out responding to output")
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))
}

func TestSwitchNoMatch(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockOutputs := []*mock.OutputChanneled{{}, {}, {}}
	confStr := `
cases:
  - output:
      drop: {}
    check: 'this.foo == "bar"'
  - output:
      drop: {}
    check: 'this.foo == "baz"'
  - output:
      drop: {}
    check: 'false'
`
	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	msg := message.QuickBatch([][]byte{[]byte(`{"foo":"qux"}`)})
	select {
	case readChan <- message.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for output send")
	}

	select {
	case res := <-resChan:
		require.NoError(t, res)
	case <-time.After(time.Second):
		t.Fatal("Timed out responding to output")
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))
}

func TestSwitchNoMatchStrict(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockOutputs := []*mock.OutputChanneled{{}, {}, {}}
	confStr := `
strict_mode: true
cases:
  - output:
      drop: {}
    check: 'this.foo == "bar"'
  - output:
      drop: {}
    check: 'this.foo == "baz"'
  - output:
      drop: {}
    check: 'false'
`
	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	msg := message.QuickBatch([][]byte{[]byte(`{"foo":"qux"}`)})
	select {
	case readChan <- message.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for output send")
	}

	select {
	case res := <-resChan:
		require.Error(t, res)
	case <-time.After(time.Second):
		t.Fatal("Timed out responding to output")
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))
}

func TestSwitchWithConditionsNoFallthrough(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nMsgs := 100

	mockOutputs := []*mock.OutputChanneled{{}, {}, {}}
	confStr := `
strict_mode: true
cases:
  - output:
      drop: {}
    check: 'this.foo == "bar"'
  - output:
      drop: {}
    check: 'this.foo == "baz"'
  - output:
      drop: {}
`
	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		closed := 0
		bar := `{"foo":"bar"}`
		baz := `{"foo":"baz"}`

	outputLoop:
		for closed < len(mockOutputs) {
			resFns := []func(context.Context, error) error{}
			for len(resFns) < 1 {
				select {
				case ts, ok := <-mockOutputs[0].TChan:
					if !ok {
						closed++
						continue outputLoop
					}
					if act := string(ts.Payload.Get(0).AsBytes()); act != bar {
						t.Errorf("Expected output 0 msgs to equal %s, got %s", bar, act)
					}
					resFns = append(resFns, ts.Ack)
				case ts, ok := <-mockOutputs[1].TChan:
					if !ok {
						closed++
						continue outputLoop
					}
					if act := string(ts.Payload.Get(0).AsBytes()); act != baz {
						t.Errorf("Expected output 1 msgs to equal %s, got %s", baz, act)
					}
					resFns = append(resFns, ts.Ack)
				case _, ok := <-mockOutputs[2].TChan:
					if !ok {
						closed++
						continue outputLoop
					}
					t.Error("Unexpected msg received by output 3")
				case <-time.After(time.Second):
					t.Error("Timed out waiting for output to propagate")
					break outputLoop
				}
			}

			for i := 0; i < len(resFns); i++ {
				require.NoError(t, resFns[i](ctx, nil))
			}
		}
	}()

	for i := 0; i < nMsgs; i++ {
		foo := "bar"
		if i%2 == 0 {
			foo = "baz"
		}
		content := [][]byte{[]byte(fmt.Sprintf("{\"foo\":%q}", foo))}
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for output send")
		}

		select {
		case res := <-resChan:
			require.NoError(t, res)
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to output")
		}
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))

	wg.Wait()
}

func TestSwitchShutDownFromErrorResponse(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockOutputs := []*mock.OutputChanneled{{}, {}}
	confStr := `
cases:
  - output:
      drop: {}
    continue: true
  - output:
      drop: {}
    continue: true
`
	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	select {
	case readChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foo")}), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg send")
	}

	var ts message.Transaction
	var open bool
	select {
	case ts, open = <-mockOutputs[0].TChan:
		require.True(t, open)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg rcv")
	}
	select {
	case _, open = <-mockOutputs[1].TChan:
		require.True(t, open)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg rcv")
	}
	require.NoError(t, ts.Ack(ctx, errors.New("test")))

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))

	select {
	case _, open := <-mockOutputs[0].TChan:
		assert.False(t, open)
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestSwitchShutDownFromReceive(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockOutputs := []*mock.OutputChanneled{{}, {}}
	confStr := `
cases:
  - output:
      drop: {}
    continue: true
  - output:
      drop: {}
    continue: true
`
	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	select {
	case readChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foo")}), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg send")
	}

	select {
	case _, open := <-mockOutputs[0].TChan:
		require.True(t, open)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg rcv")
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))

	select {
	case _, open := <-mockOutputs[0].TChan:
		assert.False(t, open)
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestSwitchShutDownFromSend(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockOutputs := []*mock.OutputChanneled{{}, {}}
	confStr := `
cases:
  - output:
      drop: {}
    continue: true
  - output:
      drop: {}
    continue: true
`
	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	select {
	case readChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foo")}), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg send")
	}

	s.TriggerCloseNow()
	require.NoError(t, s.WaitForClose(ctx))

	select {
	case _, open := <-mockOutputs[0].TChan:
		assert.False(t, open)
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestSwitchBackPressure(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	t.Parallel()

	mockOutputs := []*mock.OutputChanneled{{}, {}}
	confStr := `
cases:
  - output:
      drop: {}
    continue: true
  - output:
      drop: {}
    continue: true
`
	s := newSwitch(t, mockOutputs, confStr)

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	require.NoError(t, s.Consume(readChan))

	wg := sync.WaitGroup{}
	wg.Add(1)
	doneChan := make(chan struct{})
	go func() {
		defer wg.Done()
		// Consume as fast as possible from mock one
		for {
			select {
			case ts := <-mockOutputs[0].TChan:
				require.NoError(t, ts.Ack(ctx, nil))
			case <-doneChan:
				return
			}
		}
	}()

	i := 0
bpLoop:
	for ; i < 1000; i++ {
		select {
		case readChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("hello world")}), resChan):
		case <-time.After(time.Millisecond * 200):
			break bpLoop
		}
	}
	if i > 500 {
		t.Error("We shouldn't be capable of dumping this many messages into a blocked broker")
	}

	close(readChan)
	close(doneChan)
	wg.Wait()
}
