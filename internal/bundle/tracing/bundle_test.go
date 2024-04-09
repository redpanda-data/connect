package tracing_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/bundle/tracing"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/message"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestBundleInputTracing(t *testing.T) {
	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)

	inConfig, err := testutil.InputFromYAML(`
label: foo
generate:
  count: 10
  interval: 1us
  mapping: 'root.count = counter()'
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	in, err := mgr.NewInput(inConfig)
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	for i := 0; i < 10; i++ {
		select {
		case tran := <-in.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	in.TriggerStopConsuming()
	require.NoError(t, in.WaitForClose(ctx))

	assert.Equal(t, uint64(10), summary.Input)
	assert.Equal(t, uint64(0), summary.ProcessorErrors)
	assert.Equal(t, uint64(0), summary.Output)

	inEvents := summary.InputEvents(false)
	require.Contains(t, inEvents, "foo")

	events := inEvents["foo"]
	require.Len(t, events, 10)

	for i, e := range events {
		assert.Equal(t, tracing.EventProduce, e.Type)
		assert.Equal(t, fmt.Sprintf(`{"count":%v}`, i+1), e.Content)
	}
}

func TestBundleInputTracingFlush(t *testing.T) {
	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)

	inConfig, err := testutil.InputFromYAML(`
label: foo
generate:
  count: 10
  interval: 1us
  mapping: 'root.count = counter()'
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	in, err := mgr.NewInput(inConfig)
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	for i := 0; i < 10; i++ {
		select {
		case tran := <-in.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	in.TriggerStopConsuming()
	require.NoError(t, in.WaitForClose(ctx))

	assert.Equal(t, uint64(10), summary.Input)
	assert.Equal(t, uint64(0), summary.ProcessorErrors)
	assert.Equal(t, uint64(0), summary.Output)

	inEvents := summary.InputEvents(false)
	require.Contains(t, inEvents, "foo")

	events := inEvents["foo"]
	require.Len(t, events, 10)

	for i, e := range events {
		assert.Equal(t, tracing.EventProduce, e.Type)
		assert.Equal(t, fmt.Sprintf(`{"count":%v}`, i+1), e.Content)
	}

	// Not flushed
	inEvents = summary.InputEvents(true)
	require.Contains(t, inEvents, "foo")
	require.Len(t, inEvents["foo"], 10)

	// Now it's flushed
	inEvents = summary.InputEvents(true)
	require.Contains(t, inEvents, "foo")
	require.Empty(t, inEvents["foo"])

	// Run more stuff
	inConfig, err = testutil.InputFromYAML(`
label: foo
generate:
  count: 5
  interval: 1us
  mapping: 'root.count = counter()'
`)
	require.NoError(t, err)

	in, err = mgr.NewInput(inConfig)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		select {
		case tran := <-in.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	in.TriggerStopConsuming()
	require.NoError(t, in.WaitForClose(ctx))

	inEvents = summary.InputEvents(false)
	require.Contains(t, inEvents, "foo")

	events = inEvents["foo"]
	require.Len(t, events, 5)

	for i, e := range events {
		assert.Equal(t, tracing.EventProduce, e.Type)
		assert.Equal(t, fmt.Sprintf(`{"count":%v}`, i+1), e.Content)
	}
}

func TestBundleInputTracingDisabled(t *testing.T) {
	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)
	summary.SetEnabled(false)

	inConfig, err := testutil.InputFromYAML(`
label: foo
generate:
  count: 10
  interval: 1us
  mapping: 'root.count = counter()'
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	in, err := mgr.NewInput(inConfig)
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	for i := 0; i < 10; i++ {
		select {
		case tran := <-in.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	in.TriggerStopConsuming()
	require.NoError(t, in.WaitForClose(ctx))

	assert.Equal(t, uint64(0), summary.Input)
	assert.Equal(t, uint64(0), summary.ProcessorErrors)
	assert.Equal(t, uint64(0), summary.Output)

	inEvents := summary.InputEvents(false)
	require.Contains(t, inEvents, "foo")

	events := inEvents["foo"]
	require.Empty(t, events)
}

func TestBundleOutputTracing(t *testing.T) {
	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)

	outConfig := output.NewConfig()
	outConfig.Label = "foo"
	outConfig.Type = "drop"

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	out, err := mgr.NewOutput(outConfig)
	require.NoError(t, err)

	tranChan := make(chan message.Transaction)
	require.NoError(t, out.Consume(tranChan))

	for i := 0; i < 10; i++ {
		resChan := make(chan error)
		tran := message.NewTransaction(message.QuickBatch([][]byte{[]byte(strconv.Itoa(i))}), resChan)
		select {
		case tranChan <- tran:
			select {
			case <-resChan:
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	out.TriggerCloseNow()
	require.NoError(t, out.WaitForClose(ctx))

	assert.Equal(t, uint64(0), summary.Input)
	assert.Equal(t, uint64(0), summary.ProcessorErrors)
	assert.Equal(t, uint64(10), summary.Output)

	outEvents := summary.OutputEvents(false)
	require.Contains(t, outEvents, "foo")

	events := outEvents["foo"]
	require.Len(t, events, 10)

	for i, e := range events {
		assert.Equal(t, tracing.EventConsume, e.Type)
		assert.Equal(t, strconv.Itoa(i), e.Content)
	}
}

func TestBundleOutputTracingDisabled(t *testing.T) {
	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)
	summary.SetEnabled(false)

	outConfig := output.NewConfig()
	outConfig.Label = "foo"
	outConfig.Type = "drop"

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	out, err := mgr.NewOutput(outConfig)
	require.NoError(t, err)

	tranChan := make(chan message.Transaction)
	require.NoError(t, out.Consume(tranChan))

	for i := 0; i < 10; i++ {
		resChan := make(chan error)
		tran := message.NewTransaction(message.QuickBatch([][]byte{[]byte(strconv.Itoa(i))}), resChan)
		select {
		case tranChan <- tran:
			select {
			case <-resChan:
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	out.TriggerCloseNow()
	require.NoError(t, out.WaitForClose(ctx))

	assert.Equal(t, uint64(0), summary.Input)
	assert.Equal(t, uint64(0), summary.ProcessorErrors)
	assert.Equal(t, uint64(0), summary.Output)

	outEvents := summary.OutputEvents(false)
	require.Contains(t, outEvents, "foo")

	events := outEvents["foo"]
	require.Empty(t, events)
}

func TestBundleOutputWithProcessorsTracing(t *testing.T) {
	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)

	outConfig := output.NewConfig()
	outConfig.Label = "foo"
	outConfig.Type = "drop"

	blobConf := processor.NewConfig()
	blobConf.Type = "bloblang"
	blobConf.Plugin = "root = content().uppercase()"
	outConfig.Processors = append(outConfig.Processors, blobConf)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	out, err := mgr.NewOutput(outConfig)
	require.NoError(t, err)

	tranChan := make(chan message.Transaction)
	require.NoError(t, out.Consume(tranChan))

	for i := 0; i < 10; i++ {
		resChan := make(chan error)
		tran := message.NewTransaction(message.QuickBatch([][]byte{[]byte("hello world " + strconv.Itoa(i))}), resChan)
		select {
		case tranChan <- tran:
			select {
			case <-resChan:
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	close(tranChan)

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, out.WaitForClose(ctx))

	assert.Equal(t, uint64(0), summary.Input)
	assert.Equal(t, uint64(0), summary.ProcessorErrors)
	assert.Equal(t, uint64(10), summary.Output)

	outEvents := summary.OutputEvents(false)
	require.Contains(t, outEvents, "foo")

	outputEvents := outEvents["foo"]
	require.Len(t, outputEvents, 10)

	for i, e := range outputEvents {
		assert.Equal(t, tracing.EventConsume, e.Type)
		assert.Equal(t, "HELLO WORLD "+strconv.Itoa(i), e.Content)
	}

	procEvents := summary.ProcessorEvents(false)
	require.Contains(t, procEvents, "root.processors.0")

	processorEvents := procEvents["root.processors.0"]
	require.Len(t, processorEvents, 20)

	for i := 0; i < len(processorEvents); i += 2 {
		consumeEvent := processorEvents[i]
		produceEvent := processorEvents[i+1]

		assert.Equal(t, tracing.EventConsume, consumeEvent.Type)
		assert.Equal(t, tracing.EventProduce, produceEvent.Type)

		assert.Equal(t, "hello world "+strconv.Itoa(i/2), consumeEvent.Content)
		assert.Equal(t, "HELLO WORLD "+strconv.Itoa(i/2), produceEvent.Content)
	}
}

func TestBundleOutputWithBatchProcessorsTracing(t *testing.T) {
	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)

	outConfig, err := testutil.OutputFromYAML(`
broker:
  outputs:
    - label: foo
      drop: {}
  batching:
    count: 2
    processors:
      - mapping: 'root = content().uppercase()'
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	out, err := mgr.NewOutput(outConfig)
	require.NoError(t, err)

	tranChan := make(chan message.Transaction)
	require.NoError(t, out.Consume(tranChan))

	for i := 0; i < 5; i++ {
		resChan := make(chan error)
		tran := message.NewTransaction(message.QuickBatch([][]byte{
			[]byte("hello world " + strconv.Itoa(i*2)),
			[]byte("hello world " + strconv.Itoa(i*2+1)),
		}), resChan)
		select {
		case tranChan <- tran:
			select {
			case <-resChan:
			case <-time.After(time.Second):
				t.Fatal("timed out", i)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	out.TriggerCloseNow()
	require.NoError(t, out.WaitForClose(ctx))

	assert.Equal(t, 0, int(summary.Input))
	assert.Equal(t, 0, int(summary.ProcessorErrors))
	assert.Equal(t, 20, int(summary.Output))

	outEvents := summary.OutputEvents(false)
	require.Contains(t, outEvents, "foo")

	outputEvents := outEvents["foo"]
	require.Len(t, outputEvents, 10)

	for i, e := range outputEvents {
		assert.Equal(t, tracing.EventConsume, e.Type)
		assert.Equal(t, "HELLO WORLD "+strconv.Itoa(i), e.Content)
	}

	procEvents := summary.ProcessorEvents(false)
	require.Contains(t, procEvents, "root.batching.processors.0")

	processorEvents := procEvents["root.batching.processors.0"]
	require.Len(t, processorEvents, 20)

	for i := 0; i < len(processorEvents); i += 4 {
		consumeEventA := processorEvents[i]
		consumeEventB := processorEvents[i+1]
		produceEventA := processorEvents[i+2]
		produceEventB := processorEvents[i+3]

		assert.Equal(t, tracing.EventConsume, consumeEventA.Type)
		assert.Equal(t, tracing.EventConsume, consumeEventB.Type)
		assert.Equal(t, tracing.EventProduce, produceEventA.Type)
		assert.Equal(t, tracing.EventProduce, produceEventB.Type)

		assert.Equal(t, "hello world "+strconv.Itoa(i/2), consumeEventA.Content, i)
		assert.Equal(t, "hello world "+strconv.Itoa(i/2+1), consumeEventB.Content, i)
		assert.Equal(t, "HELLO WORLD "+strconv.Itoa(i/2), produceEventA.Content, i)
		assert.Equal(t, "HELLO WORLD "+strconv.Itoa(i/2+1), produceEventB.Content, i)
	}
}

func TestBundleProcessorTracing(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)

	procConfig := processor.NewConfig()
	procConfig.Label = "foo"
	procConfig.Type = "bloblang"
	procConfig.Plugin = `
let ctr = content().number()
root.count = if $ctr % 2 == 0 { throw("nah %v".format($ctr)) } else { $ctr }
meta bar = "new bar value"
`

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(procConfig)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		part := message.NewPart([]byte(strconv.Itoa(i)))
		part.MetaSetMut("foo", fmt.Sprintf("foo value %v", i))
		batch, res := proc.ProcessBatch(tCtx, message.Batch{part})
		require.NoError(t, res)
		require.Len(t, batch, 1)
		assert.Equal(t, 1, batch[0].Len())
	}

	require.NoError(t, proc.Close(tCtx))

	assert.Equal(t, uint64(0), summary.Input)
	assert.Equal(t, uint64(5), summary.ProcessorErrors)
	assert.Equal(t, uint64(0), summary.Output)

	procEvents := summary.ProcessorEvents(false)
	require.Contains(t, procEvents, "foo")

	events := procEvents["foo"]
	require.Len(t, events, 25)

	type tMap = map[string]any

	assert.Equal(t, []tracing.NodeEvent{
		{Type: "CONSUME", Content: "0", Meta: tMap{"foo": "foo value 0"}},
		{Type: "PRODUCE", Content: "0", Meta: tMap{"foo": "foo value 0"}},
		{Type: "ERROR", Content: "failed assignment (line 3): nah 0"},
		{Type: "CONSUME", Content: "1", Meta: tMap{"foo": "foo value 1"}},
		{Type: "PRODUCE", Content: "{\"count\":1}", Meta: tMap{"bar": "new bar value", "foo": "foo value 1"}},
		{Type: "CONSUME", Content: "2", Meta: tMap{"foo": "foo value 2"}},
		{Type: "PRODUCE", Content: "2", Meta: tMap{"foo": "foo value 2"}},
		{Type: "ERROR", Content: "failed assignment (line 3): nah 2"},
		{Type: "CONSUME", Content: "3", Meta: tMap{"foo": "foo value 3"}},
		{Type: "PRODUCE", Content: "{\"count\":3}", Meta: tMap{"bar": "new bar value", "foo": "foo value 3"}},
		{Type: "CONSUME", Content: "4", Meta: tMap{"foo": "foo value 4"}},
		{Type: "PRODUCE", Content: "4", Meta: tMap{"foo": "foo value 4"}},
		{Type: "ERROR", Content: "failed assignment (line 3): nah 4"},
		{Type: "CONSUME", Content: "5", Meta: tMap{"foo": "foo value 5"}},
		{Type: "PRODUCE", Content: "{\"count\":5}", Meta: tMap{"bar": "new bar value", "foo": "foo value 5"}},
		{Type: "CONSUME", Content: "6", Meta: tMap{"foo": "foo value 6"}},
		{Type: "PRODUCE", Content: "6", Meta: tMap{"foo": "foo value 6"}},
		{Type: "ERROR", Content: "failed assignment (line 3): nah 6"},
		{Type: "CONSUME", Content: "7", Meta: tMap{"foo": "foo value 7"}},
		{Type: "PRODUCE", Content: "{\"count\":7}", Meta: tMap{"bar": "new bar value", "foo": "foo value 7"}},
		{Type: "CONSUME", Content: "8", Meta: tMap{"foo": "foo value 8"}},
		{Type: "PRODUCE", Content: "8", Meta: tMap{"foo": "foo value 8"}},
		{Type: "ERROR", Content: "failed assignment (line 3): nah 8"},
		{Type: "CONSUME", Content: "9", Meta: tMap{"foo": "foo value 9"}},
		{Type: "PRODUCE", Content: "{\"count\":9}", Meta: tMap{"bar": "new bar value", "foo": "foo value 9"}},
	}, events)
}

func TestBundleProcessorTracingError(t *testing.T) {
	tenv, _ := tracing.TracedBundle(bundle.GlobalEnvironment)

	procConfig := processor.NewConfig()
	procConfig.Label = "foo"
	procConfig.Type = "bloblang"
	procConfig.Plugin = `let nope`

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	_, err = mgr.NewProcessor(procConfig)
	require.EqualError(t, err, "failed to init processor 'foo': line 1 char 9: expected whitespace")
}

func TestBundleProcessorTracingDisabled(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)
	summary.SetEnabled(false)

	procConfig := processor.NewConfig()
	procConfig.Label = "foo"
	procConfig.Type = "bloblang"
	procConfig.Plugin = `
let ctr = content().number()
root.count = if $ctr % 2 == 0 { throw("nah %v".format($ctr)) } else { $ctr }
meta bar = "new bar value"
`

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(procConfig)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		part := message.NewPart([]byte(strconv.Itoa(i)))
		part.MetaSetMut("foo", fmt.Sprintf("foo value %v", i))
		batch, res := proc.ProcessBatch(tCtx, message.Batch{part})
		require.NoError(t, res)
		require.Len(t, batch, 1)
		assert.Equal(t, 1, batch[0].Len())
	}

	require.NoError(t, proc.Close(tCtx))

	assert.Equal(t, uint64(0), summary.Input)
	assert.Equal(t, uint64(0), summary.ProcessorErrors)
	assert.Equal(t, uint64(0), summary.Output)

	procEvents := summary.ProcessorEvents(false)
	require.Contains(t, procEvents, "foo")

	events := procEvents["foo"]
	require.Empty(t, events)
}
