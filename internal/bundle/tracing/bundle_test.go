package tracing_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/bundle/tracing"
	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/response"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestBundleInputTracing(t *testing.T) {
	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)

	inConfig := input.NewConfig()
	inConfig.Label = "foo"
	inConfig.Type = input.TypeGenerate
	inConfig.Generate.Count = 10
	inConfig.Generate.Interval = "1us"
	inConfig.Generate.Mapping = `root.count = count("counting the number of input tracing messages")`

	mgr, err := manager.NewV2(
		manager.NewResourceConfig(),
		mock.NewManager(),
		log.Noop(),
		metrics.Noop(),
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	in, err := mgr.NewInput(inConfig)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		select {
		case tran := <-in.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewError(nil):
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	in.CloseAsync()
	require.NoError(t, in.WaitForClose(time.Second))

	assert.Equal(t, uint64(10), summary.Input)
	assert.Equal(t, uint64(0), summary.ProcessorErrors)
	assert.Equal(t, uint64(0), summary.Output)

	inEvents := summary.InputEvents()
	require.Contains(t, inEvents, "foo")

	events := inEvents["foo"]
	require.Len(t, events, 10)

	for i, e := range events {
		assert.Equal(t, tracing.EventProduce, e.Type)
		assert.Equal(t, fmt.Sprintf(`{"count":%v}`, i+1), e.Content)
	}
}

func TestBundleOutputTracing(t *testing.T) {
	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)

	outConfig := output.NewConfig()
	outConfig.Label = "foo"
	outConfig.Type = output.TypeDrop

	mgr, err := manager.NewV2(
		manager.NewResourceConfig(),
		mock.NewManager(),
		log.Noop(),
		metrics.Noop(),
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	out, err := mgr.NewOutput(outConfig)
	require.NoError(t, err)

	tranChan := make(chan message.Transaction)
	require.NoError(t, out.Consume(tranChan))

	for i := 0; i < 10; i++ {
		resChan := make(chan response.Error)
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

	out.CloseAsync()
	require.NoError(t, out.WaitForClose(time.Second))

	assert.Equal(t, uint64(0), summary.Input)
	assert.Equal(t, uint64(0), summary.ProcessorErrors)
	assert.Equal(t, uint64(10), summary.Output)

	outEvents := summary.OutputEvents()
	require.Contains(t, outEvents, "foo")

	events := outEvents["foo"]
	require.Len(t, events, 10)

	for i, e := range events {
		assert.Equal(t, tracing.EventConsume, e.Type)
		assert.Equal(t, strconv.Itoa(i), e.Content)
	}
}

func TestBundleOutputWithProcessorsTracing(t *testing.T) {
	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)

	outConfig := output.NewConfig()
	outConfig.Label = "foo"
	outConfig.Type = output.TypeDrop

	blobConf := processor.NewConfig()
	blobConf.Type = "bloblang"
	blobConf.Bloblang = "root = content().uppercase()"
	outConfig.Processors = append(outConfig.Processors, blobConf)

	mgr, err := manager.NewV2(
		manager.NewResourceConfig(),
		mock.NewManager(),
		log.Noop(),
		metrics.Noop(),
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	out, err := mgr.NewOutput(outConfig)
	require.NoError(t, err)

	tranChan := make(chan message.Transaction)
	require.NoError(t, out.Consume(tranChan))

	for i := 0; i < 10; i++ {
		resChan := make(chan response.Error)
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

	out.CloseAsync()
	require.NoError(t, out.WaitForClose(time.Second))

	assert.Equal(t, uint64(0), summary.Input)
	assert.Equal(t, uint64(0), summary.ProcessorErrors)
	assert.Equal(t, uint64(10), summary.Output)

	outEvents := summary.OutputEvents()
	require.Contains(t, outEvents, "foo")

	outputEvents := outEvents["foo"]
	require.Len(t, outputEvents, 10)

	for i, e := range outputEvents {
		assert.Equal(t, tracing.EventConsume, e.Type)
		assert.Equal(t, "HELLO WORLD "+strconv.Itoa(i), e.Content)
	}

	procEvents := summary.ProcessorEvents()
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

func TestBundleProcessorTracing(t *testing.T) {
	tenv, summary := tracing.TracedBundle(bundle.GlobalEnvironment)

	procConfig := processor.NewConfig()
	procConfig.Label = "foo"
	procConfig.Type = processor.TypeBloblang
	procConfig.Bloblang = `
let ctr = content().number()
root.count = if $ctr % 2 == 0 { throw("nah %v".format($ctr)) } else { $ctr }
`

	mgr, err := manager.NewV2(
		manager.NewResourceConfig(),
		mock.NewManager(),
		log.Noop(),
		metrics.Noop(),
		manager.OptSetEnvironment(tenv),
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(procConfig)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		batch, res := proc.ProcessMessage(message.QuickBatch([][]byte{[]byte(strconv.Itoa(i))}))
		require.Nil(t, res)
		require.Len(t, batch, 1)
		assert.Equal(t, 1, batch[0].Len())
	}

	proc.CloseAsync()
	require.NoError(t, proc.WaitForClose(time.Second))

	assert.Equal(t, uint64(0), summary.Input)
	assert.Equal(t, uint64(5), summary.ProcessorErrors)
	assert.Equal(t, uint64(0), summary.Output)

	procEvents := summary.ProcessorEvents()
	require.Contains(t, procEvents, "foo")

	events := procEvents["foo"]
	require.Len(t, events, 25)

	assert.Equal(t, []tracing.NodeEvent{
		{Type: "CONSUME", Content: "0"},
		{Type: "PRODUCE", Content: "0"},
		{Type: "ERROR", Content: "failed assignment (line 3): nah 0"},
		{Type: "CONSUME", Content: "1"},
		{Type: "PRODUCE", Content: "{\"count\":1}"},
		{Type: "CONSUME", Content: "2"},
		{Type: "PRODUCE", Content: "2"},
		{Type: "ERROR", Content: "failed assignment (line 3): nah 2"},
		{Type: "CONSUME", Content: "3"},
		{Type: "PRODUCE", Content: "{\"count\":3}"},
		{Type: "CONSUME", Content: "4"},
		{Type: "PRODUCE", Content: "4"},
		{Type: "ERROR", Content: "failed assignment (line 3): nah 4"},
		{Type: "CONSUME", Content: "5"},
		{Type: "PRODUCE", Content: "{\"count\":5}"},
		{Type: "CONSUME", Content: "6"},
		{Type: "PRODUCE", Content: "6"},
		{Type: "ERROR", Content: "failed assignment (line 3): nah 6"},
		{Type: "CONSUME", Content: "7"},
		{Type: "PRODUCE", Content: "{\"count\":7}"},
		{Type: "CONSUME", Content: "8"},
		{Type: "PRODUCE", Content: "8"},
		{Type: "ERROR", Content: "failed assignment (line 3): nah 8"},
		{Type: "CONSUME", Content: "9"},
		{Type: "PRODUCE", Content: "{\"count\":9}"},
	}, events)
}
