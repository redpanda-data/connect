package sentry

import (
	"context"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestCaptureProcessor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  environment: testing
  release: benthos-sentry
  level: WARN
  message: "hello ${! this.name }"
  context: |
    root = {"profile": {"country": this.country}}
  tags:
    pipeline: test-pipeline
    app: "test ${! this.appversion }"
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	var rawEvent any
	transport := &mockTransport{}
	transport.On("SendEvent", argEvent).Return().Run(func(args mock.Arguments) {
		rawEvent = args.Get(0)
	})
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us", "appversion": "0.1.0"}`))
	b, err := proc.Process(ctx, msg)
	require.NoError(t, err, "failed to processor message")
	require.Len(t, b, 1, "wrong batch size received")
	require.Same(t, msg, b[0])

	require.NotNil(t, rawEvent, "expected to get an event from SendEvent mock")

	event, ok := rawEvent.(*sentry.Event)
	require.True(t, ok, "wrong argument type to SendEvent")
	require.Equal(t, sentry.LevelWarning, event.Level, "event has wrong level")
	require.Equal(t, "hello jane", event.Message)
	require.Equal(t, "testing", event.Environment, "event has wrong environment")
	require.Equal(t, "benthos-sentry", event.Release, "event has wrong release")
	require.Equal(t, map[string]any{"country": "us"}, event.Contexts["profile"])
	require.Equal(t, map[string]string{"app": "test 0.1.0", "pipeline": "test-pipeline", "benthos": "mock"}, event.Tags)
}

func TestCaptureProcessor_Sync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  transport_mode: sync
  environment: testing
  release: benthos-sentry
  level: DEBUG
  message: "hello ${! this.name }"
  context: |
    root = {"profile": {"country": this.country}}
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	var rawEvent any
	transport := &mockTransport{}
	transport.On("SendEvent", argEvent).Return().Run(func(args mock.Arguments) {
		rawEvent = args.Get(0)
	})
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us"}`))
	b, err := proc.Process(ctx, msg)
	require.NoError(t, err, "failed to processor message")
	require.Len(t, b, 1, "wrong batch size received")
	require.Same(t, msg, b[0])

	require.NotNil(t, rawEvent, "expected to get an event from SendEvent mock")

	event, ok := rawEvent.(*sentry.Event)
	require.True(t, ok, "wrong argument type to SendEvent")
	require.Equal(t, "hello jane", event.Message)
	require.Equal(t, map[string]any{"country": "us"}, event.Contexts["profile"])
	require.Equal(t, "testing", event.Environment, "event has wrong environment")
	require.Equal(t, "benthos-sentry", event.Release, "event has wrong release")
	require.Equal(t, sentry.LevelDebug, event.Level, "event has wrong level")
}

func TestCaptureProcessor_InvalidMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  message: 'hello ${! throw("simulated error") }'
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	transport := &mockTransport{}
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us"}`))
	b, err := proc.Process(ctx, msg)
	require.ErrorContains(t, err, "simulated error", "message mapping error not caught")
	require.Nil(t, b, "should not have received a message batch")

	transport.AssertNotCalled(t, "SendEvent", mock.Anything)
}

// TestCaptureProcessor_NoSampling checks that sentry capture is disabled if
// sampling rate is 0.
func TestCaptureProcessor_NoSampling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  sampling_rate: 0
  environment: testing
  release: benthos-sentry
  level: INFO
  message: "hello ${! this.name }"
  context: |
    root = {"profile": {"country": this.country}}
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	transport := &mockTransport{}
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us"}`))
	b, err := proc.Process(ctx, msg)
	require.NoError(t, err, "failed to process message")
	require.Len(t, b, 1, "wrong batch size received")
	require.Same(t, msg, b[0])

	transport.AssertNotCalled(t, "SendEvent", mock.Anything)
}

func TestCaptureProcessor_FlushOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	d, err := time.ParseDuration("3s")
	require.NoError(t, err, "bad flush timeout duration")

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  flush_timeout: 3s
  environment: testing
  release: benthos-sentry
  level: INFO
  message: "hello ${! this.name }"
  context: |
    root = {"profile": {"country": this.country}}
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	transport := &mockTransport{}
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", d).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })
}

func TestCaptureProcessor_FlushFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  environment: testing
  release: benthos-sentry
  level: INFO
  message: "hello ${! this.name }"
  context: |
    root = {"profile": {"country": this.country}}
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	transport := &mockTransport{}
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(false)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")

	err = proc.Close(ctx)
	require.ErrorContains(t, err, "failed to flush sentry events before timeout")
}

// TestCaptureProcessor_EmptyContext checks that deleting context in mapping
// results in empty context on sentry event.
func TestCaptureProcessor_EmptyContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  message: "hello ${! this.name }"
  context: root = deleted()
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	var rawEvent any
	transport := &mockTransport{}
	transport.On("SendEvent", argEvent).Return().Run(func(args mock.Arguments) {
		rawEvent = args.Get(0)
	})
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us"}`))
	b, err := proc.Process(ctx, msg)
	require.NoError(t, err, "failed to process message")
	require.Len(t, b, 1, "wrong batch size received")
	require.Same(t, msg, b[0])

	require.NotNil(t, rawEvent, "expected to get an event from SendEvent mock")

	event, ok := rawEvent.(*sentry.Event)
	require.True(t, ok, "wrong argument type to SendEvent")

	var contextKeys []string
	for k := range event.Contexts {
		contextKeys = append(contextKeys, k)
	}
	require.Len(t, contextKeys, 3, "wrong number of context keys found")
	require.ElementsMatch(t, []string{"device", "os", "runtime"}, contextKeys)
}

// TestCaptureProcessor_NoContext checks that leaving context config unset
// results in empty context on sentry event.
func TestCaptureProcessor_NoContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  message: "hello ${! this.name }"
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	var rawEvent any
	transport := &mockTransport{}
	transport.On("SendEvent", argEvent).Return().Run(func(args mock.Arguments) {
		rawEvent = args.Get(0)
	})
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us"}`))
	b, err := proc.Process(ctx, msg)
	require.NoError(t, err, "failed to process message")
	require.Len(t, b, 1, "wrong batch size received")
	require.Same(t, msg, b[0])

	require.NotNil(t, rawEvent, "expected to get an event from SendEvent mock")

	event, ok := rawEvent.(*sentry.Event)
	require.True(t, ok, "wrong argument type to SendEvent")

	var contextKeys []string
	for k := range event.Contexts {
		contextKeys = append(contextKeys, k)
	}
	require.Len(t, contextKeys, 3, "wrong number of context keys found")
	require.ElementsMatch(t, []string{"device", "os", "runtime"}, contextKeys)
}

func TestCaptureProcessor_NilContextValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  message: "hello ${! this.name }"
  context: |
    root = {"profile": null}
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	var rawEvent any
	transport := &mockTransport{}
	transport.On("SendEvent", argEvent).Return().Run(func(args mock.Arguments) {
		rawEvent = args.Get(0)
	})
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us"}`))
	b, err := proc.Process(ctx, msg)
	require.NoError(t, err, "failed to process message")
	require.Len(t, b, 1, "wrong batch size received")
	require.Same(t, msg, b[0])

	require.NotNil(t, rawEvent, "expected to get an event from SendEvent mock")

	event, ok := rawEvent.(*sentry.Event)
	require.True(t, ok, "wrong argument type to SendEvent")

	var contextKeys []string
	for k := range event.Contexts {
		contextKeys = append(contextKeys, k)
	}
	require.Len(t, contextKeys, 3, "wrong number of context keys found")
	require.ElementsMatch(t, []string{"device", "os", "runtime"}, contextKeys)
}

func TestCaptureProcessor_InvalidContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  message: "hello ${! this.name }"
  context: |
    root = {"country": {"code": throw("simulated error")}}
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	transport := &mockTransport{}
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us"}`))
	b, err := proc.Process(ctx, msg)
	require.ErrorContains(t, err, "simulated error", "message mapping error not caught")
	require.Nil(t, b, "should not have received a message batch")

	transport.AssertNotCalled(t, "SendEvent", mock.Anything)
}

func TestCaptureProcessor_ContextNotStructured(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  message: "hello ${! this.name }"
  context: |
    root = "i should be a structured value"
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	transport := &mockTransport{}
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us"}`))
	b, err := proc.Process(ctx, msg)
	require.ErrorContains(t, err, "failed to get structured data for context", "message mapping error not caught")
	require.Nil(t, b, "should not have received a message batch")

	transport.AssertNotCalled(t, "SendEvent", mock.Anything)
}

func TestCaptureProcessor_ContextNotMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  message: "hello ${! this.name }"
  context: |
    root = [{"foo":"bar"}]
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	transport := &mockTransport{}
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us"}`))
	b, err := proc.Process(ctx, msg)
	require.ErrorContains(t, err, "expected object from context mapping but got: []interface {}", "message mapping error not caught")
	require.Nil(t, b, "should not have received a message batch")

	transport.AssertNotCalled(t, "SendEvent", mock.Anything)
}

func TestCaptureProcessor_ContextValueNotMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  message: "hello ${! this.name }"
  context: |
    root = {"country": this.country}
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	transport := &mockTransport{}
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us"}`))
	b, err := proc.Process(ctx, msg)
	require.ErrorContains(t, err, "expected an object for context key: country: got string")
	require.Nil(t, b, "should not have received a message batch")

	transport.AssertNotCalled(t, "SendEvent", mock.Anything)
}

func TestCaptureProcessor_InvalidTag(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec := newCaptureProcessorConfig()
	conf, err := spec.ParseYAML(`
  message: "hello ${! this.name }"
  tags:
    foo: '${! throw("simulated error") }'
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	transport := &mockTransport{}
	transport.On("Configure", mock.Anything).Return()
	transport.On("Flush", mock.Anything).Return(true)
	t.Cleanup(func() { transport.AssertExpectations(t) })

	proc, err := newCaptureProcessor(conf, service.MockResources(), withTransport(transport))
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	msg := service.NewMessage([]byte(`{"name": "jane", "country": "us"}`))
	b, err := proc.Process(ctx, msg)
	require.ErrorContains(t, err, "failed to evaluate sentry tag: foo: simulated error", "message mapping error not caught")
	require.Nil(t, b, "should not have received a message batch")

	transport.AssertNotCalled(t, "SendEvent", mock.Anything)
}
