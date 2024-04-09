package sentry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/getsentry/sentry-go"

	"github.com/benthosdev/benthos/v4/internal/cli"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	transportAsync = "async"
	transportSync  = "sync"
)

func newCaptureProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.16.0").
		Summary("Captures log events from messages and submits them to [Sentry](https://sentry.io/).").
		Fields(
			service.NewStringField("dsn").
				Default("").
				Description("The DSN address to send sentry events to. If left empty, then SENTRY_DSN is used."),

			service.NewInterpolatedStringField("message").
				Description("A message to set on the sentry event").
				Example("webhook event received").
				Example("failed to find product in database: ${! error() }"),

			service.NewBloblangField("context").
				Optional().
				Description("A mapping that must evaluate to an object-of-objects or `deleted()`. If this mapping produces a value, then it is set on a sentry event as additional context.").
				Example(`root = {"order": {"product_id": "P93174", "quantity": 5}}`).
				Example(`root = deleted()`),

			service.NewInterpolatedStringMapField("tags").
				Optional().
				Description("Sets key/value string tags on an event. Unlike context, these are indexed and searchable on Sentry but have length limitations."),

			service.NewStringField("environment").
				Default("").
				Description("The environment to be sent with events. If left empty, then SENTRY_ENVIRONMENT is used."),

			service.NewStringField("release").
				Default("").
				Description("The version of the code deployed to an environment. If left empty, then the Sentry client will attempt to detect the release from the environment."),

			service.NewStringEnumField("level", "DEBUG", "INFO", "WARN", "ERROR", "FATAL").
				Default("INFO").
				Description("Sets the level on sentry events similar to logging levels."),

			service.NewStringEnumField("transport_mode", transportAsync, transportSync).
				Default(transportAsync).
				Description("Determines how events are sent. A sync transport will block when sending each event until a response is received from the Sentry server. The recommended async transport will enqueue events in a buffer and send them in the background."),

			service.NewDurationField("flush_timeout").
				Default("5s").
				Description("The duration to wait when closing the processor to flush any remaining enqueued events."),

			service.NewFloatField("sampling_rate").
				Default(1.0).
				LintRule(`root = if this < 0 || this > 1 { ["sampling rate must be between 0.0 and 1.0" ] }`).
				Description("The rate at which events are sent to the server. A value of 0 disables capturing sentry events entirely. A value of 1 results in sending all events to Sentry. Any value in between results sending some percentage of events."),
		)
}

type captureProcessor struct {
	logger *service.Logger

	hub      *sentry.Hub
	messageQ *service.InterpolatedString
	contextQ *bloblang.Executor
	tagsQ    map[string]*service.InterpolatedString

	samplingRate float64
	flushTimeout time.Duration
}

func newCaptureProcessor(conf *service.ParsedConfig, mgr *service.Resources, opts ...clientOptionsFunc) (*captureProcessor, error) {
	logger := mgr.Logger()

	dsn, err := conf.FieldString("dsn")
	if err != nil {
		return nil, err
	}

	environment, err := conf.FieldString("environment")
	if err != nil {
		return nil, err
	}

	release, err := conf.FieldString("release")
	if err != nil {
		return nil, err
	}

	samplingRate, err := conf.FieldFloat("sampling_rate")
	if err != nil {
		return nil, err
	}

	inlevel, err := conf.FieldString("level")
	if err != nil {
		return nil, err
	}

	level, err := mapLevel(inlevel)
	if err != nil {
		return nil, err
	}

	messageQ, err := conf.FieldInterpolatedString("message")
	if err != nil {
		return nil, err
	}

	var contextQ *bloblang.Executor
	if conf.Contains("context") {
		cq, err := conf.FieldBloblang("context")
		if err != nil {
			return nil, err
		}
		contextQ = cq
	}

	var tagsQ map[string]*service.InterpolatedString
	if conf.Contains("tags") {
		tq, err := conf.FieldInterpolatedStringMap("tags")
		if err != nil {
			return nil, err
		}
		tagsQ = tq
	}

	flushTimeout, err := conf.FieldDuration("flush_timeout")
	if err != nil {
		return nil, err
	}

	transportMode, err := conf.FieldString("transport_mode")
	if err != nil {
		return nil, err
	}

	var transport sentry.Transport
	if transportMode == transportSync {
		transport = sentry.NewHTTPSyncTransport()
	}

	clientOptions := &sentry.ClientOptions{
		Dsn:         dsn,
		Environment: environment,
		Release:     release,
		SampleRate:  samplingRate,
		Transport:   transport,
	}

	for _, opt := range opts {
		clientOptions = opt(clientOptions)
	}

	client, err := sentry.NewClient(*clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create sentry client: %w", err)
	}

	version := cli.Version
	if len(version) > 200 {
		version = version[:200]
	}
	if version == "" {
		logger.Warn("failed to resolve benthos version to set as sentry tag")
		version = "unknown"
	}

	scope := sentry.NewScope()
	scope.SetLevel(level)
	scope.SetTag("benthos", version)

	label := mgr.Label()
	if label != "" {
		scope.SetTag("component", mgr.Label())
	}

	hub := sentry.NewHub(client, scope)

	return &captureProcessor{
		logger: logger,

		hub:      hub,
		messageQ: messageQ,
		contextQ: contextQ,
		tagsQ:    tagsQ,

		samplingRate: samplingRate,
		flushTimeout: flushTimeout,
	}, nil
}

func (proc *captureProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	out := service.MessageBatch{msg}

	// For historical reasons, a sampling rate of 0 or 1 on the sentry client
	// means _always_ capture the event. Let's correct this when the value is 0 to
	// never capture an event.
	if proc.samplingRate <= 0 {
		return out, nil
	}

	// Process is called in multiple goroutines. Sentry hub must be cloned for
	// each goroutine since it is not safe to share between goroutines.
	// See https://docs.sentry.io/platforms/go/concurrency/.
	hub := proc.hub.Clone()

	message, err := proc.messageQ.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to generate sentry message: %w", err)
	}

	sentryCtx, err := proc.queryContext(msg)
	if err != nil {
		return nil, err
	}

	tags := make(map[string]string, len(proc.tagsQ))
	for key, query := range proc.tagsQ {
		tag, err := query.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate sentry tag: %s: %w", key, err)
		}
		tags[key] = tag
	}

	hub.WithScope(func(scope *sentry.Scope) {
		scope.SetContexts(sentryCtx)
		scope.SetTags(tags)

		hub.CaptureMessage(message)
	})

	return out, nil
}

func (proc *captureProcessor) Close(ctx context.Context) error {
	if flushed := proc.hub.Flush(proc.flushTimeout); !flushed {
		return errors.New("failed to flush sentry events before timeout")
	}

	return nil
}

func (proc *captureProcessor) queryContext(msg *service.Message) (map[string]sentry.Context, error) {
	out := make(map[string]sentry.Context)
	if proc.contextQ == nil {
		return out, nil
	}

	result, err := msg.BloblangQuery(proc.contextQ)
	if err != nil {
		return nil, fmt.Errorf("failed to query for context: %w", err)
	}

	if result == nil {
		return out, nil
	}

	raw, err := result.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("failed to get structured data for context: %w", err)
	}

	c, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected object from context mapping but got: %T", raw)
	}

	for key, value := range c {
		// Silently omit null context values instead of erroring on them. Bloblang
		// authors can add more explicit checks in their mappings if needed
		// (e.g. not_empty() method)
		if value == nil {
			continue
		}

		contextVal, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected an object for context key: %s: got %T", key, value)
		}

		// Print a useful warning if user is going to override one of the context
		// keys that sentry-go automatically populates for each event.
		if key == "device" || key == "os" || key == "runtime" {
			proc.logger.Warnf("sentry context mapping will override a built-in context: %s", key)
		}

		out[key] = contextVal
	}

	return out, nil
}

func mapLevel(raw string) (sentry.Level, error) {
	switch raw {
	case "DEBUG":
		return sentry.LevelDebug, nil
	case "INFO":
		return sentry.LevelInfo, nil
	case "WARN":
		return sentry.LevelWarning, nil
	case "ERROR":
		return sentry.LevelError, nil
	case "FATAL":
		return sentry.LevelFatal, nil
	default:
		return sentry.Level(""), fmt.Errorf("unrecognized sentry level: %s", raw)
	}
}

func init() {
	if err := service.RegisterProcessor(
		"sentry_capture",
		newCaptureProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newCaptureProcessor(conf, mgr)
		},
	); err != nil {
		panic(err)
	}
}
