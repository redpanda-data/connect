package generic

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/hashicorp/go-multierror"
)

func nilAck(context.Context, error) error {
	return nil
}

// CorkConfig holds configuration options for the cork input
type CorkConfig struct {
	Logger *service.Logger `json:"-" yaml:"-"`

	InitiallyCorked bool                `json:"initially_corked" yaml:"initially_corked"`
	Input           *service.OwnedInput `json:"input" yaml:"input"`
	Signal          *service.OwnedInput `json:"signal" yaml:"signal"`
}

type corkInput struct {
	logger *service.Logger

	closedCorkC chan struct{}
	readyCVal   atomic.Value

	input *service.OwnedInput

	signal          *service.OwnedInput
	signalErrC      chan error
	signalCtx       context.Context
	signalCtxCancel context.CancelFunc
}

func newCorkInput(cfg *CorkConfig) *corkInput {
	closedCorkC := make(chan struct{})
	close(closedCorkC)

	readyC := make(chan struct{})
	var readyCVal atomic.Value
	readyCVal.Store(readyC)

	if !cfg.InitiallyCorked {
		close(readyC)
	}

	signalCtx, signalCtxCancel := context.WithCancel(context.Background())

	inp := corkInput{
		logger:          cfg.Logger,
		readyCVal:       readyCVal,
		closedCorkC:     closedCorkC,
		input:           cfg.Input,
		signal:          cfg.Signal,
		signalErrC:      make(chan error, 1),
		signalCtx:       signalCtx,
		signalCtxCancel: signalCtxCancel,
	}

	return &inp
}

func (ci *corkInput) Connect(ctx context.Context) error {
	go ci.signalLoop()
	return nil
}

func (ci *corkInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case err := <-ci.signalErrC:
		ci.logger.Errorf("error reading cork signals: %s", err)
		return nil, nilAck, multierror.Append(service.ErrEndOfInput, err)
	default:
		// noop
	}

	// Placing the wait call in a go routine and using a channel for coordination
	// so that we do not block the benthos process from terminating gracefully
	ready := ci.readyCVal.Load().(chan struct{})
	select {
	case <-ready:
	default:
		return nil, nilAck, nil
	}

	return ci.input.ReadBatch(ctx)
}

func (ci *corkInput) Close(ctx context.Context) error {
	ci.signalCtxCancel()

	var result error
	if err := ci.signal.Close(ctx); err != nil {
		result = multierror.Append(result, err)
	}
	if err := ci.input.Close(ctx); err != nil {
		result = multierror.Append(result, err)
	}

	return result
}

func (ci *corkInput) signalLoop() {
	ctx, cancel := shutdown.NewSignaller().CloseAtLeisureCtx(ci.signalCtx)
	defer cancel()
	defer ci.logger.Debug("cork signal reader stopped")

	for {
		batch, ack, err := ci.signal.ReadBatch(ctx)
		if err == service.ErrEndOfInput {
			return
		}
		if err != nil {
			ci.signalErrC <- fmt.Errorf("failed to read cork signal: %w", err)
			return
		}

		if len(batch) == 0 {
			if err := ack(ctx, nil); err != nil {
				ci.signalErrC <- fmt.Errorf("failed to ack cork signal: %w", err)
				return
			}
			continue
		}

		var message *service.Message
		for _, msg := range batch {
			if msg != nil {
				message = msg
				break
			}
		}

		if message == nil {
			if err := ack(ctx, nil); err != nil {
				ci.signalErrC <- fmt.Errorf("failed to ack cork signal: %w", err)
				return
			}
			continue
		}

		if mErr := message.GetError(); mErr != nil {
			if err := ack(ctx, mErr); err != nil {
				ci.signalErrC <- fmt.Errorf("failed to nack cork signal: %w", err)
				return
			}
			continue
		}

		bs, bErr := message.AsBytes()
		if bErr != nil {
			if err := ack(ctx, bErr); err != nil {
				ci.signalErrC <- fmt.Errorf("failed to nack cork signal: %w", err)
				return
			}
			continue
		}

		switch string(bs) {
		case "cork":
			ci.cork()
		case "uncork":
			ci.uncork()
		default:
			// noop
		}

		if err := ack(ctx, nil); err != nil {
			ci.signalErrC <- fmt.Errorf("failed to ack cork signal: %w", err)
			return
		}
	}
}

// Corked returns the point-in-time status of the input. This is mostly useful
// in sequencing test code.
func (ci *corkInput) Corked() bool {
	return ci.readyCVal.Load().(chan struct{}) != ci.closedCorkC
}

func (ci *corkInput) cork() {
	if ci.readyCVal.CompareAndSwap(ci.closedCorkC, make(chan struct{})) {
		ci.logger.Debug("corking input")
	}
}

func (ci *corkInput) uncork() {
	if old := ci.readyCVal.Swap(ci.closedCorkC); old != ci.closedCorkC {
		ci.logger.Debug("uncorking input")
		close(old.(chan struct{}))
	}
}

func newCorkFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*corkInput, error) {
	wrapped, err := conf.FieldInput("input")
	if err != nil {
		return nil, err
	}

	signal, err := conf.FieldInput("signal")
	if err != nil {
		return nil, err
	}

	initCorked, err := conf.FieldBool("initially_corked")
	if err != nil {
		return nil, err
	}

	return newCorkInput(&CorkConfig{
		Input:           wrapped,
		Signal:          signal,
		InitiallyCorked: initCorked,
		Logger:          logger,
	}), nil
}

func newCorkConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Summary("Use signals from one input to suspend or resume reads from another input").
		Categories("Utility").
		Description(`
This input operates in of two modes: corked and uncorked.
When corked, reading from the underlying input is suspended.
When uncorked, reading messages from the underlying input works as normal.

When there are no more messages on the signal input, this input will continue running with the last mode that was set.
`).
		Field(service.NewBoolField("initially_corked").
			Default(true).
			Description("Sets the cork mode at the start of the process"),
		).
		Field(service.NewInputField("input").
			Description("The input to wrap with a cork"),
		).
		Field(service.NewInputField("signal").
			Description(`
An input that supplies cork/uncork signals.

It must produce one of two string values that control the read modes:

- ` + "`\"cork\"`" + `: suspends reading from the underlying input
- ` + "`\"uncork\"`" + `: resumes reading from the underlying input

Any other values are ignored.
`).
			Example(map[string]interface{}{
				"broker": map[string]interface{}{
					"inputs": []interface{}{
						map[string]interface{}{
							"http_server": map[string]interface{}{
								"path":          "/cork",
								"allowed_verbs": []string{"POST"},
							},
							"processors": []interface{}{
								map[string]interface{}{"bloblang": "root = \"cork\""},
							},
						},
						map[string]interface{}{
							"http_server": map[string]interface{}{
								"path":          "/uncork",
								"allowed_verbs": []string{"POST"},
							},
							"processors": []interface{}{
								map[string]interface{}{"bloblang": "root = \"uncork\""},
							},
						},
					},
				},
			}),
		)
}

func init() {
	err := service.RegisterBatchInput(
		"cork",
		newCorkConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newCorkFromConfig(conf, mgr.Logger())
		},
	)

	if err != nil {
		panic(err)
	}
}
