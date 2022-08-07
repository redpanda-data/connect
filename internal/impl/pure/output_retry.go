package pure

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
		return retryOutputFromConfig(conf.Retry, mgr)
	}), docs.ComponentSpec{
		Name: "retry",
		Summary: `
Attempts to write messages to a child output and if the write fails for any
reason the message is retried either until success or, if the retries or max
elapsed time fields are non-zero, either is reached.`,
		Description: `
All messages in Benthos are always retried on an output error, but this would
usually involve propagating the error back to the source of the message, whereby
it would be reprocessed before reaching the output layer once again.

This output type is useful whenever we wish to avoid reprocessing a message on
the event of a failed send. We might, for example, have a dedupe processor that
we want to avoid reapplying to the same message more than once in the pipeline.

Rather than retrying the same output you may wish to retry the send using a
different output target (a dead letter queue). In which case you should instead
use the ` + "[`fallback`](/docs/components/outputs/fallback)" + ` output type.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInt("max_retries", "The maximum number of retries before giving up on the request. If set to zero there is no discrete limit.").HasDefault(0).Advanced(),
			docs.FieldObject("backoff", "Control time intervals between retry attempts.").WithChildren(
				docs.FieldString("initial_interval", "The initial period to wait between retry attempts.").HasDefault("500ms"),
				docs.FieldString("max_interval", "The maximum period to wait between retry attempts.").HasDefault("3s"),
				docs.FieldString("max_elapsed_time", "The maximum period to wait before retry attempts are abandoned. If zero then no limit is used.").HasDefault("0s"),
			).Advanced(),
			docs.FieldOutput("output", "A child output."),
		),
		Categories: []string{
			"Utility",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// RetryOutputIndefinitely returns a wrapped variant of the provided output
// where send errors downstream are automatically caught and retried rather than
// propagated upstream as nacks.
func RetryOutputIndefinitely(mgr bundle.NewManagement, wrapped output.Streamed) (output.Streamed, error) {
	return newIndefiniteRetry(mgr, nil, wrapped)
}

func retryOutputFromConfig(conf output.RetryConfig, mgr bundle.NewManagement) (output.Streamed, error) {
	if conf.Output == nil {
		return nil, errors.New("cannot create retry output without a child")
	}

	wrapped, err := mgr.NewOutput(*conf.Output)
	if err != nil {
		return nil, err
	}

	var boffCtor func() backoff.BackOff
	if boffCtor, err = conf.GetCtor(); err != nil {
		return nil, err
	}

	return newIndefiniteRetry(mgr, boffCtor, wrapped)
}

func newIndefiniteRetry(mgr bundle.NewManagement, backoffCtor func() backoff.BackOff, wrapped output.Streamed) (*indefiniteRetry, error) {
	if backoffCtor == nil {
		tmpConf := output.NewRetryConfig()
		var err error
		if backoffCtor, err = tmpConf.GetCtor(); err != nil {
			return nil, err
		}
	}

	return &indefiniteRetry{
		log:             mgr.Logger(),
		wrapped:         wrapped,
		backoffCtor:     backoffCtor,
		transactionsOut: make(chan message.Transaction),
		shutSig:         shutdown.NewSignaller(),
	}, nil
}

// indefiniteRetry is an output type that continuously writes a message to a
// child output until the send is successful.
type indefiniteRetry struct {
	wrapped     output.Streamed
	backoffCtor func() backoff.BackOff

	log log.Modular

	transactionsIn  <-chan message.Transaction
	transactionsOut chan message.Transaction

	shutSig *shutdown.Signaller
}

func (r *indefiniteRetry) loop() {
	wg := sync.WaitGroup{}

	defer func() {
		wg.Wait()
		close(r.transactionsOut)
		r.wrapped.TriggerCloseNow()
		_ = r.wrapped.WaitForClose(context.Background())
		r.shutSig.ShutdownComplete()
	}()

	cnCtx, cnDone := r.shutSig.CloseNowCtx(context.Background())
	defer cnDone()

	errInterruptChan := make(chan struct{})
	var errLooped int64

	for !r.shutSig.ShouldCloseAtLeisure() {
		// Do not consume another message while pending messages are being
		// reattempted.
		for atomic.LoadInt64(&errLooped) > 0 {
			select {
			case <-errInterruptChan:
			case <-time.After(time.Millisecond * 100):
				// Just incase an interrupt doesn't arrive.
			case <-r.shutSig.CloseNowChan():
				return
			}
		}

		var tran message.Transaction
		var open bool
		select {
		case tran, open = <-r.transactionsIn:
			if !open {
				return
			}
		case <-r.shutSig.CloseNowChan():
			return
		}

		rChan := make(chan error)
		select {
		case r.transactionsOut <- message.NewTransaction(tran.Payload.ShallowCopy(), rChan):
		case <-r.shutSig.CloseNowChan():
			return
		}

		wg.Add(1)
		go func(ts message.Transaction, resChan chan error) {
			var backOff backoff.BackOff
			var resOut error
			var inErrLoop bool

			defer func() {
				wg.Done()
				if inErrLoop {
					atomic.AddInt64(&errLooped, -1)

					// We're exiting our error loop, so (attempt to) interrupt the
					// consumer.
					select {
					case errInterruptChan <- struct{}{}:
					default:
					}
				}
			}()

			for !r.shutSig.ShouldCloseNow() {
				var res error
				select {
				case res = <-resChan:
				case <-r.shutSig.CloseNowChan():
					return
				}

				if res != nil {
					if !inErrLoop {
						inErrLoop = true
						atomic.AddInt64(&errLooped, 1)
					}

					if backOff == nil {
						backOff = r.backoffCtor()
					}

					nextBackoff := backOff.NextBackOff()
					if nextBackoff == backoff.Stop {
						r.log.Errorf("Failed to send message: %v\n", res)
						resOut = errors.New("message failed to reach a target destination")
						break
					} else {
						r.log.Warnf("Failed to send message: %v\n", res)
					}
					select {
					case <-time.After(nextBackoff):
					case <-r.shutSig.CloseNowChan():
						return
					}

					select {
					case r.transactionsOut <- message.NewTransaction(ts.Payload.ShallowCopy(), resChan):
					case <-r.shutSig.CloseNowChan():
						return
					}
				} else {
					resOut = nil
					break
				}
			}

			if err := ts.Ack(cnCtx, resOut); err != nil && cnCtx.Err() != nil {
				return
			}
		}(tran, rChan)
	}
}

// Consume assigns a messages channel for the output to read.
func (r *indefiniteRetry) Consume(ts <-chan message.Transaction) error {
	if r.transactionsIn != nil {
		return component.ErrAlreadyStarted
	}
	if err := r.wrapped.Consume(r.transactionsOut); err != nil {
		return err
	}
	r.transactionsIn = ts
	go r.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (r *indefiniteRetry) Connected() bool {
	return r.wrapped.Connected()
}

// CloseAsync shuts down the Retry input and stops processing requests.
func (r *indefiniteRetry) TriggerCloseNow() {
	r.shutSig.CloseNow()
}

// WaitForClose blocks until the Retry input has closed down.
func (r *indefiniteRetry) WaitForClose(ctx context.Context) error {
	select {
	case <-r.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
