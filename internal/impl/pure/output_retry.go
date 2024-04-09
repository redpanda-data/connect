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
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	crboFieldMaxRetries     = "max_retries"
	crboFieldBackOff        = "backoff"
	crboFieldInitInterval   = "initial_interval"
	crboFieldMaxInterval    = "max_interval"
	crboFieldMaxElapsedTime = "max_elapsed_time"
)

func CommonRetryBackOffFields(
	defaultMaxRetries int,
	defaultInitInterval string,
	defaultMaxInterval string,
	defaultMaxElapsed string,
) []*service.ConfigField {
	return []*service.ConfigField{
		service.NewIntField(crboFieldMaxRetries).
			Description("The maximum number of retries before giving up on the request. If set to zero there is no discrete limit.").
			Default(defaultMaxRetries).
			Advanced(),
		service.NewObjectField(crboFieldBackOff,
			service.NewDurationField(crboFieldInitInterval).
				Description("The initial period to wait between retry attempts.").
				Default(defaultInitInterval),
			service.NewDurationField(crboFieldMaxInterval).
				Description("The maximum period to wait between retry attempts.").
				Default(defaultMaxInterval),
			service.NewDurationField(crboFieldMaxElapsedTime).
				Description("The maximum period to wait before retry attempts are abandoned. If zero then no limit is used.").
				Default(defaultMaxElapsed),
		).
			Description("Control time intervals between retry attempts.").
			Advanced(),
	}
}

func fieldDurationOrEmptyStr(pConf *service.ParsedConfig, path ...string) (time.Duration, error) {
	if dStr, err := pConf.FieldString(path...); err == nil && dStr == "" {
		return 0, nil
	}
	return pConf.FieldDuration(path...)
}

func CommonRetryBackOffCtorFromParsed(pConf *service.ParsedConfig) (ctor func() backoff.BackOff, err error) {
	var maxRetries int
	if maxRetries, err = pConf.FieldInt(crboFieldMaxRetries); err != nil {
		return
	}

	var initInterval, maxInterval, maxElapsed time.Duration
	if pConf.Contains(crboFieldBackOff) {
		bConf := pConf.Namespace(crboFieldBackOff)
		if initInterval, err = fieldDurationOrEmptyStr(bConf, crboFieldInitInterval); err != nil {
			return
		}
		if maxInterval, err = fieldDurationOrEmptyStr(bConf, crboFieldMaxInterval); err != nil {
			return
		}
		if maxElapsed, err = fieldDurationOrEmptyStr(bConf, crboFieldMaxElapsedTime); err != nil {
			return
		}
	}

	return func() backoff.BackOff {
		boff := backoff.NewExponentialBackOff()

		boff.InitialInterval = initInterval
		boff.MaxInterval = maxInterval
		boff.MaxElapsedTime = maxElapsed

		if maxRetries > 0 {
			return backoff.WithMaxRetries(boff, uint64(maxRetries))
		}
		return boff
	}, nil
}

//------------------------------------------------------------------------------

const roFieldOutput = "output"

func retryOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary("Attempts to write messages to a child output and if the write fails for any reason the message is retried either until success or, if the retries or max elapsed time fields are non-zero, either is reached.").
		Description(`
All messages in Benthos are always retried on an output error, but this would usually involve propagating the error back to the source of the message, whereby it would be reprocessed before reaching the output layer once again.

This output type is useful whenever we wish to avoid reprocessing a message on the event of a failed send. We might, for example, have a dedupe processor that we want to avoid reapplying to the same message more than once in the pipeline.

Rather than retrying the same output you may wish to retry the send using a different output target (a dead letter queue). In which case you should instead use the ` + "[`fallback`](/docs/components/outputs/fallback)" + ` output type.`).
		Fields(CommonRetryBackOffFields(0, "500ms", "3s", "0s")...).
		Fields(
			service.NewOutputField(roFieldOutput).
				Description("A child output."),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"retry", retryOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			maxInFlight = 1

			var s output.Streamed
			if s, err = retryOutputFromConfig(conf, interop.UnwrapManagement(mgr)); err != nil {
				return
			}
			out = interop.NewUnwrapInternalOutput(s)
			return
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

func retryOutputFromConfig(conf *service.ParsedConfig, mgr bundle.NewManagement) (output.Streamed, error) {
	pOut, err := conf.FieldOutput(dooFieldOutput)
	if err != nil {
		return nil, err
	}

	var boffCtor func() backoff.BackOff
	if boffCtor, err = CommonRetryBackOffCtorFromParsed(conf); err != nil {
		return nil, err
	}

	return newIndefiniteRetry(mgr, boffCtor, interop.UnwrapOwnedOutput(pOut))
}

func newIndefiniteRetry(mgr bundle.NewManagement, backoffCtor func() backoff.BackOff, wrapped output.Streamed) (*indefiniteRetry, error) {
	if backoffCtor == nil {
		backoffCtor = func() backoff.BackOff {
			boff := backoff.NewExponentialBackOff()
			boff.InitialInterval = time.Millisecond * 500
			boff.MaxInterval = time.Second * 3
			boff.MaxElapsedTime = 0
			return boff
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
						r.log.Error("Failed to send message: %v\n", res)
						resOut = errors.New("message failed to reach a target destination")
						break
					}

					r.log.Warn("Failed to send message: %v\n", res)

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
