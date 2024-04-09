package pure

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	ruiFieldInput       = "input"
	ruiFieldRestart     = "restart_input"
	ruiFieldCheck       = "check"
	ruiFieldIdleTimeout = "idle_timeout"
)

func readUntilInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Utility").
		Summary("Reads messages from a child input until a consumed message passes a [Bloblang query](/docs/guides/bloblang/about/), at which point the input closes. It is also possible to configure a timeout after which the input is closed if no new messages arrive in that period.").
		Description(`
Messages are read continuously while the query check returns false, when the query returns true the message that triggered the check is sent out and the input is closed. Use this to define inputs where the stream should end once a certain message appears.

If the idle timeout is configured, the input will be closed if no new messages arrive after that period of time. Use this field if you want to empty out and close an input that doesn't have a logical end.

Sometimes inputs close themselves. For example, when the `+"`file`"+` input type reaches the end of a file it will shut down. By default this type will also shut down. If you wish for the input type to be restarted every time it shuts down until the query check is met then set `+"`restart_input` to `true`."+`

### Metadata

A metadata key `+"`benthos_read_until` containing the value `final`"+` is added to the first part of the message that triggers the input to stop.`).
		Example(
			"Consume N Messages",
			"A common reason to use this input is to consume only N messages from an input and then stop. This can easily be done with the [`count` function](/docs/guides/bloblang/functions/#count):",
			`
# Only read 100 messages, and then exit.
input:
  read_until:
    check: count("messages") >= 100
    input:
      kafka:
        addresses: [ TODO ]
        topics: [ foo, bar ]
        consumer_group: foogroup
`,
		).
		Example(
			"Read from a kafka and close when empty",
			"A common reason to use this input is a job that consumes all messages and exits once its empty:",
			`
# Consumes all messages and exit when the last message was consumed 5s ago.
input:
  read_until:
    idle_timeout: 5s
    input:
      kafka:
        addresses: [ TODO ]
        topics: [ foo, bar ]
        consumer_group: foogroup
`,
		).Fields(
		service.NewInputField(ruiFieldInput).
			Description("The child input to consume from."),
		service.NewBloblangField(ruiFieldCheck).
			Description("A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether the input should now be closed.").
			Examples(
				`this.type == "foo"`,
				`count("messages") >= 100`,
			).
			Optional(),
		service.NewDurationField(ruiFieldIdleTimeout).
			Description("The maximum amount of time without receiving new messages after which the input is closed.").
			Example("5s").
			Optional(),
		service.NewBoolField(ruiFieldRestart).
			Description("Whether the input should be reopened if it closes itself before the condition has resolved to true.").
			Default(false),
	)
}

func init() {
	err := service.RegisterBatchInput("read_until", readUntilInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newReadUntilInputFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalInput(i), nil
		})
	if err != nil {
		panic(err)
	}
}

type readUntilInput struct {
	restart bool

	wrappedInputLocked *atomic.Pointer[input.Streamed]
	check              *mapping.Executor
	idleTimeout        time.Duration

	wrappedCtor func() (input.Streamed, error)

	log log.Modular

	transactions chan message.Transaction

	shutSig *shutdown.Signaller
}

func newReadUntilInputFromParsed(conf *service.ParsedConfig, res *service.Resources) (input.Streamed, error) {
	mgr := interop.UnwrapManagement(res)

	wrappedCtor := func() (input.Streamed, error) {
		ownedInput, err := conf.FieldInput(ruiFieldInput)
		if err != nil {
			return nil, err
		}
		return interop.UnwrapOwnedInput(ownedInput), nil
	}

	wrapped, err := wrappedCtor()
	if err != nil {
		return nil, err
	}

	restart, err := conf.FieldBool(ruiFieldRestart)
	if err != nil {
		return nil, err
	}

	var check *mapping.Executor
	if checkStr, _ := conf.FieldString(ruiFieldCheck); checkStr != "" {
		if check, err = mgr.BloblEnvironment().NewMapping(checkStr); err != nil {
			return nil, fmt.Errorf("failed to parse check query: %w", err)
		}
	}

	var idleTimeout time.Duration = -1
	if idleTimeoutStr, _ := conf.FieldString(ruiFieldIdleTimeout); idleTimeoutStr != "" {
		if idleTimeout, err = time.ParseDuration(idleTimeoutStr); err != nil {
			return nil, fmt.Errorf("failed to parse idle_timeout string: %v", err)
		}
	}

	if check == nil && idleTimeout < 0 {
		return nil, errors.New("it is required to set either check or idle_timeout")
	}

	wInputLocked := &atomic.Pointer[input.Streamed]{}
	wInputLocked.Store(&wrapped)
	rdr := &readUntilInput{
		restart: restart,

		wrappedCtor:        wrappedCtor,
		wrappedInputLocked: wInputLocked,

		log:          mgr.Logger(),
		check:        check,
		idleTimeout:  idleTimeout,
		transactions: make(chan message.Transaction),

		shutSig: shutdown.NewSignaller(),
	}

	go rdr.loop()
	return rdr, nil
}

func (r *readUntilInput) loop() {
	defer func() {
		wrappedP := r.wrappedInputLocked.Load()
		if wrappedP != nil {
			wrapped := *wrappedP
			wrapped.TriggerStopConsuming()
			wrapped.TriggerCloseNow()
			_ = wrapped.WaitForClose(context.Background())
		}

		close(r.transactions)
		r.shutSig.ShutdownComplete()
	}()

	// Prevents busy loop when an input never yields messages.
	restartBackoff := backoff.NewExponentialBackOff()
	restartBackoff.InitialInterval = time.Millisecond
	restartBackoff.MaxInterval = time.Millisecond * 100
	restartBackoff.MaxElapsedTime = 0

	var open bool

	closeCtx, done := r.shutSig.CloseAtLeisureCtx(context.Background())
	defer done()

runLoop:
	for !r.shutSig.ShouldCloseAtLeisure() {
		var wrapped input.Streamed
		wrappedP := r.wrappedInputLocked.Load()
		if wrappedP == nil {
			if r.restart {
				select {
				case <-time.After(restartBackoff.NextBackOff()):
				case <-r.shutSig.CloseAtLeisureChan():
					return
				}
				var err error
				if wrapped, err = r.wrappedCtor(); err != nil {
					r.log.Error("Failed to create input: %v\n", err)
					return
				}
				r.wrappedInputLocked.Store(&wrapped)
			} else {
				return
			}
		} else {
			wrapped = *wrappedP
		}

		var tran message.Transaction
		{
			timeoutChan, timeoutDone := resetIdleTimeout(r.idleTimeout)
			select {
			case tran, open = <-wrapped.TransactionChan():
				timeoutDone()
				if !open {
					r.wrappedInputLocked.Store(nil)
					continue runLoop
				}
				restartBackoff.Reset()
			case <-r.shutSig.CloseAtLeisureChan():
				timeoutDone()
				return
			case <-timeoutChan:
				timeoutDone()
				r.log.Info("Idle timeout reached")
				return
			}
		}

		var err error
		check := false
		if r.check != nil {
			check, err = r.check.QueryPart(0, tran.Payload)
			if err != nil {
				check = false
				r.log.Error("Failed to execute check query: %v\n", err)
			}
		}
		if !check {
			select {
			case r.transactions <- tran:
			case <-r.shutSig.CloseAtLeisureChan():
				return
			}
			continue
		}

		tran.Payload.Get(0).MetaSetMut("benthos_read_until", "final")

		// If this transaction succeeds we shut down.
		tmpRes := make(chan error)
		select {
		case r.transactions <- message.NewTransaction(tran.Payload, tmpRes):
		case <-r.shutSig.CloseAtLeisureChan():
			return
		}

		var res error
		select {
		case res, open = <-tmpRes:
			if !open {
				return
			}
			streamEnds := res == nil
			if err := tran.Ack(closeCtx, res); err != nil && r.shutSig.ShouldCloseAtLeisure() {
				return
			}
			if streamEnds {
				return
			}
		case <-r.shutSig.CloseAtLeisureChan():
			return
		}
	}
}

// TransactionChan returns a transactions channel for consuming messages from
// this input type.
func (r *readUntilInput) TransactionChan() <-chan message.Transaction {
	return r.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (r *readUntilInput) Connected() bool {
	wrappedP := r.wrappedInputLocked.Load()
	if wrappedP != nil {
		i := *wrappedP
		return i.Connected()
	}
	return false
}

func (r *readUntilInput) TriggerStopConsuming() {
	r.shutSig.CloseAtLeisure()
}

func (r *readUntilInput) TriggerCloseNow() {
	r.shutSig.CloseNow()
}

func (r *readUntilInput) WaitForClose(ctx context.Context) error {
	select {
	case <-r.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func resetIdleTimeout(d time.Duration) (waitForTimeout <-chan time.Time, done func()) {
	done = func() {}
	if d > 0 {
		timer := time.NewTimer(d)
		waitForTimeout = timer.C
		done = func() {
			_ = timer.Stop()
		}
	}
	return
}
