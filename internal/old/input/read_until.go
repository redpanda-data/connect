package input

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

func init() {
	Constructors[TypeReadUntil] = TypeSpec{
		constructor: fromSimpleConstructor(NewReadUntil),
		Summary: `
Reads messages from a child input until a consumed message passes a [Bloblang query](/docs/guides/bloblang/about/), at which point the input closes.`,
		Description: `
Messages are read continuously while the query check returns false, when the query returns true the message that triggered the check is sent out and the input is closed. Use this to define inputs where the stream should end once a certain message appears.

Sometimes inputs close themselves. For example, when the ` + "`file`" + ` input type reaches the end of a file it will shut down. By default this type will also shut down. If you wish for the input type to be restarted every time it shuts down until the query check is met then set ` + "`restart_input` to `true`." + `

### Metadata

A metadata key ` + "`benthos_read_until` containing the value `final`" + ` is added to the first part of the message that triggers the input to stop.`,
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Consume N Messages",
				Summary: "A common reason to use this input is to consume only N messages from an input and then stop. This can easily be done with the [`count` function](/docs/guides/bloblang/functions/#count):",
				Config: `
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
			},
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInput("input", "The child input to consume from."),
			docs.FieldBloblang(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether the input should now be closed.",
				`this.type == "foo"`,
				`count("messages") >= 100`,
			).HasDefault(""),
			docs.FieldBool("restart_input", "Whether the input should be reopened if it closes itself before the condition has resolved to true.").HasDefault(false),
		),
		Categories: []string{
			"Utility",
		},
	}
}

//------------------------------------------------------------------------------

// ReadUntilConfig contains configuration values for the ReadUntil input type.
type ReadUntilConfig struct {
	Input   *Config `json:"input" yaml:"input"`
	Restart bool    `json:"restart_input" yaml:"restart_input"`
	Check   string  `json:"check" yaml:"check"`
}

// NewReadUntilConfig creates a new ReadUntilConfig with default values.
func NewReadUntilConfig() ReadUntilConfig {
	return ReadUntilConfig{
		Input:   nil,
		Restart: false,
		Check:   "",
	}
}

//------------------------------------------------------------------------------

type dummyReadUntilConfig struct {
	Input   interface{} `json:"input" yaml:"input"`
	Restart bool        `json:"restart_input" yaml:"restart_input"`
	Check   string      `json:"check" yaml:"check"`
}

// MarshalJSON prints an empty object instead of nil.
func (r ReadUntilConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyReadUntilConfig{
		Input:   r.Input,
		Restart: r.Restart,
		Check:   r.Check,
	}
	if r.Input == nil {
		dummy.Input = struct{}{}
	}
	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (r ReadUntilConfig) MarshalYAML() (interface{}, error) {
	dummy := dummyReadUntilConfig{
		Input:   r.Input,
		Restart: r.Restart,
		Check:   r.Check,
	}
	if r.Input == nil {
		dummy.Input = struct{}{}
	}
	return dummy, nil
}

//------------------------------------------------------------------------------

// ReadUntil is an input type that continuously reads another input type until a
// condition returns true on a message consumed.
type ReadUntil struct {
	conf ReadUntilConfig

	wrapped input.Streamed
	check   *mapping.Executor

	wrapperMgr   interop.Manager
	wrapperLog   log.Modular
	wrapperStats metrics.Type

	stats metrics.Type
	log   log.Modular

	transactions chan message.Transaction

	shutSig *shutdown.Signaller
}

// NewReadUntil creates a new ReadUntil input type.
func NewReadUntil(
	conf Config,
	mgr interop.Manager,
	log log.Modular,
	stats metrics.Type,
) (input.Streamed, error) {
	if conf.ReadUntil.Input == nil {
		return nil, errors.New("cannot create read_until input without a child")
	}

	wMgr := mgr.IntoPath("read_until", "input")
	wLog, wStats := wMgr.Logger(), wMgr.Metrics()
	wrapped, err := New(*conf.ReadUntil.Input, wMgr, wLog, wStats)
	if err != nil {
		return nil, err
	}

	var check *mapping.Executor
	if len(conf.ReadUntil.Check) > 0 {
		if check, err = mgr.BloblEnvironment().NewMapping(conf.ReadUntil.Check); err != nil {
			return nil, fmt.Errorf("failed to parse check query: %w", err)
		}
	}

	if check == nil {
		return nil, errors.New("a check query is required")
	}

	rdr := &ReadUntil{
		conf: conf.ReadUntil,

		wrapperLog:   wLog,
		wrapperStats: wStats,
		wrapperMgr:   wMgr,

		log:          log,
		stats:        stats,
		wrapped:      wrapped,
		check:        check,
		transactions: make(chan message.Transaction),

		shutSig: shutdown.NewSignaller(),
	}

	go rdr.loop()
	return rdr, nil
}

//------------------------------------------------------------------------------

func (r *ReadUntil) loop() {
	defer func() {
		if r.wrapped != nil {
			r.wrapped.CloseAsync()

			// TODO: This triggers a tier 2 shutdown in the child.
			err := r.wrapped.WaitForClose(time.Second)
			for ; err != nil; err = r.wrapped.WaitForClose(time.Second) {
			}
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
		if r.wrapped == nil {
			if r.conf.Restart {
				select {
				case <-time.After(restartBackoff.NextBackOff()):
				case <-r.shutSig.CloseAtLeisureChan():
					return
				}
				var err error
				if r.wrapped, err = New(*r.conf.Input, r.wrapperMgr, r.wrapperLog, r.wrapperStats); err != nil {
					r.log.Errorf("Failed to create input '%v': %v\n", r.conf.Input.Type, err)
					return
				}
			} else {
				return
			}
		}

		var tran message.Transaction
		select {
		case tran, open = <-r.wrapped.TransactionChan():
			if !open {
				r.wrapped = nil
				continue runLoop
			}
			restartBackoff.Reset()
		case <-r.shutSig.CloseAtLeisureChan():
			return
		}

		check, err := r.check.QueryPart(0, tran.Payload)
		if err != nil {
			check = false
			r.log.Errorf("Failed to execute check query: %v\n", err)
		}
		if !check {
			select {
			case r.transactions <- tran:
			case <-r.shutSig.CloseAtLeisureChan():
				return
			}
			continue
		}

		tran.Payload.Get(0).MetaSet("benthos_read_until", "final")

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
func (r *ReadUntil) TransactionChan() <-chan message.Transaction {
	return r.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (r *ReadUntil) Connected() bool {
	return r.wrapped.Connected()
}

// CloseAsync shuts down the ReadUntil input and stops processing requests.
func (r *ReadUntil) CloseAsync() {
	r.shutSig.CloseAtLeisure()
}

// WaitForClose blocks until the ReadUntil input has closed down.
func (r *ReadUntil) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
