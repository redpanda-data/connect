package input

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/cenkalti/backoff/v4"
)

//------------------------------------------------------------------------------

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
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("input", "The child input to consume from.").HasType(docs.FieldTypeInput),
			docs.FieldBloblang(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether the input should now be closed.",
				`this.type == "foo"`,
				`count("messages") >= 100`,
			).HasDefault(""),
			docs.FieldCommon("restart_input", "Whether the input should be reopened if it closes itself before the condition has resolved to true."),
		},
		Categories: []Category{
			CategoryUtility,
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
	running int32
	conf    ReadUntilConfig

	wrapped input.Streamed
	check   *mapping.Executor

	wrapperMgr   interop.Manager
	wrapperLog   log.Modular
	wrapperStats metrics.Type

	stats metrics.Type
	log   log.Modular

	transactions chan message.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
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
		return nil, fmt.Errorf("failed to create input '%v': %v", conf.ReadUntil.Input.Type, err)
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
		running: 1,
		conf:    conf.ReadUntil,

		wrapperLog:   wLog,
		wrapperStats: wStats,
		wrapperMgr:   wMgr,

		log:          log,
		stats:        stats,
		wrapped:      wrapped,
		check:        check,
		transactions: make(chan message.Transaction),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
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
		close(r.closedChan)
	}()

	// Prevents busy loop when an input never yields messages.
	restartBackoff := backoff.NewExponentialBackOff()
	restartBackoff.InitialInterval = time.Millisecond
	restartBackoff.MaxInterval = time.Millisecond * 100
	restartBackoff.MaxElapsedTime = 0

	var open bool

runLoop:
	for atomic.LoadInt32(&r.running) == 1 {
		if r.wrapped == nil {
			if r.conf.Restart {
				select {
				case <-time.After(restartBackoff.NextBackOff()):
				case <-r.closeChan:
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
		case <-r.closeChan:
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
			case <-r.closeChan:
				return
			}
			continue
		}

		tran.Payload.Get(0).MetaSet("benthos_read_until", "final")

		// If this transaction succeeds we shut down.
		tmpRes := make(chan response.Error)
		select {
		case r.transactions <- message.NewTransaction(tran.Payload, tmpRes):
		case <-r.closeChan:
			return
		}

		var res response.Error
		select {
		case res, open = <-tmpRes:
			if !open {
				return
			}
			streamEnds := res.AckError() == nil
			select {
			case tran.ResponseChan <- res:
			case <-r.closeChan:
				return
			}
			if streamEnds {
				return
			}
		case <-r.closeChan:
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
	if atomic.CompareAndSwapInt32(&r.running, 1, 0) {
		close(r.closeChan)
	}
}

// WaitForClose blocks until the ReadUntil input has closed down.
func (r *ReadUntil) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
