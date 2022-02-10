package output

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDropOn] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
			if conf.DropOn.Output == nil {
				return nil, errors.New("cannot create a drop_on output without a child")
			}
			wrapped, err := New(*conf.DropOn.Output, mgr, log, stats)
			if err != nil {
				return nil, fmt.Errorf("failed to create output '%v': %v", conf.DropOn.Output.Type, err)
			}
			return newDropOn(conf.DropOn.DropOnConditions, wrapped, log, stats)
		}),
		Summary: `
Attempts to write messages to a child output and if the write fails for one of a list of configurable reasons the message is dropped instead of being reattempted.`,
		Description: `
Regular Benthos outputs will apply back pressure when downstream services aren't accessible, and Benthos retries (or nacks) all messages that fail to be delivered. However, in some circumstances, or for certain output types, we instead might want to relax these mechanisms, which is when this output becomes useful.`,
		Categories: []Category{
			CategoryUtility,
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("error", "Whether messages should be dropped when the child output returns an error. For example, this could be when an http_client output gets a 4XX response code."),
			docs.FieldCommon("back_pressure", "An optional duration string that determines the maximum length of time to wait for a given message to be accepted by the child output before the message should be dropped instead. The most common reason for an output to block is when waiting for a lost connection to be re-established. Once a message has been dropped due to back pressure all subsequent messages are dropped immediately until the output is ready to process them again. Note that if `error` is set to `false` and this field is specified then messages dropped due to back pressure will return an error response.", "30s", "1m"),
			docs.FieldCommon("output", "A child output.").HasType(docs.FieldTypeOutput),
		},
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Dropping failed HTTP requests",
				Summary: "In this example we have a fan_out broker, where we guarantee delivery to our Kafka output, but drop messages if they fail our secondary HTTP client output.",
				Config: `
output:
  broker:
    pattern: fan_out
    outputs:
      - kafka:
          addresses: [ foobar:6379 ]
          topic: foo
      - drop_on:
          error: true
          output:
            http_client:
              url: http://example.com/foo/messages
              verb: POST
`,
			},
			{
				Title:   "Dropping from outputs that cannot connect",
				Summary: "Most outputs that attempt to establish and long-lived connection will apply back-pressure when the connection is lost. The following example has a websocket output where if it takes longer than 10 seconds to establish a connection, or recover a lost one, pending messages are dropped.",
				Config: `
output:
  drop_on:
    back_pressure: 10s
    output:
      websocket:
        url: ws://example.com/foo/messages
`,
			},
		},
	}
}

//------------------------------------------------------------------------------

// DropOnConditions is a config struct representing the different circumstances
// under which messages should be dropped.
type DropOnConditions struct {
	Error        bool   `json:"error" yaml:"error"`
	BackPressure string `json:"back_pressure" yaml:"back_pressure"`
}

// DropOnConfig contains configuration values for the DropOn output type.
type DropOnConfig struct {
	DropOnConditions `json:",inline" yaml:",inline"`
	Output           *Config `json:"output" yaml:"output"`
}

// NewDropOnConfig creates a new DropOnConfig with default values.
func NewDropOnConfig() DropOnConfig {
	return DropOnConfig{
		DropOnConditions: DropOnConditions{
			Error:        false,
			BackPressure: "",
		},
		Output: nil,
	}
}

//------------------------------------------------------------------------------

type dummyDropOnConfig struct {
	DropOnConditions `json:",inline" yaml:",inline"`
	Output           interface{} `json:"output" yaml:"output"`
}

// MarshalJSON prints an empty object instead of nil.
func (d DropOnConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyDropOnConfig{
		Output:           d.Output,
		DropOnConditions: d.DropOnConditions,
	}
	if d.Output == nil {
		dummy.Output = struct{}{}
	}
	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (d DropOnConfig) MarshalYAML() (interface{}, error) {
	dummy := dummyDropOnConfig{
		Output:           d.Output,
		DropOnConditions: d.DropOnConditions,
	}
	if d.Output == nil {
		dummy.Output = struct{}{}
	}
	return dummy, nil
}

//------------------------------------------------------------------------------

// dropOn attempts to forward messages to a child output, but under certain
// conditions will abandon the request and drop the message.
type dropOn struct {
	stats metrics.Type
	log   log.Modular

	onError        bool
	onBackpressure time.Duration
	wrapped        output.Streamed

	transactionsIn  <-chan message.Transaction
	transactionsOut chan message.Transaction

	ctx        context.Context
	done       func()
	closedChan chan struct{}
}

func newDropOn(conf DropOnConditions, wrapped output.Streamed, log log.Modular, stats metrics.Type) (*dropOn, error) {
	var backPressure time.Duration
	if len(conf.BackPressure) > 0 {
		var err error
		if backPressure, err = time.ParseDuration(conf.BackPressure); err != nil {
			return nil, fmt.Errorf("failed to parse back_pressure duration: %w", err)
		}
	}

	ctx, done := context.WithCancel(context.Background())
	return &dropOn{
		log:             log,
		stats:           stats,
		wrapped:         wrapped,
		transactionsOut: make(chan message.Transaction),

		onError:        conf.Error,
		onBackpressure: backPressure,

		ctx:        ctx,
		done:       done,
		closedChan: make(chan struct{}),
	}, nil
}

//------------------------------------------------------------------------------

func (d *dropOn) loop() {
	// Metrics paths
	var (
		mDropped      = d.stats.GetCounter("drop_on.dropped")
		mDroppedBatch = d.stats.GetCounter("drop_on.batch.dropped")
	)

	defer func() {
		close(d.transactionsOut)
		d.wrapped.CloseAsync()
		_ = d.wrapped.WaitForClose(shutdown.MaximumShutdownWait())
		close(d.closedChan)
	}()

	resChan := make(chan response.Error)

	var gotBackPressure bool
	for {
		var ts message.Transaction
		var open bool
		select {
		case ts, open = <-d.transactionsIn:
			if !open {
				return
			}
		case <-d.ctx.Done():
			return
		}

		var res response.Error
		if d.onBackpressure > 0 {
			if !func() bool {
				// Use a ticker here and call Stop explicitly.
				ticker := time.NewTicker(d.onBackpressure)
				defer ticker.Stop()

				if gotBackPressure {
					select {
					case d.transactionsOut <- message.NewTransaction(ts.Payload, resChan):
						gotBackPressure = false
					default:
					}
				} else {
					select {
					case d.transactionsOut <- message.NewTransaction(ts.Payload, resChan):
					case <-ticker.C:
						gotBackPressure = true
					case <-d.ctx.Done():
						return false
					}
				}
				if !gotBackPressure {
					select {
					case res = <-resChan:
					case <-ticker.C:
						gotBackPressure = true
						go func() {
							// We must pull the response that we're due, since
							// the component isn't being shut down.
							<-resChan
						}()
					case <-d.ctx.Done():
						return false
					}
				}
				if gotBackPressure {
					mDropped.Incr(int64(ts.Payload.Len()))
					mDroppedBatch.Incr(1)
					d.log.Warnln("Message dropped due to back pressure.")
					if d.onError {
						res = response.NewError(nil)
					} else {
						res = response.NewError(fmt.Errorf("experienced back pressure beyond: %v", d.onBackpressure))
					}
				}
				return true
			}() {
				return
			}
		} else {
			// Push data as usual, if the output blocks due to a disconnect then
			// we wait as long as it takes.
			select {
			case d.transactionsOut <- message.NewTransaction(ts.Payload, resChan):
			case <-d.ctx.Done():
				return
			}
			select {
			case res = <-resChan:
			case <-d.ctx.Done():
				return
			}
		}

		if res.AckError() != nil && d.onError {
			mDropped.Incr(int64(ts.Payload.Len()))
			mDroppedBatch.Incr(1)
			d.log.Warnf("Message dropped due to: %v\n", res.AckError())
			res = response.NewError(nil)
		}

		select {
		case ts.ResponseChan <- res:
		case <-d.ctx.Done():
			return
		}
	}
}

// Consume assigns a messages channel for the output to read.
func (d *dropOn) Consume(ts <-chan message.Transaction) error {
	if d.transactionsIn != nil {
		return component.ErrAlreadyStarted
	}
	if err := d.wrapped.Consume(d.transactionsOut); err != nil {
		return err
	}
	d.transactionsIn = ts
	go d.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (d *dropOn) Connected() bool {
	return d.wrapped.Connected()
}

func (d *dropOn) MaxInFlight() (int, bool) {
	return output.GetMaxInFlight(d.wrapped)
}

// CloseAsync shuts down the DropOnError input and stops processing requests.
func (d *dropOn) CloseAsync() {
	d.done()
}

// WaitForClose blocks until the DropOnError input has closed down.
func (d *dropOn) WaitForClose(timeout time.Duration) error {
	select {
	case <-d.closedChan:
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
