package output

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDropOnError] = TypeSpec{
		constructor: fromSimpleConstructor(NewDropOnError),
		Status:      docs.StatusDeprecated,
		Summary: `
Attempts to write messages to a child output and if the write fails for any
reason the message is dropped instead of being reattempted.`,
		Description: `
## Alternatives

This output has been replaced with the more explicit and configurable ` + "[`drop_on`](/docs/components/outputs/drop_on)" + ` output.

This output can be combined with a child [` + "`retry`" + `](/docs/components/outputs/retry)
output in order to set an explicit number of retry attempts before dropping a
message.

For example, the following configuration attempts to send to a hypothetical
output type ` + "`foo`" + ` three times, but if all three attempts fail the
message is dropped entirely:

` + "```yaml" + `
output:
  drop_on_error:
    retry:
      max_retries: 2
      output:
        type: foo
` + "```" + ``,
		config: docs.FieldComponent().Map(),
	}
}

//------------------------------------------------------------------------------

// DropOnErrorConfig contains configuration values for the DropOnError output
// type.
type DropOnErrorConfig struct {
	*Config `yaml:",inline" json:",inline"`
}

// NewDropOnErrorConfig creates a new DropOnErrorConfig with default values.
func NewDropOnErrorConfig() DropOnErrorConfig {
	return DropOnErrorConfig{
		Config: nil,
	}
}

//------------------------------------------------------------------------------

// MarshalJSON prints an empty object instead of nil.
func (d DropOnErrorConfig) MarshalJSON() ([]byte, error) {
	if d.Config != nil {
		return json.Marshal(d.Config)
	}
	return json.Marshal(struct{}{})
}

// MarshalYAML prints an empty object instead of nil.
func (d DropOnErrorConfig) MarshalYAML() (interface{}, error) {
	if d.Config != nil {
		return *d.Config, nil
	}
	return struct{}{}, nil
}

//------------------------------------------------------------------------------

// UnmarshalJSON ensures that when parsing child config it is initialised.
func (d *DropOnErrorConfig) UnmarshalJSON(bytes []byte) error {
	if d.Config == nil {
		nConf := NewConfig()
		d.Config = &nConf
	}

	return json.Unmarshal(bytes, d.Config)
}

// UnmarshalYAML ensures that when parsing child config it is initialised.
func (d *DropOnErrorConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if d.Config == nil {
		nConf := NewConfig()
		d.Config = &nConf
	}

	return unmarshal(d.Config)
}

//------------------------------------------------------------------------------

// DropOnError is an output type that continuously writes a message to a child output
// until the send is successful.
type DropOnError struct {
	running int32

	wrapped Type

	stats metrics.Type
	log   log.Modular

	transactionsIn  <-chan types.Transaction
	transactionsOut chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewDropOnError creates a new DropOnError input type.
func NewDropOnError(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if conf.DropOnError.Config == nil {
		return nil, errors.New("cannot create a drop_on_error output without a child")
	}

	wrapped, err := New(*conf.DropOnError.Config, mgr, log, stats)
	if err != nil {
		return nil, fmt.Errorf("failed to create output '%v': %v", conf.DropOnError.Config.Type, err)
	}

	return &DropOnError{
		running: 1,

		log:             log,
		stats:           stats,
		wrapped:         wrapped,
		transactionsOut: make(chan types.Transaction),

		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}, nil
}

//------------------------------------------------------------------------------

func (d *DropOnError) loop() {
	// Metrics paths
	var (
		mDropped      = d.stats.GetCounter("drop_on_error.dropped")
		mDroppedBatch = d.stats.GetCounter("drop_on_error.batch.dropped")
	)

	defer func() {
		close(d.transactionsOut)
		d.wrapped.CloseAsync()
		err := d.wrapped.WaitForClose(time.Second)
		for ; err != nil; err = d.wrapped.WaitForClose(time.Second) {
		}
		close(d.closedChan)
	}()

	resChan := make(chan types.Response)

	for atomic.LoadInt32(&d.running) == 1 {
		var ts types.Transaction
		var open bool
		select {
		case ts, open = <-d.transactionsIn:
			if !open {
				return
			}
		case <-d.closeChan:
			return
		}

		select {
		case d.transactionsOut <- types.NewTransaction(ts.Payload, resChan):
		case <-d.closeChan:
			return
		}

		var res types.Response
		select {
		case res = <-resChan:
		case <-d.closeChan:
			return
		}

		if res.Error() != nil {
			mDropped.Incr(int64(ts.Payload.Len()))
			mDroppedBatch.Incr(1)
			d.log.Warnf("Message dropped due to: %v\n", res.Error())
		}

		select {
		case ts.ResponseChan <- response.NewAck():
		case <-d.closeChan:
			return
		}
	}
}

// Consume assigns a messages channel for the output to read.
func (d *DropOnError) Consume(ts <-chan types.Transaction) error {
	if d.transactionsIn != nil {
		return types.ErrAlreadyStarted
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
func (d *DropOnError) Connected() bool {
	return d.wrapped.Connected()
}

// MaxInFlight returns the maximum number of in flight messages permitted by the
// output. This value can be used to determine a sensible value for parent
// outputs, but should not be relied upon as part of dispatcher logic.
func (d *DropOnError) MaxInFlight() (int, bool) {
	return output.GetMaxInFlight(d.wrapped)
}

// CloseAsync shuts down the DropOnError input and stops processing requests.
func (d *DropOnError) CloseAsync() {
	if atomic.CompareAndSwapInt32(&d.running, 1, 0) {
		close(d.closeChan)
	}
}

// WaitForClose blocks until the DropOnError input has closed down.
func (d *DropOnError) WaitForClose(timeout time.Duration) error {
	select {
	case <-d.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
