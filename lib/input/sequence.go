package input

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSequence] = TypeSpec{
		constructor: NewSequence,
		Status:      docs.StatusBeta,
		Summary: `
Reads messages from a sequence of child inputs, starting with the first and once
that input gracefully terminates starts consuming from the next, and so on.`,
		Description: `
This input is useful for consuming from inputs that have an explicit end but
must not be consumed in parallel.`,
		Footnotes: `
## Examples

A common use case might be to generate a message at the end of our main input:

` + "```yaml" + `
input:
  sequence:
    inputs:
      - csv:
          paths: [ ./dataset.csv ]
      - bloblang:
          count: 1
          mapping: 'root = {"status":"finished"}'
` + "```" + `

With this config once the records within ` + "`./dataset.csv`" + ` are exhausted
our final payload ` + "`" + `{"status":"finished"}` + "`" + ` will be routed
through the pipeline.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			inputsSanit := make([]interface{}, 0, len(conf.Sequence.Inputs))
			for _, in := range conf.Sequence.Inputs {
				sanit, err := SanitiseConfig(in)
				if err != nil {
					return nil, err
				}
				inputsSanit = append(inputsSanit, sanit)
			}
			return map[string]interface{}{
				"inputs": inputsSanit,
			}, nil
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("inputs", "An array of inputs to read from sequentially."),
		},
		Categories: []Category{
			CategoryUtility,
		},
	}
}

//------------------------------------------------------------------------------

// SequenceConfig contains configuration values for the Sequence input type.
type SequenceConfig struct {
	Inputs []Config `json:"inputs" yaml:"inputs"`
}

// NewSequenceConfig creates a new SequenceConfig with default values.
func NewSequenceConfig() SequenceConfig {
	return SequenceConfig{
		Inputs: []Config{},
	}
}

//------------------------------------------------------------------------------

// Sequence is an input type that reads from a sequence of inputs, starting with
// the first, and when it ends gracefully it moves onto the next, and so on.
type Sequence struct {
	running int32
	conf    SequenceConfig

	targetMut sync.Mutex
	target    Type
	remaining []sequenceTarget

	wrapperMgr   types.Manager
	wrapperLog   log.Modular
	wrapperStats metrics.Type

	stats metrics.Type
	log   log.Modular

	transactions chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

type sequenceTarget struct {
	index  int
	config Config
}

// NewSequence creates a new Sequence input type.
func NewSequence(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if len(conf.Sequence.Inputs) == 0 {
		return nil, errors.New("requires at least one child input")
	}

	targets := make([]sequenceTarget, 0, len(conf.Sequence.Inputs))
	for i, c := range conf.Sequence.Inputs {
		targets = append(targets, sequenceTarget{
			index:  i,
			config: c,
		})
	}

	rdr := &Sequence{
		running: 1,
		conf:    conf.Sequence,

		remaining: targets,

		wrapperLog:   log,
		wrapperStats: stats,
		wrapperMgr:   mgr,

		log:          log.NewModule(".sequence"),
		stats:        metrics.Namespaced(stats, "sequence"),
		transactions: make(chan types.Transaction),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	if target, err := rdr.createNextTarget(); err != nil {
		return nil, err
	} else if target == nil {
		return nil, errors.New("failed to initialize first input")
	}

	go rdr.loop()
	return rdr, nil
}

//------------------------------------------------------------------------------

func (r *Sequence) getTarget() Type {
	r.targetMut.Lock()
	target := r.target
	r.targetMut.Unlock()
	return target
}

func (r *Sequence) createNextTarget() (Type, error) {
	var target Type
	var err error

	r.targetMut.Lock()
	r.target = nil
	if len(r.remaining) > 0 {
		if target, err = New(
			r.remaining[0].config,
			r.wrapperMgr,
			r.wrapperLog,
			r.wrapperStats,
		); err == nil {
			r.remaining = r.remaining[1:]
		} else {
			err = fmt.Errorf("failed to initialize input index %v: %w", r.remaining[0].index, err)
		}
	}
	if target != nil {
		r.target = target
	}
	r.targetMut.Unlock()

	return target, err
}

func (r *Sequence) loop() {
	defer func() {
		if t := r.getTarget(); t != nil {
			t.CloseAsync()
			err := t.WaitForClose(time.Second)
			for ; err != nil; err = t.WaitForClose(time.Second) {
			}
		}
		close(r.transactions)
		close(r.closedChan)
	}()

	target := r.getTarget()

runLoop:
	for atomic.LoadInt32(&r.running) == 1 {
		if target == nil {
			var err error
			if target, err = r.createNextTarget(); err != nil {
				r.log.Errorf("Unable to start next sequence: %v\n", err)
				select {
				case <-time.After(time.Second):
				case <-r.closeChan:
					return
				}
				continue runLoop
			} else if target == nil {
				r.log.Infoln("Exhausted all sequence inputs, shutting down.")
				return
			}
		}

		var tran types.Transaction
		var open bool
		select {
		case tran, open = <-target.TransactionChan():
			if !open {
				target.CloseAsync() // For good measure.
				target = nil
				continue runLoop
			}
		case <-r.closeChan:
			return
		}

		select {
		case r.transactions <- tran:
		case <-r.closeChan:
			return
		}
	}
}

// TransactionChan returns a transactions channel for consuming messages from
// this input type.
func (r *Sequence) TransactionChan() <-chan types.Transaction {
	return r.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (r *Sequence) Connected() bool {
	if t := r.getTarget(); t != nil {
		return t.Connected()
	}
	return false
}

// CloseAsync shuts down the Sequence input and stops processing requests.
func (r *Sequence) CloseAsync() {
	if atomic.CompareAndSwapInt32(&r.running, 1, 0) {
		close(r.closeChan)
	}
}

// WaitForClose blocks until the Sequence input has closed down.
func (r *Sequence) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
