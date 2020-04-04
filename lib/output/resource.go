package output

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeResource] = TypeSpec{
		constructor: NewResource,
		Summary: `
Resource is an output type that runs a resource output by its name. This output
allows you to run the same configured output resource in multiple places.`,
		Description: `
Resource outputs also have the advantage of name based metrics and logging. For
example, the config:

` + "``` yaml" + `
output:
  broker:
    pattern: fan_out
    outputs:
    - kafka:
        addresses: [ TODO ]
        topic: foo
    - gcp_pubsub:
        project: bar
        topic: baz
` + "```" + `

Is equivalent to:

` + "``` yaml" + `
output:
  broker:
    pattern: fan_out
    outputs:
    - resource: foo
    - resource: bar

resources:
  outputs:
    foo:
      kafka_balanced:
        addresses: [ TODO ]
        topic: foo

    bar:
      gcp_pubsub:
        project: bar
        topic: baz
 ` + "```" + `

But now the metrics path of Kafka output will be
` + "`resources.outputs.foo`" + `, this way of flattening observability
labels becomes more useful as configs get larger and more nested.`,
	}
}

//------------------------------------------------------------------------------

type outputProvider interface {
	GetOutput(name string) (types.OutputWriter, error)
}

//------------------------------------------------------------------------------

// Resource is a processor that returns the result of a output resource.
type Resource struct {
	mgr   outputProvider
	name  string
	log   log.Modular
	stats metrics.Type

	transactions <-chan types.Transaction

	ctx  context.Context
	done func()
}

// NewResource returns a resource output.
func NewResource(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	// TODO: V4 Remove this
	outputProvider, ok := mgr.(outputProvider)
	if !ok {
		return nil, errors.New("manager does not support output resources")
	}

	if _, err := outputProvider.GetOutput(conf.Resource); err != nil {
		return nil, fmt.Errorf("failed to obtain output resource '%v': %v", conf.Resource, err)
	}

	ctx, done := context.WithCancel(context.Background())
	return &Resource{
		mgr:  outputProvider,
		name: conf.Resource,
		log:  log,
		ctx:  ctx,
		done: done,
	}, nil
}

//------------------------------------------------------------------------------

func (r *Resource) loop() {
	// Metrics paths
	var (
		mCount       = r.stats.GetCounter("count")
		mErrNotFound = r.stats.GetCounter("error_not_found")
	)

	var ts *types.Transaction
	for {
		if ts == nil {
			select {
			case t, open := <-r.transactions:
				if !open {
					r.done()
					return
				}
				ts = &t
			case <-r.ctx.Done():
				return
			}
		}
		mCount.Incr(1)
		out, err := r.mgr.GetOutput(r.name)
		if err != nil {
			r.log.Debugf("Failed to obtain output resource '%v': %v", r.name, err)
			mErrNotFound.Incr(1)
			select {
			case <-time.After(time.Second):
			case <-r.ctx.Done():
				return
			}
		} else {
			out.WriteTransaction(r.ctx, *ts)
			ts = nil
		}
	}
}

//------------------------------------------------------------------------------

// Consume assigns a messages channel for the output to read.
func (r *Resource) Consume(ts <-chan types.Transaction) error {
	if r.transactions != nil {
		return types.ErrAlreadyStarted
	}
	r.transactions = ts
	go r.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (r *Resource) Connected() bool {
	out, err := r.mgr.GetOutput(r.name)
	if err != nil {
		r.log.Debugf("Failed to obtain output resource '%v': %v", r.name, err)
		r.mErrNotFound.Incr(1)
		return false
	}
	return out.Connected()
}

// CloseAsync shuts down the output and stops processing requests.
func (r *Resource) CloseAsync() {
	r.done()
}

// WaitForClose blocks until the output has closed down.
func (r *Resource) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.ctx.Done():
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
