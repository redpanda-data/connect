package output

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func init() {
	Constructors[TypeResource] = TypeSpec{
		constructor: fromSimpleConstructor(NewResource),
		Summary: `
Resource is an output type that runs a resource output by its name.`,
		Description: `
This output allows you to reference the same configured output resource in multiple places, and can also tidy up large nested configs. For example, the config:

` + "```yaml" + `
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

Could also be expressed as:

` + "``` yaml" + `
output:
  broker:
    pattern: fan_out
    outputs:
    - resource: foo
    - resource: bar

output_resources:
  - label: foo
    kafka:
      addresses: [ TODO ]
      topic: foo

  - label: bar
    gcp_pubsub:
      project: bar
      topic: baz
 ` + "```" + `

You can find out more about resources [in this document.](/docs/configuration/resources)`,
		Categories: []Category{
			CategoryUtility,
		},
		config: docs.FieldComponent().HasType(docs.FieldTypeString).HasDefault(""),
	}
}

//------------------------------------------------------------------------------

// Resource is a processor that returns the result of a output resource.
type Resource struct {
	mgr   types.Manager
	name  string
	log   log.Modular
	stats metrics.Type

	transactions <-chan message.Transaction

	ctx  context.Context
	done func()

	mErrNotFound metrics.StatCounter
}

// NewResource returns a resource output.
func NewResource(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (output.Streamed, error) {
	if err := interop.ProbeOutput(context.Background(), mgr, conf.Resource); err != nil {
		return nil, err
	}
	ctx, done := context.WithCancel(context.Background())
	return &Resource{
		mgr:          mgr,
		name:         conf.Resource,
		log:          log,
		stats:        stats,
		ctx:          ctx,
		done:         done,
		mErrNotFound: stats.GetCounter("error_not_found"),
	}, nil
}

//------------------------------------------------------------------------------

func (r *Resource) loop() {
	// Metrics paths
	var (
		mCount = r.stats.GetCounter("count")
	)

	var ts *message.Transaction
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

		var err error
		if oerr := interop.AccessOutput(context.Background(), r.mgr, r.name, func(o output.Sync) {
			err = o.WriteTransaction(r.ctx, *ts)
		}); oerr != nil {
			err = oerr
		}
		if err != nil {
			r.log.Debugf("Failed to obtain output resource '%v': %v", r.name, err)
			r.mErrNotFound.Incr(1)
			select {
			case <-time.After(time.Second):
			case <-r.ctx.Done():
				return
			}
		} else {
			ts = nil
		}
	}
}

//------------------------------------------------------------------------------

// Consume assigns a messages channel for the output to read.
func (r *Resource) Consume(ts <-chan message.Transaction) error {
	if r.transactions != nil {
		return component.ErrAlreadyStarted
	}
	r.transactions = ts
	go r.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (r *Resource) Connected() (isConnected bool) {
	var err error
	if err = interop.AccessOutput(context.Background(), r.mgr, r.name, func(o output.Sync) {
		isConnected = o.Connected()
	}); err != nil {
		r.log.Debugf("Failed to obtain output resource '%v': %v", r.name, err)
		r.mErrNotFound.Incr(1)
	}
	return
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
		return component.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
