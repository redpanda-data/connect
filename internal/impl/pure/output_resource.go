package pure

import (
	"context"
	"fmt"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	ooutput "github.com/benthosdev/benthos/v4/internal/old/output"
)

func init() {
	err := bundle.AllOutputs.Add(bundle.OutputConstructorFromSimple(func(c ooutput.Config, nm bundle.NewManagement) (output.Streamed, error) {
		if !nm.ProbeOutput(c.Resource) {
			return nil, fmt.Errorf("output resource '%v' was not found", c.Resource)
		}
		ctx, done := context.WithCancel(context.Background())
		return &resourceOutput{
			mgr:  nm,
			name: c.Resource,
			log:  nm.Logger(),
			ctx:  ctx,
			done: done,
		}, nil
	}), docs.ComponentSpec{
		Name: "resource",
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

` + "```yaml" + `
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
		Categories: []string{
			"Utility",
		},
		Config: docs.FieldString("", "").HasDefault(""),
	})
	if err != nil {
		panic(err)
	}
}

type resourceOutput struct {
	mgr  interop.Manager
	name string
	log  log.Modular

	transactions <-chan message.Transaction

	ctx  context.Context
	done func()
}

func (r *resourceOutput) loop() {
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

		var err error
		if oerr := r.mgr.AccessOutput(context.Background(), r.name, func(o output.Sync) {
			err = o.WriteTransaction(r.ctx, *ts)
		}); oerr != nil {
			err = oerr
		}
		if err != nil {
			r.log.Errorf("Failed to obtain output resource '%v': %v", r.name, err)
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

func (r *resourceOutput) Consume(ts <-chan message.Transaction) error {
	if r.transactions != nil {
		return component.ErrAlreadyStarted
	}
	r.transactions = ts
	go r.loop()
	return nil
}

func (r *resourceOutput) Connected() (isConnected bool) {
	var err error
	if err = r.mgr.AccessOutput(context.Background(), r.name, func(o output.Sync) {
		isConnected = o.Connected()
	}); err != nil {
		r.log.Errorf("Failed to obtain output resource '%v': %v", r.name, err)
	}
	return
}

func (r *resourceOutput) CloseAsync() {
	r.done()
}

func (r *resourceOutput) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.ctx.Done():
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
