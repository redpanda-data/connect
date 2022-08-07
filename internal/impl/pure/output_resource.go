package pure

import (
	"context"
	"fmt"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		if !nm.ProbeOutput(c.Resource) {
			return nil, fmt.Errorf("output resource '%v' was not found", c.Resource)
		}
		return &resourceOutput{
			mgr:     nm,
			name:    c.Resource,
			log:     nm.Logger(),
			shutSig: shutdown.NewSignaller(),
		}, nil
	}), docs.ComponentSpec{
		Name:    "resource",
		Summary: `Resource is an output type that channels messages to a resource output, identified by its name.`,
		Description: `Resources allow you to tidy up deeply nested configs. For example, the config:

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
	mgr  bundle.NewManagement
	name string
	log  log.Modular

	transactions <-chan message.Transaction

	shutSig *shutdown.Signaller
}

func (r *resourceOutput) loop() {
	cnCtx, cnDone := r.shutSig.CloseNowCtx(context.Background())
	defer cnDone()

	defer func() {
		r.shutSig.ShutdownComplete()
	}()

	var ts *message.Transaction
	for {
		if ts == nil {
			select {
			case t, open := <-r.transactions:
				if !open {
					return
				}
				ts = &t
			case <-r.shutSig.CloseNowChan():
				return
			}
		}

		var err error
		if oerr := r.mgr.AccessOutput(cnCtx, r.name, func(o output.Sync) {
			err = o.WriteTransaction(cnCtx, *ts)
		}); oerr != nil {
			err = oerr
		}
		if err != nil {
			r.log.Errorf("Failed to obtain output resource '%v': %v", r.name, err)
			select {
			case <-time.After(time.Second):
			case <-r.shutSig.CloseNowChan():
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

func (r *resourceOutput) TriggerCloseNow() {
	r.shutSig.CloseNow()
}

func (r *resourceOutput) WaitForClose(ctx context.Context) error {
	select {
	case <-r.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
