package pure

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(c input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		if !nm.ProbeInput(c.Resource) {
			return nil, fmt.Errorf("input resource '%v' was not found", c.Resource)
		}
		return &resourceInput{
			mgr:  nm,
			name: c.Resource,
			log:  nm.Logger(),
		}, nil
	}), docs.ComponentSpec{
		Name:    "resource",
		Summary: `Resource is an input type that channels messages from a resource input, identified by its name.`,
		Description: `Resources allow you to tidy up deeply nested configs. For example, the config:
		
` + "```yaml" + `
input:
  broker:
    inputs:
      - kafka:
          addresses: [ TODO ]
          topics: [ foo ]
          consumer_group: foogroup
      - gcp_pubsub:
          project: bar
          subscription: baz
` + "```" + `

Could also be expressed as:

` + "```yaml" + `
input:
  broker:
    inputs:
      - resource: foo
      - resource: bar

input_resources:
  - label: foo
    kafka:
      addresses: [ TODO ]
      topics: [ foo ]
      consumer_group: foogroup

  - label: bar
    gcp_pubsub:
      project: bar
      subscription: baz
 ` + "```" + `

Resources also allow you to reference a single input in multiple places, such as multiple streams mode configs, or multiple entries in a broker input. However, when a resource is referenced more than once the messages it produces are distributed across those references, so each message will only be directed to a single reference, not all of them.

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

//------------------------------------------------------------------------------

type resourceInput struct {
	mgr  bundle.NewManagement
	name string
	log  log.Modular
}

func (r *resourceInput) TransactionChan() (tChan <-chan message.Transaction) {
	if err := r.mgr.AccessInput(context.Background(), r.name, func(i input.Streamed) {
		tChan = i.TransactionChan()
	}); err != nil {
		r.log.Errorf("Failed to obtain input resource '%v': %v", r.name, err)
	}
	return
}

func (r *resourceInput) Connected() (isConnected bool) {
	if err := r.mgr.AccessInput(context.Background(), r.name, func(i input.Streamed) {
		isConnected = i.Connected()
	}); err != nil {
		r.log.Errorf("Failed to obtain input resource '%v': %v", r.name, err)
	}
	return
}

func (r *resourceInput) TriggerStopConsuming() {
}

func (r *resourceInput) TriggerCloseNow() {
}

func (r *resourceInput) WaitForClose(ctx context.Context) error {
	return nil
}
