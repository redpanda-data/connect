package pure

import (
	"context"
	"fmt"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	oinput "github.com/benthosdev/benthos/v4/internal/old/input"
)

func init() {
	err := bundle.AllInputs.Add(bundle.InputConstructorFromSimple(func(c oinput.Config, nm bundle.NewManagement) (input.Streamed, error) {
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
		Summary: `Resource is an input type that runs a resource input by its name.`,
		Description: `This input allows you to reference the same configured input resource in multiple places, and can also tidy up large nested configs. For
example, the config:

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
	mgr  interop.Manager
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

func (r *resourceInput) CloseAsync() {
}

func (r *resourceInput) WaitForClose(timeout time.Duration) error {
	return nil
}
