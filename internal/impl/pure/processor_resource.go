package pure

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		return newResourceProcessor(conf, mgr, mgr.Logger())
	}, docs.ComponentSpec{
		Name: "resource",
		Categories: []string{
			"Utility",
		},
		Summary: `
Resource is a processor type that runs a processor resource identified by its label.`,
		Description: `
This processor allows you to reference the same configured processor resource in multiple places, and can also tidy up large nested configs. For example, the config:

` + "```yaml" + `
pipeline:
  processors:
    - mapping: |
        root.message = this
        root.meta.link_count = this.links.length()
        root.user.age = this.user.age.number()
` + "```" + `

Is equivalent to:

` + "```yaml" + `
pipeline:
  processors:
    - resource: foo_proc

processor_resources:
  - label: foo_proc
    mapping: |
      root.message = this
      root.meta.link_count = this.links.length()
      root.user.age = this.user.age.number()
` + "```" + `

You can find out more about resources [in this document.](/docs/configuration/resources)`,
		Config: docs.FieldString("", "").HasDefault(""),
	})
	if err != nil {
		panic(err)
	}
}

type resourceProcessor struct {
	mgr  bundle.NewManagement
	name string
	log  log.Modular
}

func newResourceProcessor(conf processor.Config, mgr bundle.NewManagement, log log.Modular) (*resourceProcessor, error) {
	if !mgr.ProbeProcessor(conf.Resource) {
		return nil, fmt.Errorf("processor resource '%v' was not found", conf.Resource)
	}
	return &resourceProcessor{
		mgr:  mgr,
		name: conf.Resource,
		log:  log,
	}, nil
}

func (r *resourceProcessor) ProcessBatch(ctx context.Context, msg message.Batch) (msgs []message.Batch, res error) {
	if err := r.mgr.AccessProcessor(ctx, r.name, func(p processor.V1) {
		msgs, res = p.ProcessBatch(ctx, msg)
	}); err != nil {
		r.log.Errorf("Failed to obtain processor resource '%v': %v", r.name, err)
		return nil, err
	}
	return msgs, res
}

func (r *resourceProcessor) Close(ctx context.Context) error {
	return nil
}
