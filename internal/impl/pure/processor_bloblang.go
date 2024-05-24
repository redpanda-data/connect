package pure

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/parser"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterBatchProcessor("bloblang", service.NewConfigSpec().
		Stable().
		Categories("Mapping", "Parsing").
		Summary("Executes a xref:guides:bloblang/about.adoc[Bloblang] mapping on messages.").
		Description(`
Bloblang is a powerful language that enables a wide range of mapping, transformation and filtering tasks. For more information see xref:guides:bloblang/about.adoc[].

If your mapping is large and you'd prefer for it to live in a separate file then you can execute a mapping directly from a file with the expression `+"`from \"<path>\"`"+`, where the path must be absolute, or relative from the location that Benthos is executed from.

== Component rename

This processor was recently renamed to the `+"xref:components:processors/mapping.adoc[`mapping` processor]"+` in order to make the purpose of the processor more prominent. It is still valid to use the existing `+"`bloblang`"+` name but eventually it will be deprecated and replaced by the new name in example configs.`).
		Footnotes(`
== Error handling

Bloblang mappings can fail, in which case the message remains unchanged, errors are logged, and the message is flagged as having failed, allowing you to use
xref:configuration:error_handling.adoc[standard processor error handling patterns].

However, Bloblang itself also provides powerful ways of ensuring your mappings do not fail by specifying desired fallback behavior, which you can read about in xref:guides:bloblang/about#error-handling.adoc[Error handling].`).
		Example("Mapping", `
Given JSON documents containing an array of fans:

`+"```json"+`
{
  "id":"foo",
  "description":"a show about foo",
  "fans":[
    {"name":"bev","obsession":0.57},
    {"name":"grace","obsession":0.21},
    {"name":"ali","obsession":0.89},
    {"name":"vic","obsession":0.43}
  ]
}
`+"```"+`

We can reduce the fans to only those with an obsession score above 0.5, giving us:

`+"```json"+`
{
  "id":"foo",
  "description":"a show about foo",
  "fans":[
    {"name":"bev","obsession":0.57},
    {"name":"ali","obsession":0.89}
  ]
}
`+"```"+`

With the following config:`,
			`
pipeline:
  processors:
  - bloblang: |
      root = this
      root.fans = this.fans.filter(fan -> fan.obsession > 0.5)
`,
		).
		Example("More Mapping", `
When receiving JSON documents of the form:

`+"```json"+`
{
  "locations": [
    {"name": "Seattle", "state": "WA"},
    {"name": "New York", "state": "NY"},
    {"name": "Bellevue", "state": "WA"},
    {"name": "Olympia", "state": "WA"}
  ]
}
`+"```"+`

We could collapse the location names from the state of Washington into a field `+"`Cities`"+`:

`+"```json"+`
{"Cities": "Bellevue, Olympia, Seattle"}
`+"```"+`

With the following config:`,
			`
pipeline:
  processors:
    - bloblang: |
        root.Cities = this.locations.
                        filter(loc -> loc.state == "WA").
                        map_each(loc -> loc.name).
                        sort().join(", ")
`).
		Field(service.NewBloblangField("").Default("")),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			m, err := conf.FieldString()
			if err != nil {
				return nil, err
			}
			mgr := interop.UnwrapManagement(res)
			p, err := newBloblang(m, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(
				processor.NewAutoObservedBatchedProcessor("bloblang", p, mgr),
			), nil
		})
	if err != nil {
		panic(err)
	}
}

type bloblangProc struct {
	exec *mapping.Executor
	log  log.Modular
}

func newBloblang(conf string, mgr bundle.NewManagement) (processor.AutoObservedBatched, error) {
	exec, err := mgr.BloblEnvironment().NewMapping(conf)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			return nil, fmt.Errorf("%v", perr.ErrorAtPosition([]rune(conf)))
		}
		return nil, err
	}
	return &bloblangProc{
		exec: exec,
		log:  mgr.Logger(),
	}, nil
}

func (b *bloblangProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	newParts := make([]*message.Part, 0, msg.Len())
	_ = msg.Iter(func(i int, part *message.Part) error {
		p, err := b.exec.MapPart(i, msg)
		if err != nil {
			ctx.OnError(err, i, part)
			b.log.Error("%v", err)
			p = part
		}
		if p != nil {
			newParts = append(newParts, p)
		}
		return nil
	})
	if len(newParts) == 0 {
		return nil, nil
	}
	return []message.Batch{newParts}, nil
}

func (b *bloblangProc) Close(context.Context) error {
	return nil
}
