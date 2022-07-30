package pure

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/parser"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newBloblang(conf.Bloblang, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("bloblang", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "bloblang",
		Categories: []string{
			"Mapping",
			"Parsing",
		},
		Config: docs.FieldString("", "").IsBloblang().HasDefault(""),
		Summary: `
Executes a [Bloblang](/docs/guides/bloblang/about) mapping on messages.`,
		Description: `
Bloblang is a powerful language that enables a wide range of mapping, transformation and filtering tasks. For more information [check out the docs](/docs/guides/bloblang/about).

If your mapping is large and you'd prefer for it to live in a separate file then you can execute a mapping directly from a file with the expression ` + "`from \"<path>\"`" + `, where the path must be absolute, or relative from the location that Benthos is executed from.

## Component Rename

This processor was recently renamed to the ` + "[`mapping` processor](/docs/components/processors/mapping)" + ` in order to make the purpose of the processor more prominent. It is still valid to use the existing ` + "`bloblang`" + ` name but eventually it will be deprecated and replaced by the new name in example configs.`,
		Footnotes: `
## Error Handling

Bloblang mappings can fail, in which case the message remains unchanged, errors
are logged, and the message is flagged as having failed, allowing you to use
[standard processor error handling patterns](/docs/configuration/error_handling).

However, Bloblang itself also provides powerful ways of ensuring your mappings
do not fail by specifying desired fallback behaviour, which you can read about
[in this section](/docs/guides/bloblang/about#error-handling).`,
		Examples: []docs.AnnotatedExample{
			{
				Title: "Mapping",
				Summary: `
Given JSON documents containing an array of fans:

` + "```json" + `
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
` + "```" + `

We can reduce the fans to only those with an obsession score above 0.5, giving us:

` + "```json" + `
{
  "id":"foo",
  "description":"a show about foo",
  "fans":[
    {"name":"bev","obsession":0.57},
    {"name":"ali","obsession":0.89}
  ]
}
` + "```" + `

With the following config:`,
				Config: `
pipeline:
  processors:
  - bloblang: |
      root = this
      root.fans = this.fans.filter(fan -> fan.obsession > 0.5)
`,
			},
			{
				Title: "More Mapping",
				Summary: `
When receiving JSON documents of the form:

` + "```json" + `
{
  "locations": [
    {"name": "Seattle", "state": "WA"},
    {"name": "New York", "state": "NY"},
    {"name": "Bellevue", "state": "WA"},
    {"name": "Olympia", "state": "WA"}
  ]
}
` + "```" + `

We could collapse the location names from the state of Washington into a field ` + "`Cities`" + `:

` + "```json" + `
{"Cities": "Bellevue, Olympia, Seattle"}
` + "```" + `

With the following config:`,
				Config: `
pipeline:
  processors:
    - bloblang: |
        root.Cities = this.locations.
                        filter(loc -> loc.state == "WA").
                        map_each(loc -> loc.name).
                        sort().join(", ")
`,
			},
		},
	})
	if err != nil {
		panic(err)
	}
}

type bloblangProc struct {
	exec *mapping.Executor
	log  log.Modular
}

func newBloblang(conf string, mgr bundle.NewManagement) (processor.V2Batched, error) {
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

func (b *bloblangProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	newParts := make([]*message.Part, 0, msg.Len())
	_ = msg.Iter(func(i int, part *message.Part) error {
		p, err := b.exec.MapPart(i, msg)
		if err != nil {
			p = part
			b.log.Errorf("%v\n", err)
			processor.MarkErr(p, spans[i], err)
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
