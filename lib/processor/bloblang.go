package processor

import (
	"context"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBloblang] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newBloblang(conf.Bloblang, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("bloblang", p, mgr.Metrics()), nil
		},
		Categories: []Category{
			CategoryMapping,
			CategoryParsing,
		},
		config: docs.FieldComponent().HasType(docs.FieldTypeString).IsBloblang().HasDefault(""),
		Summary: `
Executes a [Bloblang](/docs/guides/bloblang/about) mapping on messages.`,
		Description: `
Bloblang is a powerful language that enables a wide range of mapping, transformation and filtering tasks. For more information [check out the docs](/docs/guides/bloblang/about).

If your mapping is large and you'd prefer for it to live in a separate file then you can execute a mapping directly from a file with the expression ` + "`from \"<path>\"`" + `, where the path must be absolute, or relative from the location that Benthos is executed from.`,
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
	}
}

//------------------------------------------------------------------------------

// BloblangConfig contains configuration fields for the Bloblang processor.
type BloblangConfig string

// NewBloblangConfig returns a BloblangConfig with default values.
func NewBloblangConfig() BloblangConfig {
	return ""
}

//------------------------------------------------------------------------------

type bloblangProc struct {
	exec *mapping.Executor
	log  log.Modular
}

func newBloblang(conf BloblangConfig, mgr interop.Manager) (processor.V2Batched, error) {
	exec, err := mgr.BloblEnvironment().NewMapping(string(conf))
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			return nil, fmt.Errorf("%v", perr.ErrorAtPosition([]rune(conf)))
		}
		return nil, err
	}
	return NewBloblangFromExecutor(exec, mgr.Logger()), nil
}

// NewBloblangFromExecutor returns a new bloblang processor from an executor.
func NewBloblangFromExecutor(exec *mapping.Executor, log log.Modular) processor.V2Batched {
	return &bloblangProc{
		exec: exec,
		log:  log,
	}
}

//------------------------------------------------------------------------------

func (b *bloblangProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg *message.Batch) ([]*message.Batch, error) {
	newParts := make([]*message.Part, 0, msg.Len())
	_ = msg.Iter(func(i int, part *message.Part) error {
		p, err := b.exec.MapPart(i, msg)
		if err != nil {
			p = part.Copy()
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

	newMsg := message.QuickBatch(nil)
	newMsg.SetAll(newParts)
	return []*message.Batch{newMsg}, nil
}

func (b *bloblangProc) Close(context.Context) error {
	return nil
}
