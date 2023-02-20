package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterBatchProcessor(
		"mapping",
		service.NewConfigSpec().
			Stable().
			Version("4.5.0").
			Categories("Mapping", "Parsing").
			Field(service.NewBloblangField("")).
			Summary("Executes a [Bloblang](/docs/guides/bloblang/about) mapping on messages, creating a new document that replaces (or filters) the original message.").
			Description(`
Bloblang is a powerful language that enables a wide range of mapping, transformation and filtering tasks. For more information [check out the docs](/docs/guides/bloblang/about).

If your mapping is large and you'd prefer for it to live in a separate file then you can execute a mapping directly from a file with the expression `+"`from \"<path>\"`"+`, where the path must be absolute, or relative from the location that Benthos is executed from.

Note: This processor is equivalent to the [bloblang](/docs/components/processors/bloblang#component-rename) one. The latter will be deprecated in a future release.

## Input Document Immutability

Mapping operates by creating an entirely new object during assignments, this has the advantage of treating the original referenced document as immutable and therefore queryable at any stage of your mapping. For example, with the following mapping:

`+"```coffee"+`
root.id = this.id
root.invitees = this.invitees.filter(i -> i.mood >= 0.5)
root.rejected = this.invitees.filter(i -> i.mood < 0.5)
`+"```"+`

Notice that we mutate the value of `+"`invitees`"+` in the resulting document by filtering out objects with a lower mood. However, even after doing so we're still able to reference the unchanged original contents of this value from the input document in order to populate a second field. Within this mapping we also have the flexibility to reference the mutable mapped document by using the keyword `+"`root` (i.e. `root.invitees`)"+` on the right-hand side instead.

Mapping documents is advantageous in situations where the result is a document with a dramatically different shape to the input document, since we are effectively rebuilding the document in its entirety and might as well keep a reference to the unchanged input document throughout. However, in situations where we are only performing minor alterations to the input document, the rest of which is unchanged, it might be more efficient to use the `+"[`mutation` processor](/docs/components/processors/mutation)"+` instead.

## Error Handling

Bloblang mappings can fail, in which case the message remains unchanged, errors are logged, and the message is flagged as having failed, allowing you to use [standard processor error handling patterns](/docs/configuration/error_handling).

However, Bloblang itself also provides powerful ways of ensuring your mappings do not fail by specifying desired fallback behaviour, which you can read about [in this section](/docs/guides/bloblang/about#error-handling).
			`).
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

We can reduce the documents down to just the ID and only those fans with an obsession score above 0.5, giving us:

`+"```json"+`
{
  "id":"foo",
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
    - mapping: |
        root.id = this.id
        root.fans = this.fans.filter(fan -> fan.obsession > 0.5)
`).
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
    - mapping: |
        root.Cities = this.locations.
                        filter(loc -> loc.state == "WA").
                        map_each(loc -> loc.name).
                        sort().join(", ")
`),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			mapping, err := conf.FieldBloblang()
			if err != nil {
				return nil, err
			}

			v1Proc := processor.NewV2BatchedToV1Processor("mapping", newMapping(mapping, mgr.Logger()), interop.UnwrapManagement(mgr))
			return interop.NewUnwrapInternalBatchProcessor(v1Proc), nil
		})
	if err != nil {
		panic(err)
	}
}

type mappingProc struct {
	exec *mapping.Executor
	log  *service.Logger
}

func newMapping(exec *bloblang.Executor, log *service.Logger) *mappingProc {
	uw := exec.XUnwrapper().(interface {
		Unwrap() *mapping.Executor
	}).Unwrap()

	return &mappingProc{
		exec: uw,
		log:  log,
	}
}

func (m *mappingProc) ProcessBatch(ctx context.Context, _ []*tracing.Span, b message.Batch) ([]message.Batch, error) {
	newBatch := make(message.Batch, 0, len(b))
	for i, msg := range b {
		newPart, err := m.exec.MapPart(i, b)
		if err != nil {
			m.log.Error(err.Error())
			msg.ErrorSet(err)
			newBatch = append(newBatch, msg)
			continue
		}
		if newPart != nil {
			newBatch = append(newBatch, newPart)
		}
	}
	if len(newBatch) == 0 {
		return nil, nil
	}
	return []message.Batch{newBatch}, nil
}

func (m *mappingProc) Close(context.Context) error {
	return nil
}
