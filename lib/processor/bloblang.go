package processor

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/mapping"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
	olog "github.com/opentracing/opentracing-go/log"
	"golang.org/x/xerrors"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBloblang] = TypeSpec{
		constructor: NewBloblang,
		Summary: `
Executes a [Bloblang](/docs/guides/bloblang/about) mapping on messages.`,
		Description: `
Bloblang is a powerful language that enables a wide range of mapping,
transformation and filtering tasks. For more information
[check out the docs](/docs/guides/bloblang/about).`,
		Footnotes: `
## Error Handling

Bloblang mappings can fail, in which case the message remains unchanged, errors
are logged, and the message is flagged as having failed, allowing you to use
[standard processor error handling patterns](/docs/configuration/error_handling).

However, Bloblang itself also provides powerful ways of ensuring your mappings
do not fail by specifying desired fallback behaviour, which you can read about
[in this section](/docs/guides/bloblang/about#error-handling).

## Examples

### Mapping

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

We can reduce the fans to only those with an obsession score above 0.5 with this
mapping:

` + "```yaml" + `
pipeline:
  processors:
  - bloblang: |
      root = this
      fans = fans.map_each(match {
        this.obsession > 0.5 => this
        _ => deleted()
      })
` + "```" + `

Giving us:

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

### Parsing CSV

Bloblang can be used to parse some basic CSV files, given files of the following
format:

` + "```" + `
foo,bar,baz
1,2,3
7,11,23
89,23,2
` + "```" + `

We can write a parser that does cool things like calculating the sum of each
line:

` + "```yaml" + `
pipeline:
  processors:
  - bloblang: |
      root = content().string().split("\n").enumerated().map_each(match {
        index == 0 => deleted() # Drop the first line
        _ => match value.trim() {
          this.length() == 0 => deleted() # Drop empty lines
          _ => this.split(",")            # Split the remaining by comma
        }
      }).map_each(
        # Then do something cool like sum each row
        this.map_each(this.trim().number(0)).sum()
      )
` + "```" + `

To give an output like this:

` + "```json" + `
[6,41,114]
` + "```" + ``,
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

// Bloblang is a processor that performs a Bloblang mapping.
type Bloblang struct {
	exec *mapping.Executor

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
	mDropped   metrics.StatCounter
}

// NewBloblang returns a Bloblang processor.
func NewBloblang(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	exec, err := mapping.NewExecutor(string(conf.Bloblang))
	if err != nil {
		return nil, xerrors.Errorf("failed to parse mapping: %w", err)
	}

	return &Bloblang{
		exec: exec,

		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
		mDropped:   stats.GetCounter("dropped"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (b *Bloblang) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	b.mCount.Incr(1)

	newParts := make([]types.Part, 0, msg.Len())

	msg.Iter(func(i int, part types.Part) error {
		span := tracing.GetSpan(part)
		if span == nil {
			span = opentracing.StartSpan(TypeBloblang)
		} else {
			span = opentracing.StartSpan(
				TypeBloblang,
				opentracing.ChildOf(span.Context()),
			)
		}

		p, err := b.exec.MapPart(i, msg)
		if err != nil {
			p = part.Copy()
			b.mErr.Incr(1)
			b.log.Errorf("%v\n", err)
			FlagErr(p, err)
			span.SetTag("error", true)
			span.LogFields(
				olog.String("event", "error"),
				olog.String("type", err.Error()),
			)
		}

		span.Finish()
		if p != nil {
			newParts = append(newParts, p)
		} else {
			b.mDropped.Incr(1)
		}
		return nil
	})

	if len(newParts) == 0 {
		return nil, response.NewAck()
	}

	newMsg := message.New(nil)
	newMsg.SetAll(newParts)

	b.mBatchSent.Incr(1)
	b.mSent.Incr(int64(newMsg.Len()))
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (b *Bloblang) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (b *Bloblang) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
