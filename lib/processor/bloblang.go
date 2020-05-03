package processor

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/mapping"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/xerrors"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBloblang] = TypeSpec{
		constructor: NewBloblang,
		Summary: `
WARNING: This processor is considered experimental and therefore subject to
change outside of major version releases.

Executes a [Bloblang](/docs/guides/bloblang/about) mapping on messages.`,
		Description: `
For more information about Bloblang
[check out the docs](/docs/guides/bloblang/about).

Check out the [examples section](#examples) in order to see how this processor
can be used.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("mapping", "The Bloblang mapping to apply"),
			partsFieldSpec,
		},
		Footnotes: `
## Examples

TODO`,
	}
}

//------------------------------------------------------------------------------

// BloblangConfig contains configuration fields for the Bloblang processor.
type BloblangConfig struct {
	Parts   []int  `json:"parts" yaml:"parts"`
	Mapping string `json:"mapping" yaml:"mapping"`
}

// NewBloblangConfig returns a BloblangConfig with default values.
func NewBloblangConfig() BloblangConfig {
	return BloblangConfig{
		Parts:   []int{},
		Mapping: "",
	}
}

//------------------------------------------------------------------------------

// Bloblang is a processor that performs a Bloblang mapping.
type Bloblang struct {
	parts []int

	exec *mapping.Executor

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewBloblang returns a Bloblang processor.
func NewBloblang(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	exec, err := mapping.NewExecutor(conf.Bloblang.Mapping)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse mapping: %w", err)
	}

	return &Bloblang{
		parts: conf.Bloblang.Parts,
		exec:  exec,

		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (b *Bloblang) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	b.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		if err := b.exec.MapPart(index, newMsg); err != nil {
			b.mErr.Incr(1)
			b.log.Debugf("Mapping failed: %v\n", err)
			return err
		}
		return nil
	}

	IteratePartsWithSpan(TypeBloblang, b.parts, newMsg, proc)

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
