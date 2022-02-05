package processor

import (
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMetadata] = TypeSpec{
		constructor: NewMetadata,
		Status:      docs.StatusDeprecated,
		Footnotes: `
## Alternatives

All functionality of this processor has been superseded by the
[bloblang](/docs/components/processors/bloblang) processor.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("operator", "The operator to apply to messages.").HasOptions("set", "delete", "delete_all", "delete_prefix"),
			docs.FieldCommon("key", "The metadata key to target with the chosen operator.").IsInterpolated(),
			docs.FieldCommon("value", "The metadata value to use with the chosen operator.").IsInterpolated(),
			PartsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// MetadataConfig contains configuration fields for the Metadata processor.
type MetadataConfig struct {
	Parts    []int  `json:"parts" yaml:"parts"`
	Operator string `json:"operator" yaml:"operator"`
	Key      string `json:"key" yaml:"key"`
	Value    string `json:"value" yaml:"value"`
}

// NewMetadataConfig returns a MetadataConfig with default values.
func NewMetadataConfig() MetadataConfig {
	return MetadataConfig{
		Parts:    []int{},
		Operator: "set",
		Key:      "example",
		Value:    `${!hostname()}`,
	}
}

//------------------------------------------------------------------------------

type metadataOperator func(m types.Metadata, key, value string) error

func newMetadataSetOperator() metadataOperator {
	return func(m types.Metadata, key, value string) error {
		m.Set(key, value)
		return nil
	}
}

func newMetadataDeleteAllOperator() metadataOperator {
	return func(m types.Metadata, key, value string) error {
		m.Iter(func(k, _ string) error {
			m.Delete(k)
			return nil
		})
		return nil
	}
}

func newMetadataDeleteOperator() metadataOperator {
	return func(m types.Metadata, key, value string) error {
		target := value
		if target == "" && len(key) > 0 {
			target = key
		}
		m.Delete(target)
		return nil
	}
}

func newMetadataDeletePrefixOperator() metadataOperator {
	return func(m types.Metadata, key, value string) error {
		prefix := value
		if prefix == "" && len(key) > 0 {
			prefix = key
		}
		m.Iter(func(k, _ string) error {
			if strings.HasPrefix(k, prefix) {
				m.Delete(k)
			}
			return nil
		})
		return nil
	}
}

func getMetadataOperator(opStr string) (metadataOperator, error) {
	switch opStr {
	case "set":
		return newMetadataSetOperator(), nil
	case "delete":
		return newMetadataDeleteOperator(), nil
	case "delete_all":
		return newMetadataDeleteAllOperator(), nil
	case "delete_prefix":
		return newMetadataDeletePrefixOperator(), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

//------------------------------------------------------------------------------

// Metadata is a processor that performs an operation on the Metadata of a
// message.
type Metadata struct {
	value *field.Expression
	key   *field.Expression

	operator metadataOperator

	parts []int

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewMetadata returns a Metadata processor.
func NewMetadata(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	value, err := interop.NewBloblangField(mgr, conf.Metadata.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}
	key, err := interop.NewBloblangField(mgr, conf.Metadata.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}
	m := &Metadata{
		conf:  conf,
		log:   log,
		stats: stats,

		parts: conf.Metadata.Parts,

		value: value,
		key:   key,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	if m.operator, err = getMetadataOperator(conf.Metadata.Operator); err != nil {
		return nil, err
	}
	return m, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *Metadata) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span *tracing.Span, part types.Part) error {
		key := p.key.StringLegacy(index, msg)
		value := p.value.StringLegacy(index, msg)

		if err := p.operator(part.Metadata(), key, value); err != nil {
			p.mErr.Incr(1)
			p.log.Debugf("Failed to apply operator: %v\n", err)
			return err
		}
		return nil
	}

	IteratePartsWithSpanV2(TypeMetadata, p.parts, newMsg, proc)

	msgs := [1]types.Message{newMsg}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *Metadata) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *Metadata) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
