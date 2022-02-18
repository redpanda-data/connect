package processor

import (
	"context"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
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
	Constructors[TypeGroupByValue] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newGroupByValue(conf.GroupByValue, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("group_by_value", p, mgr.Metrics()), nil
		},
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `Splits a batch of messages into N batches, where each resulting batch contains a group of messages determined by a [function interpolated string](/docs/configuration/interpolation#bloblang-queries) evaluated per message.`,
		Description: `
This allows you to group messages using arbitrary fields within their content or
metadata, process them individually, and send them to unique locations as per
their group.`,
		Footnotes: `
## Examples

If we were consuming Kafka messages and needed to group them by their key,
archive the groups, and send them to S3 with the key as part of the path we
could achieve that with the following:

` + "```yaml" + `
pipeline:
  processors:
    - group_by_value:
        value: ${! meta("kafka_key") }
    - archive:
        format: tar
    - compress:
        algorithm: gzip
output:
  aws_s3:
    bucket: TODO
    path: docs/${! meta("kafka_key") }/${! count("files") }-${! timestamp_unix_nano() }.tar.gz
` + "```" + ``,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"value", "The interpolated string to group based on.",
				"${! meta(\"kafka_key\") }", "${! json(\"foo.bar\") }-${! meta(\"baz\") }",
			).IsInterpolated(),
		},
		UsesBatches: true,
	}
}

//------------------------------------------------------------------------------

// GroupByValueConfig is a configuration struct containing fields for the
// GroupByValue processor, which breaks message batches down into N batches of a
// smaller size according to a function interpolated string evaluated per
// message part.
type GroupByValueConfig struct {
	Value string `json:"value" yaml:"value"`
}

// NewGroupByValueConfig returns a GroupByValueConfig with default values.
func NewGroupByValueConfig() GroupByValueConfig {
	return GroupByValueConfig{
		Value: "${! meta(\"example\") }",
	}
}

//------------------------------------------------------------------------------

type groupByValueProc struct {
	log   log.Modular
	value *field.Expression
}

func newGroupByValue(conf GroupByValueConfig, mgr interop.Manager) (processor.V2Batched, error) {
	value, err := mgr.BloblEnvironment().NewField(conf.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}
	return &groupByValueProc{
		log:   mgr.Logger(),
		value: value,
	}, nil
}

//------------------------------------------------------------------------------

func (g *groupByValueProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, batch *message.Batch) ([]*message.Batch, error) {
	if batch.Len() == 0 {
		return nil, nil
	}

	groupKeys := []string{}
	groupMap := map[string]*message.Batch{}

	_ = batch.Iter(func(i int, p *message.Part) error {
		v := g.value.String(i, batch)
		spans[i].LogKV(
			"event", "grouped",
			"type", v,
		)
		spans[i].SetTag("group", v)
		if group, exists := groupMap[v]; exists {
			group.Append(p)
		} else {
			g.log.Tracef("New group formed: %v\n", v)
			groupKeys = append(groupKeys, v)
			newMsg := message.QuickBatch(nil)
			newMsg.Append(p)
			groupMap[v] = newMsg
		}
		return nil
	})

	msgs := []*message.Batch{}
	for _, key := range groupKeys {
		msgs = append(msgs, groupMap[key])
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	return msgs, nil
}

func (g *groupByValueProc) Close(context.Context) error {
	return nil
}
