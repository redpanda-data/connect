package processor

import (
	"fmt"
	"time"

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
		constructor: NewGroupByValue,
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
Splits a batch of messages into N batches, where each resulting batch contains a
group of messages determined by a
[function interpolated string](/docs/configuration/interpolation#bloblang-queries) evaluated
per message.`,
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

// GroupByValue is a processor that breaks message batches down into N batches
// of a smaller size according to a function interpolated string evaluated per
// message part.
type GroupByValue struct {
	log   log.Modular
	stats metrics.Type

	value *field.Expression

	mCount     metrics.StatCounter
	mGroups    metrics.StatGauge
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewGroupByValue returns a GroupByValue processor.
func NewGroupByValue(
	conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type,
) (processor.V1, error) {
	value, err := mgr.BloblEnvironment().NewField(conf.GroupByValue.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}
	return &GroupByValue{
		log:   log,
		stats: stats,

		value: value,

		mCount:     stats.GetCounter("count"),
		mGroups:    stats.GetGauge("groups"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (g *GroupByValue) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	g.mCount.Incr(1)

	if msg.Len() == 0 {
		return nil, nil
	}

	groupKeys := []string{}
	groupMap := map[string]*message.Batch{}

	spans := tracing.CreateChildSpans(TypeGroupByValue, msg)

	_ = msg.Iter(func(i int, p *message.Part) error {
		v := g.value.String(i, msg)
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

	for _, s := range spans {
		s.Finish()
	}

	msgs := []*message.Batch{}
	for _, key := range groupKeys {
		msgs = append(msgs, groupMap[key])
	}

	g.mGroups.Set(int64(len(groupKeys)))

	if len(msgs) == 0 {
		return nil, nil
	}

	g.mBatchSent.Incr(int64(len(msgs)))
	for _, m := range msgs {
		g.mSent.Incr(int64(m.Len()))
	}
	return msgs, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (g *GroupByValue) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (g *GroupByValue) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
