package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
	"github.com/itchyny/gojq"
	"github.com/opentracing/opentracing-go"
)

func init() {
	Constructors[TypeJQ] = TypeSpec{
		constructor: NewJQ,
		Summary: `
Transforms and filters messages using jq queries.`,
		Description: `
Works by taking all parts of a message and passing them through gojq. The
resulting objects from gojq become the new message parts.

Query syntax is described in [jq's documentation][jq-docs].

[jq-docs]: https://stedolan.github.io/jq/manual/`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("query", "The jq query to filter and transform messages with."),
			docs.FieldAdvanced("raw", "Whether to process the input as a raw string instead of as JSON."),
			docs.FieldAdvanced("raw_output", "Whether to emit string results as raw bytes instead of as JSON."),
			docs.FieldAdvanced("slurp", "Whether to query all parts as an array instead of querying individual inputs. Per-part metadata is not available if true."),
		},
	}
}

//------------------------------------------------------------------------------

// JQConfig contains configuration fields for the JQ processor.
type JQConfig struct {
	Query     string `json:"query" yaml:"query"`
	Raw       bool   `json:"raw" yaml:"raw"`
	RawOutput bool   `json:"raw_output" yaml:"raw_output"`
	Slurp     bool   `json:"slurp" yaml:"slurp"`
}

// NewJQConfig returns a JQConfig with default values.
func NewJQConfig() JQConfig {
	return JQConfig{
		Query: ".",
	}
}

//------------------------------------------------------------------------------

var jqCompileOptions = []gojq.CompilerOption{
	gojq.WithVariables([]string{"$metadata"}),
}

// JQ is a processor that passes messages through gojq.
type JQ struct {
	conf  JQConfig
	log   log.Modular
	stats metrics.Type
	code  *gojq.Code

	mCount        metrics.StatCounter
	mCountParts   metrics.StatCounter
	mSent         metrics.StatCounter
	mBatchSent    metrics.StatCounter
	mDropped      metrics.StatCounter
	mDroppedParts metrics.StatCounter
	mErr          metrics.StatCounter
	mErrJSONParse metrics.StatCounter
	mErrJSONSet   metrics.StatCounter
	mErrQuery     metrics.StatCounter
}

// NewJQ returns a JQ processor.
func NewJQ(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	j := &JQ{
		conf:  conf.JQ,
		stats: stats,
		log:   log,

		mCount:        stats.GetCounter("count"),
		mCountParts:   stats.GetCounter("count_parts"),
		mSent:         stats.GetCounter("sent"),
		mBatchSent:    stats.GetCounter("batch.count"),
		mDropped:      stats.GetCounter("dropped"),
		mDroppedParts: stats.GetCounter("dropped_num_parts"),
		mErr:          stats.GetCounter("error"),
		mErrJSONParse: stats.GetCounter("error.json_parse"),
		mErrJSONSet:   stats.GetCounter("error.json_set"),
		mErrQuery:     stats.GetCounter("error.query"),
	}

	query, err := gojq.Parse(j.conf.Query)
	if err != nil {
		return nil, fmt.Errorf("error parsing jq query: %w", err)
	}

	j.code, err = gojq.Compile(query, jqCompileOptions...)
	if err != nil {
		return nil, fmt.Errorf("error compiling jq query: %w", err)
	}

	return j, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (j *JQ) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	j.mCount.Incr(1)
	if j.conf.Slurp {
		return j.processSlurpedParts(msg)
	}
	return j.processParts(msg, j.conf.Raw)
}

// processSlurpedParts processes a message by slurping all parts into a single
// part and processing it as a single part.
func (j *JQ) processSlurpedParts(msg types.Message) ([]types.Message, types.Response) {
	all := make([]interface{}, 0, msg.Len())
	msg.Iter(func(index int, part types.Part) error {
		obj, err := j.getPartValue(part, j.conf.Raw)
		if err != nil {
			j.mErr.Incr(1)
			return err
		}
		all = append(all, obj)
		return nil
	})

	newPart := message.NewPart(nil)
	if err := newPart.SetJSON(all); err != nil {
		return nil, response.NewError(err)
	}

	newMsg := message.New(nil)
	newMsg.SetAll([]types.Part{newPart})

	return j.processParts(newMsg, false)
}

// processParts processes a message's parts individually. If raw is true, parts
// are processed as raw strings.
func (j *JQ) processParts(msg types.Message, raw bool) ([]types.Message, types.Response) {
	newParts := make([]types.Part, 0, msg.Len())
	process := func(index int, span opentracing.Span, part types.Part) error {
		in, err := j.getPartValue(part, raw)
		if err != nil {
			j.mErr.Incr(1)
			return err
		}
		metadata := j.getPartMetadata(part)

		empty := true
		iter := j.code.Run(in, metadata)
		for {
			out, ok := iter.Next()
			if !ok {
				break
			}

			if err, ok := out.(error); ok {
				j.log.Debugf("Failed to query part: %v\n", err)
				j.mErr.Incr(1)
				j.mErrQuery.Incr(1)
				return err
			}

			newPart := part.Copy()
			var jsonError error
			switch v := out.(type) {
			case string:
				if j.conf.RawOutput {
					newPart.Set([]byte(v))
				} else {
					jsonError = newPart.SetJSON(v)
				}
			default:
				jsonError = newPart.SetJSON(v)
			}
			if jsonError != nil {
				if err == nil {
					err = jsonError
				}
				j.log.Debugf("Failed to convert json into part: %v\n", jsonError)
				j.mErr.Incr(1)
				j.mErrJSONParse.Incr(1)
				continue
			}

			j.mSent.Incr(1)
			newParts = append(newParts, newPart)
			empty = false
		}

		if empty {
			j.mDroppedParts.Incr(1)
		}

		return nil
	}

	IteratePartsWithSpan(TypeJQ, nil, msg, process)
	if len(newParts) == 0 {
		j.mDropped.Incr(1)
		return nil, response.NewAck()
	}

	newMsg := message.New(nil)
	newMsg.SetAll(newParts)

	j.mBatchSent.Incr(1)
	j.mSent.Incr(int64(newMsg.Len()))

	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (*JQ) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (*JQ) WaitForClose(timeout time.Duration) error {
	return nil
}

func (j *JQ) getPartMetadata(part types.Part) map[string]interface{} {
	metadata := map[string]interface{}{}
	part.Metadata().Iter(func(k, v string) error {
		metadata[k] = v
		return nil
	})
	return metadata
}

func (j *JQ) getPartValue(part types.Part, raw bool) (obj interface{}, err error) {
	if raw {
		return string(part.Get()), nil
	}
	obj, err = part.JSON()
	if err == nil {
		obj, err = message.CopyJSON(obj)
	}
	if err != nil {
		j.mErrJSONParse.Incr(1)
		j.log.Debugf("Failed to parse part into json: %v\n", err)
		return nil, err
	}
	return obj, nil
}
