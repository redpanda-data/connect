package processor

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMsgPack] = TypeSpec{
		constructor: NewMsgPack,
		Categories: []Category{
			CategoryParsing,
		},
		Summary: `
Performs MsgPack based operations on messages.`,
		Status: docs.StatusBeta,
		Description: `
## Operators

### ` + "`to_json`" + `

Converts MsgPack documents into a JSON structure. This makes it easier to
manipulate the contents of the document within Benthos.

### ` + "`from_json`" + `

Attempts to convert JSON documents into MsgPack documents.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("operator", "The [operator](#operators) to execute").HasOptions("to_json", "from_json"),
			PartsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// MsgPackConfig contains configuration fields for the MsgPack processor.
type MsgPackConfig struct {
	Parts    []int  `json:"parts" yaml:"parts"`
	Operator string `json:"operator" yaml:"operator"`
}

// NewMsgPackConfig returns a MsgPackConfig with default values.
func NewMsgPackConfig() MsgPackConfig {
	return MsgPackConfig{
		Parts:    []int{},
		Operator: "to_json",
	}
}

//------------------------------------------------------------------------------

type msgPackOperator func(part types.Part) error

func strToMsgPackOperator(opStr string) (msgPackOperator, error) {
	switch opStr {
	case "to_json":
		return func(part types.Part) error {
			var jObj interface{}
			if err := msgpack.Unmarshal(part.Get(), &jObj); err != nil {
				return fmt.Errorf("failed to convert MsgPack document to JSON: %v", err)
			}
			if err := part.SetJSON(jObj); err != nil {
				return fmt.Errorf("failed to set JSON: %v", err)
			}
			return nil
		}, nil
	case "from_json":
		return func(part types.Part) error {
			jObj, err := part.JSON()
			if err != nil {
				return fmt.Errorf("failed to parse message as JSON: %v", err)
			}
			bytes, err := msgpack.Marshal(jObj)
			if err != nil {
				return fmt.Errorf("failed to convert JSON to MsgPack: %v", err)
			}
			part.Set(bytes)
			return nil
		}, nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

//------------------------------------------------------------------------------

// MsgPack is a processor that performs an operation on an MsgPack payload.
type MsgPack struct {
	parts    []int
	operator msgPackOperator

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewMsgPack returns an MsgPack processor.
func NewMsgPack(
	conf Config, _ types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	a := &MsgPack{
		parts: conf.MsgPack.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	var err error

	msgpack.Register(json.Number("0"),
		func(enc *msgpack.Encoder, value reflect.Value) error {
			strValue := value.String()
			if intValue, err := strconv.ParseInt(strValue, 10, 64); err == nil {
				if err := enc.EncodeInt(intValue); err != nil {
					return err
				}
			} else if uintValue, err := strconv.ParseUint(strValue, 10, 64); err == nil {
				if err := enc.EncodeUint(uintValue); err != nil {
					return err
				}
			} else if floatValue, err := strconv.ParseFloat(strValue, 64); err == nil {
				if err := enc.EncodeFloat64(floatValue); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("unable to parse %s neither as int nor as float", strValue)
			}
			return nil
		},
		func(dec *msgpack.Decoder, value reflect.Value) error {
			return nil
		},
	)

	if a.operator, err = strToMsgPackOperator(conf.MsgPack.Operator); err != nil {
		return nil, err
	}
	return a, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *MsgPack) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		if err := p.operator(part); err != nil {
			p.mErr.Incr(1)
			p.log.Debugf("Operator failed: %v\n", err)
			return err
		}
		return nil
	}

	IteratePartsWithSpan(TypeMsgPack, p.parts, newMsg, proc)

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(newMsg.Len()))
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *MsgPack) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *MsgPack) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
