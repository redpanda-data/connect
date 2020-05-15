package processor

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/ascii85"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
	"github.com/opentracing/opentracing-go"
	"github.com/tilinna/z85"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeEncode] = TypeSpec{
		constructor: NewEncode,
		Summary: `
Encodes messages according to the selected scheme.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("scheme", "The decoding scheme to use.").HasOptions("hex", "base64", "ascii85", "z85"),
			partsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// EncodeConfig contains configuration fields for the Encode processor.
type EncodeConfig struct {
	Scheme string `json:"scheme" yaml:"scheme"`
	Parts  []int  `json:"parts" yaml:"parts"`
}

// NewEncodeConfig returns a EncodeConfig with default values.
func NewEncodeConfig() EncodeConfig {
	return EncodeConfig{
		Scheme: "base64",
		Parts:  []int{},
	}
}

//------------------------------------------------------------------------------

type encodeFunc func(bytes []byte) ([]byte, error)

func base64Encode(b []byte) ([]byte, error) {
	var buf bytes.Buffer

	e := base64.NewEncoder(base64.StdEncoding, &buf)
	e.Write(b)
	e.Close()

	return buf.Bytes(), nil
}

func hexEncode(b []byte) ([]byte, error) {
	var buf bytes.Buffer

	e := hex.NewEncoder(&buf)
	if _, err := e.Write(b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ascii85Encode(b []byte) ([]byte, error) {
	var buf bytes.Buffer

	e := ascii85.NewEncoder(&buf)
	if _, err := e.Write(b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func z85Encode(b []byte) ([]byte, error) {
	// length must be a multiple of 4 bytes
	if len(b) % 4 != 0 {
	  return nil, z85.ErrLength
	}
	enc := make([]byte, z85.EncodedLen(len(b)))
	if _, err := z85.Encode(enc, b); err != nil {
		return nil, err
	}
	return enc, nil
}

func strToEncoder(str string) (encodeFunc, error) {
	switch str {
	case "base64":
		return base64Encode, nil
	case "hex":
		return hexEncode, nil
	case "ascii85":
		return ascii85Encode, nil
	case "z85":
		return z85Encode, nil
	}
	return nil, fmt.Errorf("encode scheme not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Encode is a processor that can selectively encode parts of a message
// following a chosen scheme.
type Encode struct {
	conf EncodeConfig
	fn   encodeFunc

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewEncode returns a Encode processor.
func NewEncode(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	cor, err := strToEncoder(conf.Encode.Scheme)
	if err != nil {
		return nil, err
	}
	return &Encode{
		conf:  conf.Encode,
		fn:    cor,
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
func (c *Encode) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(i int, span opentracing.Span, part types.Part) error {
		newPart, err := c.fn(part.Get())
		if err == nil {
			newMsg.Get(i).Set(newPart)
		} else {
			c.log.Debugf("Failed to encode message part: %v\n", err)
			c.mErr.Incr(1)
		}
		return err
	}

	if newMsg.Len() == 0 {
		return nil, response.NewAck()
	}

	IteratePartsWithSpan(TypeEncode, c.conf.Parts, newMsg, proc)

	c.mBatchSent.Incr(1)
	c.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Encode) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *Encode) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
