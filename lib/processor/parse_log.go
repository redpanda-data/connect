package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
	"github.com/influxdata/go-syslog/rfc5424"
	"github.com/opentracing/opentracing-go"
)

func init() {
	Constructors[TypeParseLog] = TypeSpec{
		constructor: NewParseLog,
		Summary: `
Parses common log [formats](#format) into structured data (JSON). This is easier
and often much faster than ` + "[`grok`](/docs/components/processors/grok)" + `.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("format", "A common log format to parse.").HasOptions(
				"syslog_rfc5424",
			),
			docs.FieldCommon("codec", "Specifies the structured format to parse a log into.").HasOptions(
				"json",
			),
			partsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// ParseLogConfig contains configuration fields for the ParseLog processor.
type ParseLogConfig struct {
	Parts  []int  `json:"parts" yaml:"parts"`
	Format string `json:"format" yaml:"format"`
	Codec  string `json:"codec" yaml:"codec"`
}

// NewParseLogConfig returns a ParseLogConfig with default values.
func NewParseLogConfig() ParseLogConfig {
	return ParseLogConfig{
		Parts:  []int{},
		Format: "syslog_rfc5424",
		Codec:  "json",
	}
}

//------------------------------------------------------------------------------

type parserFormat func(body []byte) (map[string]interface{}, error)

func parserRFC5424() parserFormat {
	return func(body []byte) (map[string]interface{}, error) {
		p := rfc5424.NewParser()
		// TODO: Potentially expose this as a config field later.
		be := true
		res, err := p.Parse(body, &be)
		if err != nil {
			return nil, err
		}
		resMap := make(map[string]interface{})
		if res.Message() != nil {
			resMap["message"] = *res.Message()
		}
		if res.Timestamp() != nil {
			resMap["timestamp"] = *res.Timestamp()
		}
		if res.Hostname() != nil {
			resMap["hostname"] = *res.Hostname()
		}
		if res.ProcID() != nil {
			resMap["procid"] = *res.ProcID()
		}
		if res.Appname() != nil {
			resMap["appname"] = *res.Appname()
		}
		if res.MsgID() != nil {
			resMap["msgid"] = *res.MsgID()
		}
		if res.StructuredData() != nil {
			resMap["structureddata"] = *res.StructuredData()
		}
		return resMap, nil
	}
}

func getParseFormat(parser string) (parserFormat, error) {
	switch parser {
	case "syslog_rfc5424":
		return parserRFC5424(), nil
	}
	return nil, fmt.Errorf("format not recognised: %s", parser)
}

//------------------------------------------------------------------------------

// ParseLog is a processor that parses properly formatted messages.
type ParseLog struct {
	parts  []int
	format parserFormat

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mErrJSONS  metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewParseLog returns a ParseLog processor.
func NewParseLog(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	s := &ParseLog{
		parts: conf.ParseLog.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	var err error
	if s.format, err = getParseFormat(conf.ParseLog.Format); err != nil {
		return nil, err
	}
	return s, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *ParseLog) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	s.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		dataMap, err := s.format(part.Get())
		if err != nil {
			s.mErr.Incr(1)
			s.log.Debugf("Failed to parse message as %s: %v\n", s.conf.ParseLog.Format, err)
			return err
		}
		if err := newMsg.Get(index).SetJSON(dataMap); err != nil {
			s.mErrJSONS.Incr(1)
			s.mErr.Incr(1)
			s.log.Debugf("Failed to convert log format result into json: %v\n", err)
			return err
		}

		return nil
	}

	IteratePartsWithSpan(TypeParseLog, s.parts, newMsg, proc)

	s.mBatchSent.Incr(1)
	s.mSent.Incr(int64(newMsg.Len()))
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *ParseLog) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (s *ParseLog) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
