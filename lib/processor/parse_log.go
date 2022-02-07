package processor

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	syslog "github.com/influxdata/go-syslog/v3"
	"github.com/influxdata/go-syslog/v3/rfc3164"
	"github.com/influxdata/go-syslog/v3/rfc5424"
)

func init() {
	Constructors[TypeParseLog] = TypeSpec{
		constructor: NewParseLog,
		Categories: []Category{
			CategoryParsing,
		},
		Summary: `
Parses common log [formats](#formats) into [structured data](#codecs). This is
easier and often much faster than ` + "[`grok`](/docs/components/processors/grok)" + `.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("format", "A common log [format](#formats) to parse.").HasOptions(
				"syslog_rfc5424", "syslog_rfc3164",
			),
			docs.FieldCommon("codec", "Specifies the structured format to parse a log into.").HasOptions(
				"json",
			),
			docs.FieldAdvanced("best_effort", "Still returns partially parsed messages even if an error occurs."),
			docs.FieldAdvanced("allow_rfc3339", "Also accept timestamps in rfc3339 format while parsing."+
				" Applicable to format `syslog_rfc3164`."),
			docs.FieldAdvanced("default_year", "Sets the strategy used to set the year for rfc3164 timestamps."+
				" Applicable to format `syslog_rfc3164`. When set to `current` the current year will be set, when"+
				" set to an integer that value will be used. Leave this field empty to not set a default year at all."),
			docs.FieldAdvanced("default_timezone", "Sets the strategy to decide the timezone for rfc3164 timestamps."+
				" Applicable to format `syslog_rfc3164`. This value should follow the [time.LoadLocation](https://golang.org/pkg/time/#LoadLocation) format."),

			PartsFieldSpec,
		},
		Footnotes: `
## Codecs

Currently the only supported structured data codec is ` + "`json`" + `.

## Formats

### ` + "`syslog_rfc5424`" + `

Attempts to parse a log following the [Syslog rfc5424](https://tools.ietf.org/html/rfc5424)
spec. The resulting structured document may contain any of the following fields:

- ` + "`message`" + ` (string)
- ` + "`timestamp`" + ` (string, RFC3339)
- ` + "`facility`" + ` (int)
- ` + "`severity`" + ` (int)
- ` + "`priority`" + ` (int)
- ` + "`version`" + ` (int)
- ` + "`hostname`" + ` (string)
- ` + "`procid`" + ` (string)
- ` + "`appname`" + ` (string)
- ` + "`msgid`" + ` (string)
- ` + "`structureddata`" + ` (object)

### ` + "`syslog_rfc3164`" + `

Attempts to parse a log following the [Syslog rfc3164](https://tools.ietf.org/html/rfc3164)
spec. The resulting structured document may contain any of the following fields:

- ` + "`message`" + ` (string)
- ` + "`timestamp`" + ` (string, RFC3339)
- ` + "`facility`" + ` (int)
- ` + "`severity`" + ` (int)
- ` + "`priority`" + ` (int)
- ` + "`hostname`" + ` (string)
- ` + "`procid`" + ` (string)
- ` + "`appname`" + ` (string)
- ` + "`msgid`" + ` (string)
`,
	}
}

//------------------------------------------------------------------------------

// ParseLogConfig contains configuration fields for the ParseLog processor.
type ParseLogConfig struct {
	Parts        []int  `json:"parts" yaml:"parts"`
	Format       string `json:"format" yaml:"format"`
	Codec        string `json:"codec" yaml:"codec"`
	BestEffort   bool   `json:"best_effort" yaml:"best_effort"`
	WithRFC3339  bool   `json:"allow_rfc3339" yaml:"allow_rfc3339"`
	WithYear     string `json:"default_year" yaml:"default_year"`
	WithTimezone string `json:"default_timezone" yaml:"default_timezone"`
}

// NewParseLogConfig returns a ParseLogConfig with default values.
func NewParseLogConfig() ParseLogConfig {
	return ParseLogConfig{
		Parts:  []int{},
		Format: "syslog_rfc5424",
		Codec:  "json",

		BestEffort:   true,
		WithRFC3339:  true,
		WithYear:     "current",
		WithTimezone: "UTC",
	}
}

//------------------------------------------------------------------------------

type parserFormat func(body []byte) (map[string]interface{}, error)

func parserRFC5424(bestEffort bool) parserFormat {
	var opts []syslog.MachineOption
	if bestEffort {
		opts = append(opts, rfc5424.WithBestEffort())
	}
	p := rfc5424.NewParser(opts...)

	return func(body []byte) (map[string]interface{}, error) {
		resGen, err := p.Parse(body)
		if err != nil {
			return nil, err
		}
		res := resGen.(*rfc5424.SyslogMessage)

		resMap := make(map[string]interface{})
		if res.Message != nil {
			resMap["message"] = *res.Message
		}
		if res.Timestamp != nil {
			resMap["timestamp"] = res.Timestamp.Format(time.RFC3339Nano)
		}
		if res.Facility != nil {
			resMap["facility"] = *res.Facility
		}
		if res.Severity != nil {
			resMap["severity"] = *res.Severity
		}
		if res.Priority != nil {
			resMap["priority"] = *res.Priority
		}
		if res.Version != 0 {
			resMap["version"] = res.Version
		}
		if res.Hostname != nil {
			resMap["hostname"] = *res.Hostname
		}
		if res.ProcID != nil {
			resMap["procid"] = *res.ProcID
		}
		if res.Appname != nil {
			resMap["appname"] = *res.Appname
		}
		if res.MsgID != nil {
			resMap["msgid"] = *res.MsgID
		}
		if res.StructuredData != nil {
			resMap["structureddata"] = *res.StructuredData
		}

		return resMap, nil
	}
}

func parserRFC3164(bestEffort, wrfc3339 bool, year, tz string) (parserFormat, error) {
	var opts []syslog.MachineOption
	if bestEffort {
		opts = append(opts, rfc3164.WithBestEffort())
	}
	if wrfc3339 {
		opts = append(opts, rfc3164.WithRFC3339())
	}
	switch year {
	case "current":
		opts = append(opts, rfc3164.WithYear(rfc3164.CurrentYear{}))
	case "":
		// do nothing
	default:
		iYear, err := strconv.Atoi(year)
		if err != nil {
			return nil, fmt.Errorf("failed to convert year %s into integer:  %v", year, err)
		}
		opts = append(opts, rfc3164.WithYear(rfc3164.Year{YYYY: iYear}))
	}
	if tz != "" {
		loc, err := time.LoadLocation(tz)
		if err != nil {
			return nil, fmt.Errorf("failed to lookup timezone %s - %v", loc, err)
		}
		opts = append(opts, rfc3164.WithTimezone(loc))
	}

	p := rfc3164.NewParser(opts...)

	return func(body []byte) (map[string]interface{}, error) {
		resGen, err := p.Parse(body)
		if err != nil {
			return nil, err
		}
		res := resGen.(*rfc3164.SyslogMessage)

		resMap := make(map[string]interface{})
		if res.Message != nil {
			resMap["message"] = *res.Message
		}
		if res.Timestamp != nil {
			resMap["timestamp"] = res.Timestamp.Format(time.RFC3339Nano)
		}
		if res.Facility != nil {
			resMap["facility"] = *res.Facility
		}
		if res.Severity != nil {
			resMap["severity"] = *res.Severity
		}
		if res.Priority != nil {
			resMap["priority"] = *res.Priority
		}
		if res.Hostname != nil {
			resMap["hostname"] = *res.Hostname
		}
		if res.ProcID != nil {
			resMap["procid"] = *res.ProcID
		}
		if res.Appname != nil {
			resMap["appname"] = *res.Appname
		}
		if res.MsgID != nil {
			resMap["msgid"] = *res.MsgID
		}

		return resMap, nil
	}, nil
}

func getParseFormat(parser string, bestEffort, rfc3339 bool, defYear, defTZ string) (parserFormat, error) {
	switch parser {
	case "syslog_rfc5424":
		return parserRFC5424(bestEffort), nil
	case "syslog_rfc3164":
		return parserRFC3164(bestEffort, rfc3339, defYear, defTZ)
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
	if s.format, err = getParseFormat(conf.ParseLog.Format, conf.ParseLog.BestEffort, conf.ParseLog.WithRFC3339,
		conf.ParseLog.WithYear, conf.ParseLog.WithTimezone); err != nil {
		return nil, err
	}
	return s, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *ParseLog) ProcessMessage(msg *message.Batch) ([]*message.Batch, types.Response) {
	s.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span *tracing.Span, part *message.Part) error {
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

	IteratePartsWithSpanV2(TypeParseLog, s.parts, newMsg, proc)

	s.mBatchSent.Incr(1)
	s.mSent.Incr(int64(newMsg.Len()))
	return []*message.Batch{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *ParseLog) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (s *ParseLog) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
