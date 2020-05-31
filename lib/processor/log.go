package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeLog] = TypeSpec{
		constructor: NewLog,
		Summary: `
Prints a log event each time it processes a batch. Messages always remain
unchanged. The log message can be set using function interpolations described
[here](/docs/configuration/interpolation#bloblang-queries) which allows you to log the
contents and metadata of messages.`,
		Description: `
In order to print a log message per message of a batch place it within a
` + "[`for_each`](/docs/components/processors/for_each)" + ` processor.

For example, if we wished to create a debug log event for each message in a
pipeline in order to expose the JSON field ` + "`foo.bar`" + ` as well as the
metadata field ` + "`kafka_partition`" + ` we can achieve that with the
following config:

` + "``` yaml" + `
for_each:
- log:
    level: DEBUG
    message: 'field: ${! json("foo.bar") }, part: ${! meta("kafka_partition") }'
` + "```" + `

The ` + "`level`" + ` field determines the log level of the printed events and
can be any of the following values: TRACE, DEBUG, INFO, WARN, ERROR.

### Structured Fields

It's also possible to output a map of structured fields, this only works when
the service log is set to output as JSON. The field values are function
interpolated, meaning it's possible to output structured fields containing
message contents and metadata, e.g.:

` + "``` yaml" + `
log:
  level: DEBUG
  message: "foo"
  fields:
    id: '${! json("id") }'
    kafka_topic: '${! meta("kafka_topic") }'
` + "```" + ``,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("level", "The log level to use.").HasOptions("FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ALL"),
			docs.FieldCommon("fields", "A map of fields to print along with the log message.").SupportsInterpolation(true),
			docs.FieldCommon("message", "The message to print.").SupportsInterpolation(true),
		},
	}
}

//------------------------------------------------------------------------------

// LogConfig contains configuration fields for the Log processor.
type LogConfig struct {
	Level   string            `json:"level" yaml:"level"`
	Fields  map[string]string `json:"fields" yaml:"fields"`
	Message string            `json:"message" yaml:"message"`
}

// NewLogConfig returns a LogConfig with default values.
func NewLogConfig() LogConfig {
	return LogConfig{
		Level:   "INFO",
		Fields:  map[string]string{},
		Message: "",
	}
}

//------------------------------------------------------------------------------

// Log is a processor that prints a log event each time it processes a message.
type Log struct {
	log     log.Modular
	level   string
	message field.Expression
	fields  map[string]field.Expression
	printFn func(logger log.Modular, msg string)
}

// NewLog returns a Log processor.
func NewLog(
	conf Config, mgr types.Manager, logger log.Modular, stats metrics.Type,
) (Type, error) {
	message, err := field.New(conf.Log.Message)
	if err != nil {
		return nil, fmt.Errorf("failed to parse message expression: %v", err)
	}
	l := &Log{
		log:     logger,
		level:   conf.Log.Level,
		fields:  map[string]field.Expression{},
		message: message,
	}
	if len(conf.Log.Fields) > 0 {
		for k, v := range conf.Log.Fields {
			if l.fields[k], err = field.New(v); err != nil {
				return nil, fmt.Errorf("failed to parse field '%v' expression: %v", k, err)
			}
		}
	}
	if l.printFn, err = l.levelToLogFn(l.level); err != nil {
		return nil, err
	}
	return l, nil
}

//------------------------------------------------------------------------------

func (l *Log) levelToLogFn(level string) (func(logger log.Modular, msg string), error) {
	switch level {
	case "TRACE":
		return func(logger log.Modular, msg string) {
			logger.Traceln(msg)
		}, nil
	case "DEBUG":
		return func(logger log.Modular, msg string) {
			logger.Debugln(msg)
		}, nil
	case "INFO":
		return func(logger log.Modular, msg string) {
			logger.Infoln(msg)
		}, nil
	case "WARN":
		return func(logger log.Modular, msg string) {
			logger.Warnln(msg)
		}, nil
	case "ERROR":
		return func(logger log.Modular, msg string) {
			logger.Errorln(msg)
		}, nil
	}
	return nil, fmt.Errorf("log level not recognised: %v", level)
}

//------------------------------------------------------------------------------

// ProcessMessage logs an event and returns the message unchanged.
func (l *Log) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	targetLog := l.log
	if len(l.fields) > 0 {
		interpFields := make(map[string]string, len(l.fields))
		for k, vi := range l.fields {
			interpFields[k] = vi.String(0, msg)
		}
		targetLog = log.WithFields(targetLog, interpFields)
	}
	msgs := [1]types.Message{msg}
	l.printFn(targetLog, l.message.String(0, msg))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (l *Log) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (l *Log) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
