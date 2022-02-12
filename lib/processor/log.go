package processor

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeLog] = TypeSpec{
		constructor: NewLog,
		Categories: []Category{
			CategoryUtility,
		},
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

` + "```yaml" + `
pipeline:
  processors:
    - for_each:
      - log:
          level: DEBUG
          message: 'field: ${! json("foo.bar") }, part: ${! meta("kafka_partition") }'
` + "```" + `

The ` + "`level`" + ` field determines the log level of the printed events and
can be any of the following values: TRACE, DEBUG, INFO, WARN, ERROR.

### Structured Fields

It's also possible add custom fields to logs when the format is set to a structured form such as ` + "`json` or `logfmt`" + `. The config field ` + "`fields`" + ` allows you to provide a map of key/value string pairs, where the values support [interpolation functions](/docs/configuration/interpolation#bloblang-queries) allowing you to extract message contents and metadata like this:

` + "```yaml" + `
pipeline:
  processors:
    - log:
        level: DEBUG
        message: hello world
        fields:
          reason: cus I wana
          id: ${! json("id") }
          age: ${! json("user.age") }
          kafka_topic: ${! meta("kafka_topic") }
` + "```" + `

However, these values will always be output as string types. In cases where you want to add other types such as integers or booleans you can use the field ` + "`fields_mapping`" + ` to define a [Bloblang mapping](/docs/guides/bloblang/about) that outputs a map of key/values like this:

` + "```yaml" + `
pipeline:
  processors:
    - log:
        level: DEBUG
        message: hello world
        fields_mapping: |
          root.reason = "cus I wana"
          root.id = this.id
          root.age = this.user.age
          root.kafka_topic = meta("kafka_topic")
` + "```" + `
`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("level", "The log level to use.").HasOptions("FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ALL"),
			docs.FieldString("fields", "A map of fields to print along with the log message.").IsInterpolated().Map(),
			docs.FieldCommon(
				"fields_mapping", "An optional [Bloblang mapping](/docs/guides/bloblang/about) that can be used to specify extra fields to add to the log. If log fields are also added with the `fields` field then those values will override matching keys from this mapping.",
				`root.reason = "cus I wana"
root.id = this.id
root.age = this.user.age.number()
root.kafka_topic = meta("kafka_topic")`,
			).AtVersion("3.40.0").IsBloblang(),
			docs.FieldCommon("message", "The message to print.").IsInterpolated(),
		},
	}
}

//------------------------------------------------------------------------------

// LogConfig contains configuration fields for the Log processor.
type LogConfig struct {
	Level         string            `json:"level" yaml:"level"`
	Fields        map[string]string `json:"fields" yaml:"fields"`
	FieldsMapping string            `json:"fields_mapping" yaml:"fields_mapping"`
	Message       string            `json:"message" yaml:"message"`
}

// NewLogConfig returns a LogConfig with default values.
func NewLogConfig() LogConfig {
	return LogConfig{
		Level:         "INFO",
		Fields:        map[string]string{},
		FieldsMapping: "",
		Message:       "",
	}
}

//------------------------------------------------------------------------------

type logWith interface {
	log.Modular
	With(args ...interface{}) log.Modular
}

// Log is a processor that prints a log event each time it processes a message.
type Log struct {
	logger  log.Modular
	level   string
	message *field.Expression
	fields  map[string]*field.Expression
	printFn func(logger log.Modular, msg string)

	loggerWith    logWith
	fieldsMapping *mapping.Executor
}

// NewLog returns a Log processor.
func NewLog(
	conf Config, mgr interop.Manager, logger log.Modular, stats metrics.Type,
) (processor.V1, error) {
	message, err := mgr.BloblEnvironment().NewField(conf.Log.Message)
	if err != nil {
		return nil, fmt.Errorf("failed to parse message expression: %v", err)
	}
	l := &Log{
		logger:  logger,
		level:   conf.Log.Level,
		fields:  map[string]*field.Expression{},
		message: message,
	}
	if len(conf.Log.Fields) > 0 {
		for k, v := range conf.Log.Fields {
			if l.fields[k], err = mgr.BloblEnvironment().NewField(v); err != nil {
				return nil, fmt.Errorf("failed to parse field '%v' expression: %v", k, err)
			}
		}
	}
	if len(conf.Log.FieldsMapping) > 0 {
		var ok bool
		if l.loggerWith, ok = logger.(logWith); !ok {
			return nil, errors.New("the provided logger does not support structured fields required for `fields_mapping`")
		}
		if l.fieldsMapping, err = mgr.BloblEnvironment().NewMapping(conf.Log.FieldsMapping); err != nil {
			return nil, fmt.Errorf("failed to parse fields mapping: %w", err)
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
func (l *Log) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	targetLog := l.logger
	if l.fieldsMapping != nil {
		v, err := l.fieldsMapping.Exec(query.FunctionContext{
			Maps:     map[string]query.Function{},
			Vars:     map[string]interface{}{},
			Index:    0,
			MsgBatch: msg,
		}.WithValueFunc(func() *interface{} {
			jObj, err := msg.Get(0).JSON()
			if err != nil {
				return nil
			}
			return &jObj
		}))
		if err != nil {
			l.logger.Errorf("Failed to execute fields mapping: %v", err)
			resMsg := msg.Copy()
			_ = resMsg.Iter(func(i int, p *message.Part) error {
				FlagErr(p, err)
				return nil
			})
			return []*message.Batch{resMsg}, nil
		}
		vObj, ok := v.(map[string]interface{})
		if !ok {
			l.logger.Errorf("Fields mapping yielded a non-object result: %T", v)
			rErr := fmt.Errorf("fields mapping yielded a non-object result: %T", v)
			resMsg := msg.Copy()
			_ = resMsg.Iter(func(i int, p *message.Part) error {
				FlagErr(p, rErr)
				return nil
			})
			return []*message.Batch{resMsg}, nil
		}

		keys := make([]string, 0, len(vObj))
		for k := range vObj {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		args := make([]interface{}, 0, len(vObj)*2)
		for _, k := range keys {
			args = append(args, k, vObj[k])
		}
		targetLog = l.loggerWith.With(args...)
	}
	if len(l.fields) > 0 {
		interpFields := make(map[string]string, len(l.fields))
		for k, vi := range l.fields {
			interpFields[k] = vi.String(0, msg)
		}
		targetLog = log.WithFields(targetLog, interpFields)
	}
	l.printFn(targetLog, l.message.String(0, msg))
	return []*message.Batch{msg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (l *Log) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (l *Log) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
