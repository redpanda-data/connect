package pure

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newLogProcessor(conf, mgr, mgr.Logger())
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("log", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "log",
		Categories: []string{
			"Utility",
		},
		Summary: `Prints a log event for each message. Messages always remain unchanged. The log message can be set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries) which allows you to log the contents and metadata of messages.`,
		Description: `
The ` + "`level`" + ` field determines the log level of the printed events and can be any of the following values: TRACE, DEBUG, INFO, WARN, ERROR.

### Structured Fields

It's also possible add custom fields to logs when the format is set to a structured form such as ` + "`json` or `logfmt`" + ` with the config field ` + "[`fields_mapping`](#fields_mapping)" + `:

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
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("level", "The log level to use.").HasOptions("FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ALL").LinterFunc(nil),
			docs.FieldString("fields", "A map of fields to print along with the log message.").IsInterpolated().Map().Deprecated(),
			docs.FieldString(
				"fields_mapping", "An optional [Bloblang mapping](/docs/guides/bloblang/about) that can be used to specify extra fields to add to the log. If log fields are also added with the `fields` field then those values will override matching keys from this mapping.",
				`root.reason = "cus I wana"
root.id = this.id
root.age = this.user.age.number()
root.kafka_topic = meta("kafka_topic")`,
			).AtVersion("3.40.0").IsBloblang(),
			docs.FieldString("message", "The message to print.").IsInterpolated(),
		).ChildDefaultAndTypesFromStruct(processor.NewLogConfig()),
	})
	if err != nil {
		panic(err)
	}
}

type logProcessor struct {
	logger        log.Modular
	level         string
	message       *field.Expression
	fields        map[string]*field.Expression
	printFn       func(logger log.Modular, msg string)
	fieldsMapping *mapping.Executor
}

func newLogProcessor(conf processor.Config, mgr bundle.NewManagement, logger log.Modular) (processor.V2Batched, error) {
	message, err := mgr.BloblEnvironment().NewField(conf.Log.Message)
	if err != nil {
		return nil, fmt.Errorf("failed to parse message expression: %v", err)
	}
	l := &logProcessor{
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
		if l.fieldsMapping, err = mgr.BloblEnvironment().NewMapping(conf.Log.FieldsMapping); err != nil {
			return nil, fmt.Errorf("failed to parse fields mapping: %w", err)
		}
	}
	if l.printFn, err = l.levelToLogFn(l.level); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *logProcessor) levelToLogFn(level string) (func(logger log.Modular, msg string), error) {
	level = strings.ToUpper(level)
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

func (l *logProcessor) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	_ = msg.Iter(func(i int, _ *message.Part) error {
		targetLog := l.logger
		if l.fieldsMapping != nil {
			v, err := l.fieldsMapping.Exec(query.FunctionContext{
				Maps:     map[string]query.Function{},
				Vars:     map[string]any{},
				Index:    i,
				MsgBatch: msg,
			}.WithValueFunc(func() *any {
				jObj, err := msg.Get(i).AsStructured()
				if err != nil {
					return nil
				}
				return &jObj
			}))
			if err != nil {
				l.logger.Errorf("Failed to execute fields mapping: %v", err)
				return nil
			}

			vObj, ok := v.(map[string]any)
			if !ok {
				l.logger.Errorf("Fields mapping yielded a non-object result: %T", v)
				return nil
			}

			keys := make([]string, 0, len(vObj))
			for k := range vObj {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			args := make([]any, 0, len(vObj)*2)
			for _, k := range keys {
				args = append(args, k, vObj[k])
			}
			targetLog = targetLog.With(args...)
		}

		if len(l.fields) > 0 {
			interpFields := make(map[string]string, len(l.fields))
			for k, vi := range l.fields {
				var err error
				if interpFields[k], err = vi.String(i, msg); err != nil {
					l.logger.Errorf("Field %v interpolation error: %v", k, err)
					return nil
				}
			}
			targetLog = targetLog.WithFields(interpFields)
		}
		logMsg, err := l.message.String(i, msg)
		if err != nil {
			l.logger.Errorf("Message interpolation error: %v", err)
			return nil
		}
		l.printFn(targetLog, logMsg)
		return nil
	})

	return []message.Batch{msg}, nil
}

func (l *logProcessor) Close(ctx context.Context) error {
	return nil
}
