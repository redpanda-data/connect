package pure

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	logPFieldLevel         = "level"
	logPFieldFields        = "fields"
	logPFieldFieldsMapping = "fields_mapping"
	logPFieldMessage       = "message"
)

func logProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary(`Prints a log event for each message. Messages always remain unchanged. The log message can be set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries) which allows you to log the contents and metadata of messages.`).
		Description(`
The `+"`level`"+` field determines the log level of the printed events and can be any of the following values: TRACE, DEBUG, INFO, WARN, ERROR.

### Structured Fields

It's also possible add custom fields to logs when the format is set to a structured form such as `+"`json` or `logfmt`"+` with the config field `+"[`fields_mapping`](#fields_mapping)"+`:

`+"```yaml"+`
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
`+"```"+`
`).
		Fields(
			service.NewStringEnumField(logPFieldLevel, "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ALL").
				Description("The log level to use.").
				LintRule(``).
				Default("INFO"),
			service.NewBloblangField(logPFieldFieldsMapping).
				Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) that can be used to specify extra fields to add to the log. If log fields are also added with the `fields` field then those values will override matching keys from this mapping.").
				Examples(
					`root.reason = "cus I wana"
root.id = this.id
root.age = this.user.age.number()
root.kafka_topic = meta("kafka_topic")`,
				).
				Optional(),
			service.NewInterpolatedStringField(logPFieldMessage).
				Description("The message to print.").
				Default(""),
			service.NewInterpolatedStringMapField(logPFieldFields).
				Description("A map of fields to print along with the log message.").
				Optional().
				Deprecated(),
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"log", logProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			logLevel, err := conf.FieldString(logPFieldLevel)
			if err != nil {
				return nil, err
			}

			messageStr, err := conf.FieldString(logPFieldMessage)
			if err != nil {
				return nil, err
			}

			depFields, _ := conf.FieldStringMap(logPFieldFields)

			fieldsMappingStr, _ := conf.FieldString(logPFieldFieldsMapping)

			mgr := interop.UnwrapManagement(res)
			p, err := newLogProcessor(messageStr, logLevel, fieldsMappingStr, depFields, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("log", p, mgr)), nil
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

func newLogProcessor(messageStr, levelStr, fieldsMappingStr string, depFields map[string]string, mgr bundle.NewManagement) (processor.AutoObservedBatched, error) {
	message, err := mgr.BloblEnvironment().NewField(messageStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse message expression: %v", err)
	}
	l := &logProcessor{
		logger:  mgr.Logger(),
		level:   levelStr,
		fields:  map[string]*field.Expression{},
		message: message,
	}
	if len(depFields) > 0 {
		for k, v := range depFields {
			if l.fields[k], err = mgr.BloblEnvironment().NewField(v); err != nil {
				return nil, fmt.Errorf("failed to parse field '%v' expression: %v", k, err)
			}
		}
	}
	if fieldsMappingStr != "" {
		if l.fieldsMapping, err = mgr.BloblEnvironment().NewMapping(fieldsMappingStr); err != nil {
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
			logger.Trace(msg)
		}, nil
	case "DEBUG":
		return func(logger log.Modular, msg string) {
			logger.Debug(msg)
		}, nil
	case "INFO":
		return func(logger log.Modular, msg string) {
			logger.Info(msg)
		}, nil
	case "WARN":
		return func(logger log.Modular, msg string) {
			logger.Warn(msg)
		}, nil
	case "ERROR":
		return func(logger log.Modular, msg string) {
			logger.Error(msg)
		}, nil
	}
	return nil, fmt.Errorf("log level not recognised: %v", level)
}

func (l *logProcessor) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	_ = msg.Iter(func(i int, _ *message.Part) error {
		targetLog := l.logger
		if l.fieldsMapping != nil {
			fieldsMsg, err := l.fieldsMapping.MapPart(i, msg)
			if err != nil {
				l.logger.Error("Failed to execute fields mapping: %v", err)
				return nil
			}

			v, err := fieldsMsg.AsStructured()
			if err != nil {
				l.logger.Error("Failed to extract fields object: %v", err)
				return nil
			}

			vObj, ok := v.(map[string]any)
			if !ok {
				l.logger.Error("Fields mapping yielded a non-object result: %T", v)
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
					l.logger.Error("Field %v interpolation error: %v", k, err)
					return nil
				}
			}
			targetLog = targetLog.WithFields(interpFields)
		}
		logMsg, err := l.message.String(i, msg)
		if err != nil {
			l.logger.Error("Message interpolation error: %v", err)
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
