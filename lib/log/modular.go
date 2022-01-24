package log

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Logger level constants
const (
	LogOff   int = 0
	LogFatal int = 1
	LogError int = 2
	LogWarn  int = 3
	LogInfo  int = 4
	LogDebug int = 5
	LogTrace int = 6
	LogAll   int = 7
)

// intToLogLevel converts an integer into a human readable log level.
func intToLogLevel(i int) string {
	switch i {
	case LogOff:
		return "OFF"
	case LogFatal:
		return "FATAL"
	case LogError:
		return "ERROR"
	case LogWarn:
		return "WARN"
	case LogInfo:
		return "INFO"
	case LogDebug:
		return "DEBUG"
	case LogTrace:
		return "TRACE"
	case LogAll:
		return "ALL"
	}
	return "ALL"
}

// logLevelToInt converts a human readable log level into an integer value.
func logLevelToInt(level string) int {
	levelUpper := strings.ToUpper(level)
	switch levelUpper {
	case "OFF":
		return LogOff
	case "FATAL":
		return LogFatal
	case "ERROR":
		return LogError
	case "WARN":
		return LogWarn
	case "INFO":
		return LogInfo
	case "DEBUG":
		return LogDebug
	case "TRACE":
		return LogTrace
	case "ALL":
		return LogAll
	}
	return -1
}

//------------------------------------------------------------------------------

// Config holds configuration options for a logger object.
type Config struct {
	Prefix       string            `json:"prefix" yaml:"prefix"`
	LogLevel     string            `json:"level" yaml:"level"`
	Format       string            `json:"format" yaml:"format"`
	AddTimeStamp bool              `json:"add_timestamp" yaml:"add_timestamp"`
	JSONFormat   bool              `json:"json_format" yaml:"json_format"`
	StaticFields map[string]string `json:"static_fields" yaml:"static_fields"`
}

// NewConfig returns a config struct with the default values for each field.
func NewConfig() Config {
	return Config{
		Prefix:       "benthos",
		LogLevel:     "INFO",
		Format:       "json",
		AddTimeStamp: true,
		JSONFormat:   true,
		StaticFields: map[string]string{
			"@service": "benthos",
		},
	}
}

//------------------------------------------------------------------------------

// UnmarshalJSON ensures that when parsing configs that are in a slice the
// default values are still applied.
func (conf *Config) UnmarshalJSON(bytes []byte) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	defaultFields := aliased.StaticFields
	aliased.StaticFields = nil
	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	if aliased.StaticFields == nil {
		aliased.StaticFields = defaultFields
	}

	*conf = Config(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (conf *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	defaultFields := aliased.StaticFields
	aliased.StaticFields = nil

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	if aliased.StaticFields == nil {
		aliased.StaticFields = defaultFields
	}

	*conf = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

// Logger is an object with support for levelled logging and modular components.
type Logger struct {
	stream       io.Writer
	prefix       string
	fields       map[string]interface{}
	format       string
	addTimestamp bool
	level        int
	formatter    logFormatter
}

// New creates and returns a new logger object.
// TODO: V4 replace this with NewV2
func New(stream io.Writer, config Config) Modular {
	if !config.JSONFormat {
		config.Format = "deprecated"
	}
	if len(config.Prefix) > 0 {
		config.StaticFields["component"] = strings.TrimLeft(config.Prefix, ".")
	}

	fields := map[string]interface{}{}
	for k, v := range config.StaticFields {
		fields[k] = v
	}

	logger := Logger{
		stream:       stream,
		prefix:       config.Prefix,
		fields:       fields,
		format:       config.Format,
		addTimestamp: config.AddTimeStamp,
		level:        logLevelToInt(config.LogLevel),
	}

	logger.formatter, _ = getFormatter(config.Format, config.Prefix, config.AddTimeStamp, fields)
	if logger.formatter == nil {
		logger.formatter = deprecatedFormatter(config.Prefix, config.AddTimeStamp)
	}
	return &logger
}

// NewV2 returns a new logger from a config, or returns an error if the config
// is invalid.
func NewV2(stream io.Writer, config Config) (Modular, error) {
	if !config.JSONFormat {
		config.Format = "deprecated"
	}
	if len(config.Prefix) > 0 {
		config.StaticFields["component"] = strings.TrimLeft(config.Prefix, ".")
	}

	fields := map[string]interface{}{}
	for k, v := range config.StaticFields {
		fields[k] = v
	}

	logger := Logger{
		stream:       stream,
		prefix:       config.Prefix,
		fields:       fields,
		format:       config.Format,
		addTimestamp: config.AddTimeStamp,
		level:        logLevelToInt(config.LogLevel),
	}

	var err error
	if logger.formatter, err = getFormatter(config.Format, config.Prefix, config.AddTimeStamp, fields); err != nil {
		return nil, err
	}
	return &logger, nil
}

//------------------------------------------------------------------------------

// Noop creates and returns a new logger object that writes nothing.
func Noop() Modular {
	return &Logger{
		stream:       io.Discard,
		prefix:       "benthos",
		fields:       map[string]interface{}{},
		level:        LogOff,
		format:       "deprecated",
		addTimestamp: true,
		formatter:    deprecatedFormatter("benthos", true),
	}
}

// NewModule creates a new logger object from the previous, using the same
// configuration, but adds an extra prefix to represent a submodule.
func (l *Logger) NewModule(name string) Modular {
	newPrefix := fmt.Sprintf("%v%v", l.prefix, name)
	newFields := make(map[string]interface{}, len(l.fields))
	for k, v := range l.fields {
		if k == "component" {
			if str, ok := v.(string); ok {
				newFields[k] = strings.TrimLeft(str+name, ".")
			} else {
				newFields[k] = v
			}
		} else {
			newFields[k] = v
		}
	}

	formatter, _ := getFormatter(l.format, newPrefix, l.addTimestamp, newFields)
	if formatter == nil {
		formatter = deprecatedFormatter(newPrefix, l.addTimestamp)
	}

	return &Logger{
		stream:       l.stream,
		prefix:       newPrefix,
		fields:       newFields,
		level:        l.level,
		format:       l.format,
		addTimestamp: l.addTimestamp,
		formatter:    formatter,
	}
}

// WithFields returns a logger with new fields added to the JSON formatted
// output.
func (l *Logger) WithFields(inboundFields map[string]string) Modular {
	newFields := make(map[string]interface{}, len(l.fields))
	for k, v := range l.fields {
		newFields[k] = v
	}
	for k, v := range inboundFields {
		newFields[k] = v
	}

	formatter, _ := getFormatter(l.format, l.prefix, l.addTimestamp, newFields)
	if formatter == nil {
		formatter = deprecatedFormatter(l.prefix, l.addTimestamp)
	}

	return &Logger{
		stream:       l.stream,
		prefix:       l.prefix,
		fields:       newFields,
		level:        l.level,
		format:       l.format,
		addTimestamp: l.addTimestamp,
		formatter:    formatter,
	}
}

// WithFields attempts to cast the Modular implementation into an interface that
// implements WithFields, and if successful returns the result.
func WithFields(l Modular, fields map[string]string) Modular {
	return l.WithFields(fields)
}

// With returns a logger with new fields.
func (l *Logger) With(args ...interface{}) Modular {
	newFields := make(map[string]interface{}, len(l.fields))
	for k, v := range l.fields {
		newFields[k] = v
	}
	for i := 0; i < (len(args) - 1); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}
		newFields[key] = args[i+1]
	}

	formatter, _ := getFormatter(l.format, l.prefix, l.addTimestamp, newFields)
	if formatter == nil {
		formatter = deprecatedFormatter(l.prefix, l.addTimestamp)
	}

	return &Logger{
		stream:       l.stream,
		prefix:       l.prefix,
		fields:       newFields,
		level:        l.level,
		format:       l.format,
		addTimestamp: l.addTimestamp,
		formatter:    formatter,
	}
}

// With attempts to cast the Modular implementation into an interface that
// implements With, and if successful returns the result.
func With(l Modular, args ...interface{}) (Modular, error) {
	if m, ok := l.(interface {
		With(args ...interface{}) Modular
	}); ok {
		return m.With(args...), nil
	}
	return nil, errors.New("the logger does not support typed fields")
}

//------------------------------------------------------------------------------

type logFormatter func(w io.Writer, message string, level string, other ...interface{})

func jsonFormatter(addTimestamp bool, fields map[string]interface{}) logFormatter {
	var staticFieldsRawJSON string
	if len(fields) > 0 {
		jBytes, _ := json.Marshal(fields)
		if len(jBytes) > 2 {
			staticFieldsRawJSON = string(jBytes[1:len(jBytes)-1]) + ","
		}
	}

	return func(w io.Writer, message string, level string, other ...interface{}) {
		message = strings.TrimSuffix(message, "\n")
		timestampStr := ""
		if addTimestamp {
			timestampStr = fmt.Sprintf("\"@timestamp\":\"%v\",", time.Now().Format(time.RFC3339))
		}
		fmt.Fprintf(
			w,
			"{%v%v\"level\":\"%v\",\"message\":%v}\n",
			timestampStr, staticFieldsRawJSON, level,
			strconv.QuoteToASCII(fmt.Sprintf(message, other...)),
		)
	}
}

func logfmtFormatter(addTimestamp bool, fields map[string]interface{}) logFormatter {
	var staticFieldsRaw string
	if len(fields) > 0 {
		keys := make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var buf bytes.Buffer
		for _, k := range keys {
			v := fields[k]
			vStr, ok := v.(string)
			if ok {
				if strings.Contains(vStr, " ") {
					vStr = strconv.QuoteToASCII(vStr)
				}
				v = vStr
			}
			buf.WriteString(fmt.Sprintf("%v=%v ", k, v))
		}
		staticFieldsRaw = buf.String()
	}

	return func(w io.Writer, message string, level string, other ...interface{}) {
		message = strings.TrimSuffix(message, "\n")
		timestampStr := ""
		if addTimestamp {
			timestampStr = fmt.Sprintf("timestamp=\"%v\" ", time.Now().Format(time.RFC3339))
		}
		fmt.Fprintf(
			w,
			"%v%vlevel=%v msg=%v\n",
			timestampStr, staticFieldsRaw, level,
			strconv.QuoteToASCII(fmt.Sprintf(message, other...)),
		)
	}
}

func deprecatedFormatter(component string, addTimestamp bool) logFormatter {
	return func(w io.Writer, message string, level string, other ...interface{}) {
		if !strings.HasSuffix(message, "\n") {
			message += "\n"
		}
		timestampStr := ""
		if addTimestamp {
			timestampStr = fmt.Sprintf("%v | ", time.Now().Format(time.RFC3339))
		}
		fmt.Fprintf(w, fmt.Sprintf(
			"%v%v | %v | %v",
			timestampStr, level, component, message,
		), other...)
	}
}

func getFormatter(format, component string, addTimestamp bool, fields map[string]interface{}) (logFormatter, error) {
	switch format {
	case "json":
		return jsonFormatter(addTimestamp, fields), nil
	case "logfmt":
		return logfmtFormatter(addTimestamp, fields), nil
	case "deprecated", "classic":
		return deprecatedFormatter(component, addTimestamp), nil
	}
	return nil, fmt.Errorf("log format '%v' not recognized", format)
}

// write prints a log message with any configured extras prepended.
func (l *Logger) write(message, level string, other ...interface{}) {
	l.formatter(l.stream, message, level, other...)
}

//------------------------------------------------------------------------------

// Fatalf prints a fatal message to the console. Does NOT cause panic.
func (l *Logger) Fatalf(format string, v ...interface{}) {
	if LogFatal <= l.level {
		l.write(format, "FATAL", v...)
	}
}

// Errorf prints an error message to the console.
func (l *Logger) Errorf(format string, v ...interface{}) {
	if LogError <= l.level {
		l.write(format, "ERROR", v...)
	}
}

// Warnf prints a warning message to the console.
func (l *Logger) Warnf(format string, v ...interface{}) {
	if LogWarn <= l.level {
		l.write(format, "WARN", v...)
	}
}

// Infof prints an information message to the console.
func (l *Logger) Infof(format string, v ...interface{}) {
	if LogInfo <= l.level {
		l.write(format, "INFO", v...)
	}
}

// Debugf prints a debug message to the console.
func (l *Logger) Debugf(format string, v ...interface{}) {
	if LogDebug <= l.level {
		l.write(format, "DEBUG", v...)
	}
}

// Tracef prints a trace message to the console.
func (l *Logger) Tracef(format string, v ...interface{}) {
	if LogTrace <= l.level {
		l.write(format, "TRACE", v...)
	}
}

//------------------------------------------------------------------------------

// Fatalln prints a fatal message to the console. Does NOT cause panic.
func (l *Logger) Fatalln(message string) {
	if LogFatal <= l.level {
		l.write(message, "FATAL")
	}
}

// Errorln prints an error message to the console.
func (l *Logger) Errorln(message string) {
	if LogError <= l.level {
		l.write(message, "ERROR")
	}
}

// Warnln prints a warning message to the console.
func (l *Logger) Warnln(message string) {
	if LogWarn <= l.level {
		l.write(message, "WARN")
	}
}

// Infoln prints an information message to the console.
func (l *Logger) Infoln(message string) {
	if LogInfo <= l.level {
		l.write(message, "INFO")
	}
}

// Debugln prints a debug message to the console.
func (l *Logger) Debugln(message string) {
	if LogDebug <= l.level {
		l.write(message, "DEBUG")
	}
}

// Traceln prints a trace message to the console.
func (l *Logger) Traceln(message string) {
	if LogTrace <= l.level {
		l.write(message, "TRACE")
	}
}

//------------------------------------------------------------------------------
