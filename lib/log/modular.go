package log

import (
	"bytes"
	"encoding/json"
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
	LogLevel     string            `json:"level" yaml:"level"`
	Format       string            `json:"format" yaml:"format"`
	AddTimeStamp bool              `json:"add_timestamp" yaml:"add_timestamp"`
	StaticFields map[string]string `json:"static_fields" yaml:"static_fields"`
}

// NewConfig returns a config struct with the default values for each field.
func NewConfig() Config {
	return Config{
		LogLevel:     "INFO",
		Format:       "logfmt",
		AddTimeStamp: false,
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
	fields       map[string]interface{}
	format       string
	addTimestamp bool
	level        int
	formatter    logFormatter
}

// NewV2 returns a new logger from a config, or returns an error if the config
// is invalid.
func NewV2(stream io.Writer, config Config) (Modular, error) {
	fields := map[string]interface{}{}
	for k, v := range config.StaticFields {
		fields[k] = v
	}

	logger := Logger{
		stream:       stream,
		fields:       fields,
		format:       config.Format,
		addTimestamp: config.AddTimeStamp,
		level:        logLevelToInt(config.LogLevel),
	}

	var err error
	if logger.formatter, err = getFormatter(config.Format, config.AddTimeStamp, fields); err != nil {
		return nil, err
	}
	return &logger, nil
}

//------------------------------------------------------------------------------

// Noop creates and returns a new logger object that writes nothing.
func Noop() Modular {
	return &Logger{
		stream:       io.Discard,
		fields:       map[string]interface{}{},
		level:        LogOff,
		format:       "logfmt",
		addTimestamp: true,
		formatter:    logfmtFormatter(false, nil),
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

	formatter, _ := getFormatter(l.format, l.addTimestamp, newFields)
	return &Logger{
		stream:       l.stream,
		fields:       newFields,
		level:        l.level,
		format:       l.format,
		addTimestamp: l.addTimestamp,
		formatter:    formatter,
	}
}

// With returns a copy of the logger with new labels added to the logging
// context.
func (l *Logger) With(keyValues ...interface{}) Modular {
	newFields := make(map[string]interface{}, len(l.fields))
	for k, v := range l.fields {
		newFields[k] = v
	}
	for i := 0; i < (len(keyValues) - 1); i += 2 {
		key, ok := keyValues[i].(string)
		if !ok {
			continue
		}
		newFields[key] = keyValues[i+1]
	}

	formatter, _ := getFormatter(l.format, l.addTimestamp, newFields)
	return &Logger{
		stream:       l.stream,
		fields:       newFields,
		level:        l.level,
		format:       l.format,
		addTimestamp: l.addTimestamp,
		formatter:    formatter,
	}
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

func getFormatter(format string, addTimestamp bool, fields map[string]interface{}) (logFormatter, error) {
	switch format {
	case "json":
		return jsonFormatter(addTimestamp, fields), nil
	case "logfmt":
		return logfmtFormatter(addTimestamp, fields), nil
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
