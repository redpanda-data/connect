package log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

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

// Sanitised returns a sanitised version of the config, meaning sections that
// aren't relevant to behaviour are removed. Also optionally removes deprecated
// fields.
func (conf Config) Sanitised(removeDeprecated bool) (interface{}, error) {
	cBytes, err := yaml.Marshal(conf)
	if err != nil {
		return nil, err
	}

	hashMap := map[string]interface{}{}
	if err = yaml.Unmarshal(cBytes, &hashMap); err != nil {
		return nil, err
	}

	if conf.JSONFormat {
		delete(hashMap, "json_format")
	}

	return hashMap, nil
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
	stream    io.Writer
	config    Config
	level     int
	formatter logFormatter
}

// New creates and returns a new logger object.
// TODO: V4 replace this with NewV2
func New(stream io.Writer, config Config) Modular {
	if !config.JSONFormat {
		config.Format = "deprecated"
	}

	logger := Logger{
		stream: stream,
		config: config,
		level:  logLevelToInt(config.LogLevel),
	}

	logger.formatter, _ = getFormatter(config)
	if logger.formatter == nil {
		logger.formatter = deprecatedFormatter(config)
	}
	return &logger
}

// NewV2 returns a new logger from a config, or returns an error if the config
// is invalid.
func NewV2(stream io.Writer, config Config) (Modular, error) {
	if !config.JSONFormat {
		config.Format = "deprecated"
	}

	logger := Logger{
		stream: stream,
		config: config,
		level:  logLevelToInt(config.LogLevel),
	}

	var err error
	if logger.formatter, err = getFormatter(config); err != nil {
		return nil, err
	}
	return &logger, nil
}

//------------------------------------------------------------------------------

// Noop creates and returns a new logger object that writes nothing.
func Noop() Modular {
	return &Logger{
		stream:    ioutil.Discard,
		config:    NewConfig(),
		level:     LogOff,
		formatter: deprecatedFormatter(NewConfig()),
	}
}

// NewModule creates a new logger object from the previous, using the same
// configuration, but adds an extra prefix to represent a submodule.
func (l *Logger) NewModule(prefix string) Modular {
	config := l.config
	config.Prefix = fmt.Sprintf("%v%v", config.Prefix, prefix)

	formatter, _ := getFormatter(config)
	if formatter == nil {
		formatter = deprecatedFormatter(config)
	}

	return &Logger{
		stream:    l.stream,
		config:    config,
		level:     l.level,
		formatter: formatter,
	}
}

// WithFields returns a logger with new fields added to the JSON formatted
// output.
func (l *Logger) WithFields(fields map[string]string) Modular {
	newConfig := l.config
	newConfig.StaticFields = fields
	for k, v := range l.config.StaticFields {
		if _, exists := fields[k]; !exists {
			newConfig.StaticFields[k] = v
		}
	}

	formatter, _ := getFormatter(newConfig)
	if formatter == nil {
		formatter = deprecatedFormatter(newConfig)
	}

	return &Logger{
		stream:    l.stream,
		config:    newConfig,
		level:     l.level,
		formatter: formatter,
	}
}

// WithFields attempts to cast the Modular implementation into an interface that
// implements WithFields, and if successful returns the result.
func WithFields(l Modular, fields map[string]string) Modular {
	return l.WithFields(fields)
}

//------------------------------------------------------------------------------

type logFormatter func(w io.Writer, message string, level string, other ...interface{})

func jsonFormatter(conf Config) logFormatter {
	var staticFieldsRawJSON string
	if len(conf.StaticFields) > 0 {
		jBytes, _ := json.Marshal(conf.StaticFields)
		if len(jBytes) > 2 {
			staticFieldsRawJSON = string(jBytes[1:len(jBytes)-1]) + ","
		}
	}

	return func(w io.Writer, message string, level string, other ...interface{}) {
		message = strings.TrimSuffix(message, "\n")
		timestampStr := ""
		if conf.AddTimeStamp {
			timestampStr = fmt.Sprintf("\"@timestamp\":\"%v\",", time.Now().Format(time.RFC3339))
		}
		fmt.Fprintf(
			w,
			"{%v%v\"level\":\"%v\",\"component\":\"%v\",\"message\":%v}\n",
			timestampStr, staticFieldsRawJSON, level, conf.Prefix,
			strconv.QuoteToASCII(fmt.Sprintf(message, other...)),
		)
	}
}

func logfmtFormatter(conf Config) logFormatter {
	var staticFieldsRaw string
	if len(conf.StaticFields) > 0 {
		keys := make([]string, 0, len(conf.StaticFields))
		for k := range conf.StaticFields {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var buf bytes.Buffer
		for _, k := range keys {
			buf.WriteString(fmt.Sprintf("%v=%v ", k, conf.StaticFields[k]))
		}
		staticFieldsRaw = buf.String()
	}

	return func(w io.Writer, message string, level string, other ...interface{}) {
		message = strings.TrimSuffix(message, "\n")
		timestampStr := ""
		if conf.AddTimeStamp {
			timestampStr = fmt.Sprintf("timestamp=\"%v\" ", time.Now().Format(time.RFC3339))
		}
		fmt.Fprintf(
			w,
			"%v%vlevel=%v component=%v msg=%v\n",
			timestampStr, staticFieldsRaw, level, conf.Prefix,
			strconv.QuoteToASCII(fmt.Sprintf(message, other...)),
		)
	}
}

func deprecatedFormatter(conf Config) logFormatter {
	return func(w io.Writer, message string, level string, other ...interface{}) {
		if !strings.HasSuffix(message, "\n") {
			message = message + "\n"
		}
		timestampStr := ""
		if conf.AddTimeStamp {
			timestampStr = fmt.Sprintf("%v | ", time.Now().Format(time.RFC3339))
		}
		fmt.Fprintf(w, fmt.Sprintf(
			"%v%v | %v | %v",
			timestampStr, level, conf.Prefix, message,
		), other...)
	}
}

func getFormatter(conf Config) (logFormatter, error) {
	switch conf.Format {
	case "json":
		return jsonFormatter(conf), nil
	case "logfmt":
		return logfmtFormatter(conf), nil
	case "deprecated", "classic":
		return deprecatedFormatter(conf), nil
	}
	return nil, fmt.Errorf("log format '%v' not recognized", conf.Format)
}

// write prints a log message with any configured extras prepended.
func (l *Logger) write(message string, level string, other ...interface{}) {
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
