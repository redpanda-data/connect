package log

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/sirupsen/logrus"
)

// Config holds configuration options for a logger object.
type Config struct {
	LogLevel      string            `json:"level" yaml:"level"`
	Format        string            `json:"format" yaml:"format"`
	AddTimeStamp  bool              `json:"add_timestamp" yaml:"add_timestamp"`
	MessageName   string            `json:"message_name" yaml:"message_name"`
	TimestampName string            `json:"timestamp_name" yaml:"timestamp_name"`
	StaticFields  map[string]string `json:"static_fields" yaml:"static_fields"`
	File          File              `json:"file" yaml:"file"`
}

// File contains configuration for file based logging.
type File struct {
	Path         string `json:"path" yaml:"path"`
	Rotate       bool   `json:"rotate" yaml:"rotate"`
	RotateMaxAge int    `json:"rotate_max_age_days" yaml:"rotate_max_age_days"`
}

// NewConfig returns a config struct with the default values for each field.
func NewConfig() Config {
	return Config{
		LogLevel:      "INFO",
		Format:        "logfmt",
		AddTimeStamp:  false,
		TimestampName: "time",
		MessageName:   "msg",
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
func (conf *Config) UnmarshalYAML(unmarshal func(any) error) error {
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
	entry *logrus.Entry
}

// NewV2 returns a new logger from a config, or returns an error if the config
// is invalid.
func NewV2(stream io.Writer, config Config) (Modular, error) {
	logger := logrus.New()
	logger.Out = stream

	switch config.Format {
	case "json":
		logger.SetFormatter(&logrus.JSONFormatter{
			DisableTimestamp: !config.AddTimeStamp,
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime: config.TimestampName,
				logrus.FieldKeyMsg:  config.MessageName,
			},
		})
	case "logfmt":
		logger.SetFormatter(&logrus.TextFormatter{
			DisableTimestamp: !config.AddTimeStamp,
			QuoteEmptyFields: true,
			FullTimestamp:    config.AddTimeStamp,
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime: config.TimestampName,
				logrus.FieldKeyMsg:  config.MessageName,
			},
		})
	default:
		return nil, fmt.Errorf("log format '%v' not recognized", config.Format)
	}

	switch strings.ToUpper(config.LogLevel) {
	case "OFF", "NONE":
		logger.Level = logrus.PanicLevel
	case "FATAL":
		logger.Level = logrus.FatalLevel
	case "ERROR":
		logger.Level = logrus.ErrorLevel
	case "WARN":
		logger.Level = logrus.WarnLevel
	case "INFO":
		logger.Level = logrus.InfoLevel
	case "DEBUG":
		logger.Level = logrus.DebugLevel
	case "TRACE", "ALL":
		logger.Level = logrus.TraceLevel
		logger.Level = logrus.TraceLevel
	}

	sFields := logrus.Fields{}
	for k, v := range config.StaticFields {
		sFields[k] = v
	}
	logEntry := logger.WithFields(sFields)

	return &Logger{entry: logEntry}, nil
}

//------------------------------------------------------------------------------

// Noop creates and returns a new logger object that writes nothing.
func Noop() Modular {
	logger := logrus.New()
	logger.Out = io.Discard
	return &Logger{entry: logger.WithFields(logrus.Fields{})}
}

// WithFields returns a logger with new fields added to the JSON formatted
// output.
func (l *Logger) WithFields(inboundFields map[string]string) Modular {
	newFields := make(logrus.Fields, len(inboundFields))
	for k, v := range inboundFields {
		newFields[k] = v
	}

	newLogger := *l
	newLogger.entry = l.entry.WithFields(newFields)
	return &newLogger
}

// With returns a copy of the logger with new labels added to the logging
// context.
func (l *Logger) With(keyValues ...any) Modular {
	newEntry := l.entry.WithFields(logrus.Fields{})
	for i := 0; i < (len(keyValues) - 1); i += 2 {
		key, ok := keyValues[i].(string)
		if !ok {
			continue
		}
		newEntry = newEntry.WithField(key, keyValues[i+1])
	}

	newLogger := *l
	newLogger.entry = newEntry
	return &newLogger
}

//------------------------------------------------------------------------------

// Fatalf prints a fatal message to the console. Does NOT cause panic.
func (l *Logger) Fatalf(format string, v ...any) {
	l.entry.Fatalf(strings.TrimSuffix(format, "\n"), v...)
}

// Errorf prints an error message to the console.
func (l *Logger) Errorf(format string, v ...any) {
	l.entry.Errorf(strings.TrimSuffix(format, "\n"), v...)
}

// Warnf prints a warning message to the console.
func (l *Logger) Warnf(format string, v ...any) {
	l.entry.Warnf(strings.TrimSuffix(format, "\n"), v...)
}

// Infof prints an information message to the console.
func (l *Logger) Infof(format string, v ...any) {
	l.entry.Infof(strings.TrimSuffix(format, "\n"), v...)
}

// Debugf prints a debug message to the console.
func (l *Logger) Debugf(format string, v ...any) {
	l.entry.Debugf(strings.TrimSuffix(format, "\n"), v...)
}

// Tracef prints a trace message to the console.
func (l *Logger) Tracef(format string, v ...any) {
	l.entry.Tracef(strings.TrimSuffix(format, "\n"), v...)
}

//------------------------------------------------------------------------------

// Fatalln prints a fatal message to the console. Does NOT cause panic.
func (l *Logger) Fatalln(message string) {
	l.entry.Fatalln(message)
}

// Errorln prints an error message to the console.
func (l *Logger) Errorln(message string) {
	l.entry.Errorln(message)
}

// Warnln prints a warning message to the console.
func (l *Logger) Warnln(message string) {
	l.entry.Warnln(message)
}

// Infoln prints an information message to the console.
func (l *Logger) Infoln(message string) {
	l.entry.Infoln(message)
}

// Debugln prints a debug message to the console.
func (l *Logger) Debugln(message string) {
	l.entry.Debugln(message)
}

// Traceln prints a trace message to the console.
func (l *Logger) Traceln(message string) {
	l.entry.Traceln(message)
}
