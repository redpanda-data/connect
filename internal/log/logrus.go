package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

// Logger is an object with support for levelled logging and modular components.
type Logger struct {
	entry *logrus.Entry
}

// New returns a new logger from a config, or returns an error if the config
// is invalid.
func New(stream io.Writer, fs ifs.FS, config Config) (Modular, error) {
	if config.File.Path != "" {
		if config.File.Rotate {
			stream = &lumberjack.Logger{
				Filename:   config.File.Path,
				MaxSize:    10,
				MaxAge:     config.File.RotateMaxAge,
				MaxBackups: 1,
				Compress:   true,
			}
		} else {
			fw, err := ifs.OS().OpenFile(config.File.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
			if err == nil {
				var isw bool
				if stream, isw = fw.(io.Writer); !isw {
					err = errors.New("failed to open a writeable file")
				}
			}
			if err != nil {
				return nil, err
			}
		}
	}

	logger := logrus.New()
	logger.Out = stream

	switch config.Format {
	case "json":
		logger.SetFormatter(&logrus.JSONFormatter{
			DisableTimestamp: !config.AddTimeStamp,
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  config.TimestampName,
				logrus.FieldKeyMsg:   config.MessageName,
				logrus.FieldKeyLevel: config.LevelName,
			},
		})
	case "logfmt":
		logger.SetFormatter(&logrus.TextFormatter{
			DisableTimestamp: !config.AddTimeStamp,
			QuoteEmptyFields: true,
			FullTimestamp:    config.AddTimeStamp,
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  config.TimestampName,
				logrus.FieldKeyMsg:   config.MessageName,
				logrus.FieldKeyLevel: config.LevelName,
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

// Fatal prints a fatal message to the console. Does NOT cause panic.
func (l *Logger) Fatal(format string, v ...any) {
	l.entry.Fatalf(strings.TrimSuffix(format, "\n"), v...)
}

// Error prints an error message to the console.
func (l *Logger) Error(format string, v ...any) {
	l.entry.Errorf(strings.TrimSuffix(format, "\n"), v...)
}

// Warn prints a warning message to the console.
func (l *Logger) Warn(format string, v ...any) {
	l.entry.Warnf(strings.TrimSuffix(format, "\n"), v...)
}

// Info prints an information message to the console.
func (l *Logger) Info(format string, v ...any) {
	l.entry.Infof(strings.TrimSuffix(format, "\n"), v...)
}

// Debug prints a debug message to the console.
func (l *Logger) Debug(format string, v ...any) {
	l.entry.Debugf(strings.TrimSuffix(format, "\n"), v...)
}

// Trace prints a trace message to the console.
func (l *Logger) Trace(format string, v ...any) {
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
