package service

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/log"
)

// Logger allows plugin authors to write custom logs from components that are
// exported the same way as native Benthos logs. It's safe to pass around a nil
// pointer for testing components.
type Logger struct {
	m log.Modular
}

func newReverseAirGapLogger(l log.Modular) *Logger {
	return &Logger{l}
}

// Tracef logs a trace message using fmt.Sprintf when args are specified.
func (l *Logger) Tracef(template string, args ...any) {
	if l == nil {
		return
	}
	l.m.Tracef(template, args...)
}

// Trace logs a trace message.
func (l *Logger) Trace(message string) {
	if l == nil {
		return
	}
	l.m.Traceln(message)
}

// Debugf logs a debug message using fmt.Sprintf when args are specified.
func (l *Logger) Debugf(template string, args ...any) {
	if l == nil {
		return
	}
	l.m.Debugf(template, args...)
}

// Debug logs a debug message.
func (l *Logger) Debug(message string) {
	if l == nil {
		return
	}
	l.m.Debugln(message)
}

// Infof logs an info message using fmt.Sprintf when args are specified.
func (l *Logger) Infof(template string, args ...any) {
	if l == nil {
		return
	}
	l.m.Infof(template, args...)
}

// Info logs an info message.
func (l *Logger) Info(message string) {
	if l == nil {
		return
	}
	l.m.Infoln(message)
}

// Warnf logs a warning message using fmt.Sprintf when args are specified.
func (l *Logger) Warnf(template string, args ...any) {
	if l == nil {
		return
	}
	l.m.Warnf(template, args...)
}

// Warn logs a warning message.
func (l *Logger) Warn(message string) {
	if l == nil {
		return
	}
	l.m.Warnln(message)
}

// Errorf logs an error message using fmt.Sprintf when args are specified.
func (l *Logger) Errorf(template string, args ...any) {
	if l == nil {
		return
	}
	l.m.Errorf(template, args...)
}

// Error logs an error message.
func (l *Logger) Error(message string) {
	if l == nil {
		return
	}
	l.m.Errorln(message)
}

// With adds a variadic set of fields to a logger. Each field must consist
// of a string key and a value of any type. An odd number of key/value pairs
// will therefore result in malformed log messages, but should never panic.
func (l *Logger) With(keyValuePairs ...any) *Logger {
	if l == nil {
		return nil
	}
	fields := map[string]string{}
	for i := 0; i < (len(keyValuePairs) - 1); i += 2 {
		key, ok := keyValuePairs[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", keyValuePairs[i])
		}
		value, ok := keyValuePairs[i+1].(string)
		if !ok {
			value = fmt.Sprintf("%v", keyValuePairs[i+1])
		}
		fields[key] = value
	}
	lg := l.m.WithFields(fields)
	return &Logger{lg}
}
