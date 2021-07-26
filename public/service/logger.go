package service

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
)

// Logger allows plugin authors to write custom logs from components that are
// exported the same way as native Benthos logs.
type Logger struct {
	m log.Modular
}

func newReverseAirGapLogger(l log.Modular) *Logger {
	return &Logger{l}
}

// Debugf logs a debug message using fmt.Sprintf when args are specified.
func (l *Logger) Debugf(template string, args ...interface{}) {
	if l == nil {
		return
	}
	l.m.Debugf(template, args...)
}

// Infof logs an info message using fmt.Sprintf when args are specified.
func (l *Logger) Infof(template string, args ...interface{}) {
	if l == nil {
		return
	}
	l.m.Infof(template, args...)
}

// Warnf logs a warning message using fmt.Sprintf when args are specified.
func (l *Logger) Warnf(template string, args ...interface{}) {
	if l == nil {
		return
	}
	l.m.Warnf(template, args...)
}

// Errorf logs an error message using fmt.Sprintf when args are specified.
func (l *Logger) Errorf(template string, args ...interface{}) {
	if l == nil {
		return
	}
	l.m.Errorf(template, args...)
}

// With adds a variadic set of fields to a logger. Each field must consist
// of a string key and a value of any type. An odd number of key/value pairs
// will therefore result in malformed log messages, but should never panic.
func (l *Logger) With(keyValuePairs ...interface{}) *Logger {
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
