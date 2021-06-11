package pulsar

import (
	plog "github.com/apache/pulsar-client-go/pulsar/log"
)

// NoopLogger returns a logger that does nothing.
func NoopLogger() plog.Logger {
	return noopLogger{}
}

type noopLogger struct{}

func (n noopLogger) SubLogger(fields plog.Fields) plog.Logger {
	return n
}

func (n noopLogger) WithFields(fields plog.Fields) plog.Entry {
	return n
}
func (n noopLogger) WithField(name string, value interface{}) plog.Entry {
	return n
}
func (n noopLogger) WithError(err error) plog.Entry {
	return n
}

func (n noopLogger) Debug(args ...interface{}) {}
func (n noopLogger) Info(args ...interface{})  {}
func (n noopLogger) Warn(args ...interface{})  {}
func (n noopLogger) Error(args ...interface{}) {}

func (n noopLogger) Debugf(format string, args ...interface{}) {}
func (n noopLogger) Infof(format string, args ...interface{})  {}
func (n noopLogger) Warnf(format string, args ...interface{})  {}
func (n noopLogger) Errorf(format string, args ...interface{}) {}
