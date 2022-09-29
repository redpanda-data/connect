package pulsar

import (
	plog "github.com/apache/pulsar-client-go/pulsar/log"

	"github.com/benthosdev/benthos/v4/public/service"
)

// DefaultLogger returns a logger that wraps Benthos Modular logger.
func createDefaultLogger(l *service.Logger) plog.Logger {
	return defaultLogger{
		backend: l,
	}
}

type defaultLogger struct {
	backend *service.Logger
}

func (l defaultLogger) SubLogger(fields plog.Fields) plog.Logger {
	return l
}

func (l defaultLogger) WithFields(fields plog.Fields) plog.Entry {
	return l
}

func (l defaultLogger) WithField(name string, value any) plog.Entry {
	return l
}

func (l defaultLogger) WithError(err error) plog.Entry {
	return l
}

func (l defaultLogger) Debug(args ...any) {
	l.backend.Debugf("%v", args)
}

func (l defaultLogger) Info(args ...any) {
	l.backend.Infof("%v", args)
}

func (l defaultLogger) Warn(args ...any) {
	l.backend.Warnf("%v", args)
}

func (l defaultLogger) Error(args ...any) {
	l.backend.Errorf("%v", args)
}

func (l defaultLogger) Debugf(format string, args ...any) {
	l.backend.Debugf(format, args)
}

func (l defaultLogger) Infof(format string, args ...any) {
	l.backend.Infof(format, args)
}

func (l defaultLogger) Warnf(format string, args ...any) {
	l.backend.Warnf(format, args)
}

func (l defaultLogger) Errorf(format string, args ...any) {
	l.backend.Errorf(format, args)
}

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

func (n noopLogger) WithField(name string, value any) plog.Entry {
	return n
}

func (n noopLogger) WithError(err error) plog.Entry {
	return n
}

func (n noopLogger) Debug(args ...any) {}
func (n noopLogger) Info(args ...any)  {}
func (n noopLogger) Warn(args ...any)  {}
func (n noopLogger) Error(args ...any) {}

func (n noopLogger) Debugf(format string, args ...any) {}
func (n noopLogger) Infof(format string, args ...any)  {}
func (n noopLogger) Warnf(format string, args ...any)  {}
func (n noopLogger) Errorf(format string, args ...any) {}
