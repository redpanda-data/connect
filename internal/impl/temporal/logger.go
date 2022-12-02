package temporal

import (
	"github.com/benthosdev/benthos/v4/public/service"
)

// wrappedLogger allows temporal client to use a benthos logger for logging
type wrappedLogger struct {
	logger *service.Logger
}

func (l *wrappedLogger) Debug(msg string, keyvals ...interface{}) {
	l.logger.Debugf(msg, keyvals...)
}
func (l *wrappedLogger) Info(msg string, keyvals ...interface{}) {
	l.logger.Infof(msg, keyvals...)
}
func (l *wrappedLogger) Warn(msg string, keyvals ...interface{}) {
	l.logger.Warnf(msg, keyvals...)
}
func (l *wrappedLogger) Error(msg string, keyvals ...interface{}) {
	l.logger.Errorf(msg, keyvals...)
}
