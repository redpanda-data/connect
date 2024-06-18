package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// KGoLogger wraps a service.Logger with an implementation that works within
// the kgo library.
type KGoLogger struct {
	L *service.Logger
}

// Level returns the logger level.
func (k *KGoLogger) Level() kgo.LogLevel {
	return kgo.LogLevelDebug
}

// Log calls the underlying logger implementation using the appropriate log level.
func (k *KGoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	tmpL := k.L
	if len(keyvals) > 0 {
		tmpL = k.L.With(keyvals...)
	}

	switch level {
	case kgo.LogLevelError:
		tmpL.Error(msg)
	case kgo.LogLevelWarn:
		tmpL.Warn(msg)
	case kgo.LogLevelInfo:
		tmpL.Debug(msg)
	case kgo.LogLevelDebug:
		tmpL.Trace(msg)
	}
}
