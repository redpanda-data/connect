package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/benthosdev/benthos/v4/public/service"
)

type kgoLogger struct {
	l            *service.Logger
	debugToTrace bool
}

func (k *kgoLogger) Level() kgo.LogLevel {
	return kgo.LogLevelDebug
}

func (k *kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...interface{}) {
	tmpL := k.l
	if len(keyvals) > 0 {
		tmpL = k.l.With(keyvals...)
	}

	switch level {
	case kgo.LogLevelError:
		tmpL.Error(msg)
	case kgo.LogLevelWarn:
		tmpL.Warn(msg)
	case kgo.LogLevelInfo:
		tmpL.Info(msg)
	case kgo.LogLevelDebug:
		if k.debugToTrace {
			tmpL.Trace(msg)
		} else {
			tmpL.Debug(msg)
		}
	}
}
