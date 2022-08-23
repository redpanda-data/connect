package javascript

import "github.com/benthosdev/benthos/v4/public/service"

type Logger struct {
	l *service.Logger
}

func (l *Logger) Log(message string) {
	l.l.Info(message)
}

func (l *Logger) Warn(message string) {
	l.l.Warn(message)
}

func (l *Logger) Error(message string) {
	l.l.Error(message)
}
