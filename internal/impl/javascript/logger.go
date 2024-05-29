package javascript

import "github.com/redpanda-data/benthos/v4/public/service"

// Logger wraps the service.Logger so that we can define the below methods.
type Logger struct {
	l *service.Logger
}

// Log will be used for "console.log()" in JS
func (l *Logger) Log(message string) {
	l.l.Info(message)
}

// Warn will be used for "console.warn()" in JS
func (l *Logger) Warn(message string) {
	l.l.Warn(message)
}

// Error will be used for "console.error()" in JS
func (l *Logger) Error(message string) {
	l.l.Error(message)
}
