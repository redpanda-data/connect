package studio

import (
	"sync/atomic"

	"github.com/benthosdev/benthos/v4/internal/log"
)

var _ log.Modular = &hotSwapLogger{}

type hotSwapLogger struct {
	lPtr atomic.Pointer[log.Modular]
}

func (h *hotSwapLogger) swap(l log.Modular) {
	h.lPtr.Store(&l)
}

func (h *hotSwapLogger) WithFields(fields map[string]string) log.Modular {
	if len(fields) == 0 {
		return h
	}

	return (*h.lPtr.Load()).WithFields(fields)
}

func (h *hotSwapLogger) With(keyValues ...any) log.Modular {
	return (*h.lPtr.Load()).With(keyValues...)
}

func (h *hotSwapLogger) Fatal(format string, v ...any) {
	(*h.lPtr.Load()).Fatal(format, v...)
}

func (h *hotSwapLogger) Error(format string, v ...any) {
	(*h.lPtr.Load()).Error(format, v...)
}

func (h *hotSwapLogger) Warn(format string, v ...any) {
	(*h.lPtr.Load()).Warn(format, v...)
}

func (h *hotSwapLogger) Info(format string, v ...any) {
	(*h.lPtr.Load()).Info(format, v...)
}

func (h *hotSwapLogger) Debug(format string, v ...any) {
	(*h.lPtr.Load()).Debug(format, v...)
}

func (h *hotSwapLogger) Trace(format string, v ...any) {
	(*h.lPtr.Load()).Trace(format, v...)
}
