package log

import (
	"os"

	"log/slog"
)

type logHandler struct {
	slog *slog.Logger
}

func NewBenthosLogAdapter(l *slog.Logger) *logHandler {
	return &logHandler{slog: l}
}

func (l *logHandler) WithFields(fields map[string]string) Modular {
	attrs := make([]slog.Attr, len(fields))
	for k, v := range fields {
		attrs = append(attrs, slog.String(k, v))
	}

	c := l.clone()
	c.slog = l.slog.With(attrs)
	return c
}

func (l *logHandler) With(keyValues ...any) Modular {
	c := l.clone()
	c.slog = l.slog.With(keyValues...)
	return c
}

func (l *logHandler) Fatalf(format string, v ...any) {
	l.slog.Error(format, v...)
	os.Exit(1)
}

func (l *logHandler) Errorf(format string, v ...any) {
	l.slog.Error(format, v...)
}

func (l *logHandler) Warnf(format string, v ...any) {
	l.slog.Warn(format, v...)
}

func (l *logHandler) Infof(format string, v ...any) {
	l.slog.Info(format, v...)
}

func (l *logHandler) Debugf(format string, v ...any) {
	l.slog.Debug(format, v)
}

func (l *logHandler) Tracef(format string, v ...any) {
	l.slog.Debug(format, v...)
}

func (l *logHandler) Fatalln(message string) {
	l.slog.Error(message)
	os.Exit(1)
}

func (l *logHandler) Errorln(message string) {
	l.slog.Error(message)
}

func (l *logHandler) Warnln(message string) {
	l.slog.Warn(message)
}

func (l *logHandler) Infoln(message string) {
	l.slog.Info(message)
}

func (l *logHandler) Debugln(message string) {
	l.slog.Debug(message)
}

func (l *logHandler) Traceln(message string) {
	l.slog.Debug(message)
}

func (l *logHandler) clone() *logHandler {
	c := *l
	return &c
}
