package log

type teeLogger struct {
	a, b Modular
}

func TeeLogger(a, b Modular) Modular {
	return &teeLogger{a: a, b: b}
}

func (t *teeLogger) WithFields(fields map[string]string) Modular {
	return &teeLogger{
		a: t.a.WithFields(fields),
		b: t.b.WithFields(fields),
	}
}

func (t *teeLogger) With(keyValues ...any) Modular {
	return &teeLogger{
		a: t.a.With(keyValues...),
		b: t.b.With(keyValues...),
	}
}

func (t *teeLogger) Fatal(format string, v ...any) {
	t.a.Fatal(format, v...)
	t.b.Fatal(format, v...)
}

func (t *teeLogger) Error(format string, v ...any) {
	t.a.Error(format, v...)
	t.b.Error(format, v...)
}

func (t *teeLogger) Warn(format string, v ...any) {
	t.a.Warn(format, v...)
	t.b.Warn(format, v...)
}

func (t *teeLogger) Info(format string, v ...any) {
	t.a.Info(format, v...)
	t.b.Info(format, v...)
}

func (t *teeLogger) Debug(format string, v ...any) {
	t.a.Debug(format, v...)
	t.b.Debug(format, v...)
}

func (t *teeLogger) Trace(format string, v ...any) {
	t.a.Trace(format, v...)
	t.b.Trace(format, v...)
}
