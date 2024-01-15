package log

// Modular is a log printer that allows you to branch new modules.
type Modular interface {
	WithFields(fields map[string]string) Modular
	With(keyValues ...any) Modular

	Fatal(format string, v ...any)
	Error(format string, v ...any)
	Warn(format string, v ...any)
	Info(format string, v ...any)
	Debug(format string, v ...any)
	Trace(format string, v ...any)
}
