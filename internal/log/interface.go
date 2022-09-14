package log

// Modular is a log printer that allows you to branch new modules.
type Modular interface {
	WithFields(fields map[string]string) Modular
	With(keyValues ...any) Modular

	Fatalf(format string, v ...any)
	Errorf(format string, v ...any)
	Warnf(format string, v ...any)
	Infof(format string, v ...any)
	Debugf(format string, v ...any)
	Tracef(format string, v ...any)

	Fatalln(message string)
	Errorln(message string)
	Warnln(message string)
	Infoln(message string)
	Debugln(message string)
	Traceln(message string)
}
