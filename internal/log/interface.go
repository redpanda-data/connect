package log

// Modular is a log printer that allows you to branch new modules.
type Modular interface {
	WithFields(fields map[string]string) Modular
	With(keyValues ...interface{}) Modular

	Fatalf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Tracef(format string, v ...interface{})

	Fatalln(message string)
	Errorln(message string)
	Warnln(message string)
	Infoln(message string)
	Debugln(message string)
	Traceln(message string)
}
