package log

// PrintFormatter is an interface implemented by standard loggers.
type PrintFormatter interface {
	Printf(format string, v ...any)
	Println(v ...any)
}

//------------------------------------------------------------------------------

// Logger level constants.
const (
	LogOff   int = 0
	LogFatal int = 1
	LogError int = 2
	LogWarn  int = 3
	LogInfo  int = 4
	LogDebug int = 5
	LogTrace int = 6
	LogAll   int = 7
)

// wrapped is an object with support for levelled logging and modular components.
type wrapped struct {
	pf    PrintFormatter
	level int
}

// Wrap a PrintFormatter with a log.Modular implementation. Log level is set to
// INFO, use WrapAtLevel to set this explicitly.
func Wrap(l PrintFormatter) Modular {
	return &wrapped{
		pf:    l,
		level: LogInfo,
	}
}

// WrapAtLevel wraps a PrintFormatter with a log.Modular implementation with an
// explicit log level.
func WrapAtLevel(l PrintFormatter, level int) Modular {
	return &wrapped{
		pf:    l,
		level: level,
	}
}

//------------------------------------------------------------------------------

// WithFields is a no-op.
func (l *wrapped) WithFields(fields map[string]string) Modular {
	return l
}

// With is a no-op.
func (l *wrapped) With(keyValues ...any) Modular {
	return l
}

// Fatalf prints a fatal message to the console. Does NOT cause panic.
func (l *wrapped) Fatalf(format string, v ...any) {
	if LogFatal <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Errorf prints an error message to the console.
func (l *wrapped) Errorf(format string, v ...any) {
	if LogError <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Warnf prints a warning message to the console.
func (l *wrapped) Warnf(format string, v ...any) {
	if LogWarn <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Infof prints an information message to the console.
func (l *wrapped) Infof(format string, v ...any) {
	if LogInfo <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Debugf prints a debug message to the console.
func (l *wrapped) Debugf(format string, v ...any) {
	if LogDebug <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Tracef prints a trace message to the console.
func (l *wrapped) Tracef(format string, v ...any) {
	if LogTrace <= l.level {
		l.pf.Printf(format, v...)
	}
}

//------------------------------------------------------------------------------

// Fatalln prints a fatal message to the console. Does NOT cause panic.
func (l *wrapped) Fatalln(message string) {
	if LogFatal <= l.level {
		l.pf.Println(message)
	}
}

// Errorln prints an error message to the console.
func (l *wrapped) Errorln(message string) {
	if LogError <= l.level {
		l.pf.Println(message)
	}
}

// Warnln prints a warning message to the console.
func (l *wrapped) Warnln(message string) {
	if LogWarn <= l.level {
		l.pf.Println(message)
	}
}

// Infoln prints an information message to the console.
func (l *wrapped) Infoln(message string) {
	if LogInfo <= l.level {
		l.pf.Println(message)
	}
}

// Debugln prints a debug message to the console.
func (l *wrapped) Debugln(message string) {
	if LogDebug <= l.level {
		l.pf.Println(message)
	}
}

// Traceln prints a trace message to the console.
func (l *wrapped) Traceln(message string) {
	if LogTrace <= l.level {
		l.pf.Println(message)
	}
}
