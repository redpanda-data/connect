// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package log

//------------------------------------------------------------------------------

// PrintFormatter is an interface implemented by standard loggers.
type PrintFormatter interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

//------------------------------------------------------------------------------

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

func (l *wrapped) NewModule(prefix string) Modular {
	return l
}

//------------------------------------------------------------------------------

// WithFields is a no-op.
func (l *wrapped) WithFields(fields map[string]string) Modular {
	return l
}

// Fatalf prints a fatal message to the console. Does NOT cause panic.
func (l *wrapped) Fatalf(format string, v ...interface{}) {
	if LogFatal <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Errorf prints an error message to the console.
func (l *wrapped) Errorf(format string, v ...interface{}) {
	if LogError <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Warnf prints a warning message to the console.
func (l *wrapped) Warnf(format string, v ...interface{}) {
	if LogWarn <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Infof prints an information message to the console.
func (l *wrapped) Infof(format string, v ...interface{}) {
	if LogInfo <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Debugf prints a debug message to the console.
func (l *wrapped) Debugf(format string, v ...interface{}) {
	if LogDebug <= l.level {
		l.pf.Printf(format, v...)
	}
}

// Tracef prints a trace message to the console.
func (l *wrapped) Tracef(format string, v ...interface{}) {
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

//------------------------------------------------------------------------------
