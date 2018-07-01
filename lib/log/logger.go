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

import (
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)

//------------------------------------------------------------------------------

// Logger level constants
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

// intToLogLevel converts an integer into a human readable log level.
func intToLogLevel(i int) string {
	switch i {
	case LogOff:
		return "OFF"
	case LogFatal:
		return "FATAL"
	case LogError:
		return "ERROR"
	case LogWarn:
		return "WARN"
	case LogInfo:
		return "INFO"
	case LogDebug:
		return "DEBUG"
	case LogTrace:
		return "TRACE"
	case LogAll:
		return "ALL"
	}
	return "ALL"
}

// logLevelToInt converts a human readable log level into an integer value.
func logLevelToInt(level string) int {
	levelUpper := strings.ToUpper(level)
	switch levelUpper {
	case "OFF":
		return LogOff
	case "FATAL":
		return LogFatal
	case "ERROR":
		return LogError
	case "WARN":
		return LogWarn
	case "INFO":
		return LogInfo
	case "DEBUG":
		return LogDebug
	case "TRACE":
		return LogTrace
	case "ALL":
		return LogAll
	}
	return -1
}

//------------------------------------------------------------------------------

// Config holds configuration options for a logger object.
type Config struct {
	Prefix       string `json:"prefix" yaml:"prefix"`
	LogLevel     string `json:"log_level" yaml:"log_level"`
	AddTimeStamp bool   `json:"add_timestamp" yaml:"add_timestamp"`
	JSONFormat   bool   `json:"json_format" yaml:"json_format"`
}

// NewConfig returns a config struct with the default values for each field.
func NewConfig() Config {
	return Config{
		Prefix:       "benthos",
		LogLevel:     "INFO",
		AddTimeStamp: true,
		JSONFormat:   true,
	}
}

//------------------------------------------------------------------------------

// Logger is an object with support for levelled logging and modular components.
type Logger struct {
	stream    io.Writer
	fannedOut bool
	config    Config
	level     int
}

// New creates and returns a new logger object.
func New(stream io.Writer, config Config) Modular {
	logger := Logger{
		stream: stream,
		config: config,
		level:  logLevelToInt(config.LogLevel),
	}
	return &logger
}

// Noop creates and returns a new logger object that writes nothing.
func Noop() Modular {
	return &Logger{
		stream: ioutil.Discard,
		config: NewConfig(),
		level:  LogOff,
	}
}

// NewModule creates a new logger object from the previous, using the same
// configuration, but adds an extra prefix to represent a submodule.
func (l *Logger) NewModule(prefix string) Modular {
	config := l.config
	config.Prefix = fmt.Sprintf("%v%v", config.Prefix, prefix)

	return &Logger{
		stream: l.stream,
		config: config,
		level:  l.level,
	}
}

// AddWriter adds a new writer to the logger which receives the same log data as
// the primary writer. If this new writer returns an error it is removed. The
// logger becomes the owner of this writer and under any circumstance whereby
// the writer is removed it will also be closed by the logger.
func (l *Logger) AddWriter(w io.Writer) {
	if !l.fannedOut {
		l.stream = NewFanOutWriter(l.stream)
		l.fannedOut = true
	}
	if fo, ok := l.stream.(*FanOutWriter); ok {
		fo.Add(w)
	}
}

// RemoveWriter removes writer from the logger.
func (l *Logger) RemoveWriter(w io.Writer) {
	if fo, ok := l.stream.(*FanOutWriter); ok {
		fo.Remove(w)
	}
}

//------------------------------------------------------------------------------

// Close the logger, including the underlying io.Writer if it implements the
// io.Closer interface.
func (l *Logger) Close() error {
	if c, ok := l.stream.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

//------------------------------------------------------------------------------

// writeFormatted prints a log message with any configured extras prepended.
func (l *Logger) writeFormatted(message, level string, other ...interface{}) {
	if l.config.JSONFormat {
		if l.config.AddTimeStamp {
			fmt.Fprintf(l.stream, fmt.Sprintf(
				"{\"@timestamp\":\"%v\",\"level\":\"%v\",\"@service\":\"%v\",\"message\":%v}\n",
				time.Now().Format(time.RFC3339), level, l.config.Prefix,
				strconv.QuoteToASCII(message),
			), other...)
		} else {
			fmt.Fprintf(l.stream, fmt.Sprintf(
				"{\"level\":\"%v\",\"@service\":\"%v\",\"message\":%v}\n",
				level, l.config.Prefix,
				strconv.QuoteToASCII(message),
			), other...)
		}
	} else {
		if l.config.AddTimeStamp {
			fmt.Fprintf(l.stream, fmt.Sprintf(
				"%v | %v | %v | %v",
				time.Now().Format(time.RFC3339), level, l.config.Prefix, message,
			), other...)
		} else {
			fmt.Fprintf(l.stream, fmt.Sprintf(
				"%v | %v | %v", level, l.config.Prefix, message,
			), other...)
		}
	}
}

// writeLine prints a log message with any configured extras prepended.
func (l *Logger) writeLine(message, level string) {
	if l.config.JSONFormat {
		if l.config.AddTimeStamp {
			fmt.Fprintf(l.stream,
				"{\"@timestamp\":\"%v\",\"level\":\"%v\",\"@service\":\"%v\",\"message\":%v}\n",
				time.Now().Format(time.RFC3339), level, l.config.Prefix,
				strconv.QuoteToASCII(message),
			)
		} else {
			fmt.Fprintf(l.stream,
				"{\"level\":\"%v\",\"@service\":\"%v\",\"message\":%v}\n",
				level, l.config.Prefix,
				strconv.QuoteToASCII(message),
			)
		}
	} else {
		if l.config.AddTimeStamp {
			fmt.Fprintf(
				l.stream, "%v | %v | %v | %v\n",
				time.Now().Format(time.RFC3339), level, l.config.Prefix, message,
			)
		} else {
			fmt.Fprintf(l.stream, "%v | %v | %v\n", level, l.config.Prefix, message)
		}
	}
}

//------------------------------------------------------------------------------

// Fatalf prints a fatal message to the console. Does NOT cause panic.
func (l *Logger) Fatalf(message string, other ...interface{}) {
	if LogFatal <= l.level {
		l.writeFormatted(message, "FATAL", other...)
	}
}

// Errorf prints an error message to the console.
func (l *Logger) Errorf(message string, other ...interface{}) {
	if LogError <= l.level {
		l.writeFormatted(message, "ERROR", other...)
	}
}

// Warnf prints a warning message to the console.
func (l *Logger) Warnf(message string, other ...interface{}) {
	if LogWarn <= l.level {
		l.writeFormatted(message, "WARN", other...)
	}
}

// Infof prints an information message to the console.
func (l *Logger) Infof(message string, other ...interface{}) {
	if LogInfo <= l.level {
		l.writeFormatted(message, "INFO", other...)
	}
}

// Debugf prints a debug message to the console.
func (l *Logger) Debugf(message string, other ...interface{}) {
	if LogDebug <= l.level {
		l.writeFormatted(message, "DEBUG", other...)
	}
}

// Tracef prints a trace message to the console.
func (l *Logger) Tracef(message string, other ...interface{}) {
	if LogTrace <= l.level {
		l.writeFormatted(message, "TRACE", other...)
	}
}

//------------------------------------------------------------------------------

// Fatalln prints a fatal message to the console. Does NOT cause panic.
func (l *Logger) Fatalln(message string) {
	if LogFatal <= l.level {
		l.writeLine(message, "FATAL")
	}
}

// Errorln prints an error message to the console.
func (l *Logger) Errorln(message string) {
	if LogError <= l.level {
		l.writeLine(message, "ERROR")
	}
}

// Warnln prints a warning message to the console.
func (l *Logger) Warnln(message string) {
	if LogWarn <= l.level {
		l.writeLine(message, "WARN")
	}
}

// Infoln prints an information message to the console.
func (l *Logger) Infoln(message string) {
	if LogInfo <= l.level {
		l.writeLine(message, "INFO")
	}
}

// Debugln prints a debug message to the console.
func (l *Logger) Debugln(message string) {
	if LogDebug <= l.level {
		l.writeLine(message, "DEBUG")
	}
}

// Traceln prints a trace message to the console.
func (l *Logger) Traceln(message string) {
	if LogTrace <= l.level {
		l.writeLine(message, "TRACE")
	}
}

//------------------------------------------------------------------------------

// Output prints s to our output. Calldepth is ignored.
func (l *Logger) Output(calldepth int, s string) error {
	io.WriteString(l.stream, s)
	return nil
}

//------------------------------------------------------------------------------
