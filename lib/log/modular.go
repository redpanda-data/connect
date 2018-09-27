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
	"encoding/json"
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
	Prefix       string            `json:"prefix" yaml:"prefix"`
	LogLevel     string            `json:"level" yaml:"level"`
	AddTimeStamp bool              `json:"add_timestamp" yaml:"add_timestamp"`
	JSONFormat   bool              `json:"json_format" yaml:"json_format"`
	StaticFields map[string]string `json:"static_fields" yaml:"static_fields"`
}

// NewConfig returns a config struct with the default values for each field.
func NewConfig() Config {
	return Config{
		Prefix:       "benthos",
		LogLevel:     "INFO",
		AddTimeStamp: true,
		JSONFormat:   true,
		StaticFields: map[string]string{
			"@service": "benthos",
		},
	}
}

//------------------------------------------------------------------------------

// UnmarshalJSON ensures that when parsing configs that are in a slice the
// default values are still applied.
func (l *Config) UnmarshalJSON(bytes []byte) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	defaultFields := aliased.StaticFields
	aliased.StaticFields = nil
	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	if aliased.StaticFields == nil {
		aliased.StaticFields = defaultFields
	}

	*l = Config(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (l *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	defaultFields := aliased.StaticFields
	aliased.StaticFields = nil

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	if aliased.StaticFields == nil {
		aliased.StaticFields = defaultFields
	}

	*l = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

// Logger is an object with support for levelled logging and modular components.
type Logger struct {
	stream      io.Writer
	config      Config
	level       int
	extraFields string
}

// New creates and returns a new logger object.
func New(stream io.Writer, config Config) Modular {
	logger := Logger{
		stream: stream,
		config: config,
		level:  logLevelToInt(config.LogLevel),
	}

	if len(config.StaticFields) > 0 {
		jBytes, _ := json.Marshal(config.StaticFields)
		if len(jBytes) > 2 {
			logger.extraFields = string(jBytes[1:len(jBytes)-1]) + ","
		}
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
		stream:      l.stream,
		config:      config,
		level:       l.level,
		extraFields: l.extraFields,
	}
}

//------------------------------------------------------------------------------

// writeFormatted prints a log message with any configured extras prepended.
func (l *Logger) writeFormatted(message string, level string, other ...interface{}) {
	if l.config.JSONFormat {
		if l.config.AddTimeStamp {
			fmt.Fprintf(l.stream, fmt.Sprintf(
				"{\"@timestamp\":\"%v\",%v\"level\":\"%v\",\"component\":\"%v\",\"message\":%v}\n",
				time.Now().Format(time.RFC3339), l.extraFields, level,
				l.config.Prefix, strconv.QuoteToASCII(message),
			), other...)
		} else {
			fmt.Fprintf(l.stream, fmt.Sprintf(
				"{%v\"level\":\"%v\",\"component\":\"%v\",\"message\":%v}\n",
				l.extraFields, level, l.config.Prefix,
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
func (l *Logger) writeLine(message string, level string) {
	if l.config.JSONFormat {
		if l.config.AddTimeStamp {
			fmt.Fprintf(l.stream,
				"{\"@timestamp\":\"%v\",%v\"level\":\"%v\",\"component\":\"%v\",\"message\":%v}\n",
				time.Now().Format(time.RFC3339), l.extraFields, level,
				l.config.Prefix, strconv.QuoteToASCII(message),
			)
		} else {
			fmt.Fprintf(l.stream,
				"{%v\"level\":\"%v\",\"component\":\"%v\",\"message\":%v}\n",
				l.extraFields, level, l.config.Prefix,
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
func (l *Logger) Fatalf(format string, v ...interface{}) {
	if LogFatal <= l.level {
		l.writeFormatted(format, "FATAL", v...)
	}
}

// Errorf prints an error message to the console.
func (l *Logger) Errorf(format string, v ...interface{}) {
	if LogError <= l.level {
		l.writeFormatted(format, "ERROR", v...)
	}
}

// Warnf prints a warning message to the console.
func (l *Logger) Warnf(format string, v ...interface{}) {
	if LogWarn <= l.level {
		l.writeFormatted(format, "WARN", v...)
	}
}

// Infof prints an information message to the console.
func (l *Logger) Infof(format string, v ...interface{}) {
	if LogInfo <= l.level {
		l.writeFormatted(format, "INFO", v...)
	}
}

// Debugf prints a debug message to the console.
func (l *Logger) Debugf(format string, v ...interface{}) {
	if LogDebug <= l.level {
		l.writeFormatted(format, "DEBUG", v...)
	}
}

// Tracef prints a trace message to the console.
func (l *Logger) Tracef(format string, v ...interface{}) {
	if LogTrace <= l.level {
		l.writeFormatted(format, "TRACE", v...)
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
