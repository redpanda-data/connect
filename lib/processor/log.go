// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
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

package processor

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeLog] = TypeSpec{
		constructor: NewLog,
		description: `
Log is a processor that prints a log event each time it processes a message. The
message is then sent onwards unchanged. The log message can be set using
function interpolations described [here](../config_interpolation.md#functions)
which allows you to log the contents and metadata of a message.

For example, if we wished to create a debug log event for each message in a
pipeline in order to expose the JSON field ` + "`foo.bar`" + ` as well as the
metadata field ` + "`kafka_partition`" + ` we can achieve that with the
following config:

` + "``` yaml" + `
type: log
log:
  level: DEBUG
  message: "field: ${!json_field:foo.bar}, part: ${!metadata:kafka_partition}"
` + "```" + `

The ` + "`level`" + ` field determines the log level of the printed events and
can be any of the following values: TRACE, DEBUG, INFO, WARN, ERROR.`,
	}
}

//------------------------------------------------------------------------------

// LogConfig contains configuration fields for the Log processor.
type LogConfig struct {
	Level   string `json:"level" yaml:"level"`
	Message string `json:"message" yaml:"message"`
}

// NewLogConfig returns a LogConfig with default values.
func NewLogConfig() LogConfig {
	return LogConfig{
		Level:   "INFO",
		Message: "",
	}
}

//------------------------------------------------------------------------------

// Log is a processor that prints a log event each time it processes a message.
type Log struct {
	log     log.Modular
	level   string
	message *text.InterpolatedString
	printFn func(msg string)
}

// NewLog returns a Log processor.
func NewLog(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	l := &Log{
		log:     log,
		level:   conf.Log.Level,
		message: text.NewInterpolatedString(conf.Log.Message),
	}
	var err error
	if l.printFn, err = l.levelToLogFn(l.level); err != nil {
		return nil, err
	}
	return l, nil
}

//------------------------------------------------------------------------------

func (l *Log) levelToLogFn(level string) (func(msg string), error) {
	switch level {
	case "TRACE":
		return l.log.Traceln, nil
	case "DEBUG":
		return l.log.Debugln, nil
	case "INFO":
		return l.log.Infoln, nil
	case "WARN":
		return l.log.Warnln, nil
	case "ERROR":
		return l.log.Errorln, nil
	}
	return nil, fmt.Errorf("log level not recognised: %v", level)
}

//------------------------------------------------------------------------------

// ProcessMessage logs an event and returns the message unchanged.
func (l *Log) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	msgs := [1]types.Message{msg}
	l.printFn(l.message.Get(msg))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (l *Log) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (l *Log) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
