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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeLog] = TypeSpec{
		constructor: NewLog,
		description: `
Log is a processor that prints a log event each time it processes a batch. The
batch is then sent onwards unchanged. The log message can be set using function
interpolations described [here](../config_interpolation.md#functions) which
allows you to log the contents and metadata of a messages within a batch.

In order to print a log message per message of a batch place it within a
` + "[`for_each`](#for_each)" + ` processor.

For example, if we wished to create a debug log event for each message in a
pipeline in order to expose the JSON field ` + "`foo.bar`" + ` as well as the
metadata field ` + "`kafka_partition`" + ` we can achieve that with the
following config:

` + "``` yaml" + `
for_each:
- log:
    level: DEBUG
    message: "field: ${!json_field:foo.bar}, part: ${!metadata:kafka_partition}"
` + "```" + `

The ` + "`level`" + ` field determines the log level of the printed events and
can be any of the following values: TRACE, DEBUG, INFO, WARN, ERROR.

### Structured Fields

It's also possible to output a map of structured fields, this only works when
the service log is set to output as JSON. The field values are function
interpolated, meaning it's possible to output structured fields containing
message contents and metadata, e.g.:

` + "``` yaml" + `
log:
  level: DEBUG
  message: "foo"
  fields:
    id: "${!json_field:id}"
    kafka_topic: "${!metadata:kafka_topic}"
` + "```" + ``,
	}
}

//------------------------------------------------------------------------------

// LogConfig contains configuration fields for the Log processor.
type LogConfig struct {
	Level   string            `json:"level" yaml:"level"`
	Fields  map[string]string `json:"fields" yaml:"fields"`
	Message string            `json:"message" yaml:"message"`
}

// NewLogConfig returns a LogConfig with default values.
func NewLogConfig() LogConfig {
	return LogConfig{
		Level:   "INFO",
		Fields:  map[string]string{},
		Message: "",
	}
}

//------------------------------------------------------------------------------

// Log is a processor that prints a log event each time it processes a message.
type Log struct {
	log     log.Modular
	level   string
	message *text.InterpolatedString
	fields  map[string]*text.InterpolatedString
	printFn func(logger log.Modular, msg string)
}

// NewLog returns a Log processor.
func NewLog(
	conf Config, mgr types.Manager, logger log.Modular, stats metrics.Type,
) (Type, error) {
	l := &Log{
		log:     logger,
		level:   conf.Log.Level,
		fields:  map[string]*text.InterpolatedString{},
		message: text.NewInterpolatedString(conf.Log.Message),
	}
	if len(conf.Log.Fields) > 0 {
		staticFields := map[string]string{}
		for k, v := range conf.Log.Fields {
			if text.ContainsFunctionVariables([]byte(v)) {
				l.fields[k] = text.NewInterpolatedString(v)
			} else {
				staticFields[k] = v
			}
		}
		if len(staticFields) > 0 {
			l.log = log.WithFields(l.log, staticFields)
		}
	}
	var err error
	if l.printFn, err = l.levelToLogFn(l.level); err != nil {
		return nil, err
	}
	return l, nil
}

//------------------------------------------------------------------------------

func (l *Log) levelToLogFn(level string) (func(logger log.Modular, msg string), error) {
	switch level {
	case "TRACE":
		return func(logger log.Modular, msg string) {
			logger.Traceln(msg)
		}, nil
	case "DEBUG":
		return func(logger log.Modular, msg string) {
			logger.Debugln(msg)
		}, nil
	case "INFO":
		return func(logger log.Modular, msg string) {
			logger.Infoln(msg)
		}, nil
	case "WARN":
		return func(logger log.Modular, msg string) {
			logger.Warnln(msg)
		}, nil
	case "ERROR":
		return func(logger log.Modular, msg string) {
			logger.Errorln(msg)
		}, nil
	}
	return nil, fmt.Errorf("log level not recognised: %v", level)
}

//------------------------------------------------------------------------------

// ProcessMessage logs an event and returns the message unchanged.
func (l *Log) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	targetLog := l.log
	if len(l.fields) > 0 {
		interpFields := make(map[string]string, len(l.fields))
		for k, vi := range l.fields {
			interpFields[k] = vi.Get(msg)
		}
		targetLog = log.WithFields(targetLog, interpFields)
	}
	msgs := [1]types.Message{msg}
	l.printFn(targetLog, l.message.Get(msg))
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
