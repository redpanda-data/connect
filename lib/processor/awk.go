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
	"bytes"
	"fmt"
	"io/ioutil"
	"regexp"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/benhoyt/goawk/interp"
	"github.com/benhoyt/goawk/parser"
)

//------------------------------------------------------------------------------

var varInvalidRegexp *regexp.Regexp

func init() {
	varInvalidRegexp = regexp.MustCompile(`[^a-zA-Z0-9_]`)

	Constructors[TypeAWK] = TypeSpec{
		constructor: NewAWK,
		description: `
Executes an AWK program on messages by feeding the raw contents as the input and
replaces the contents with the result.

Metadata of a message will be automatically declared as variables, where any
invalid characters in the name will be replaced with underscores. Variables can
also automatically be extracted from the input based on a codec:

### ` + "`none`" + `

No variables are extracted.

### ` + "`json`" + `

Variables are extracted from the message by walking the flattened JSON
structure. Each value is converted into a variable by taking its full path, e.g.
the object:

` + "``` json" + `
{
	"foo": {
		"bar": {
			"value": "foobar"
		}
		"baz": 10
	}
}
` + "```" + `

Would result in the following variable declarations:

` + "```" + `
foo_bar_value = foobar
foo_baz = 10
` + "```" + ``,
	}
}

//------------------------------------------------------------------------------

// AWKConfig contains configuration fields for the AWK processor.
type AWKConfig struct {
	Parts   []int  `json:"parts" yaml:"parts"`
	Codec   string `json:"codec" yaml:"codec"`
	Program string `json:"program" yaml:"program"`
}

// NewAWKConfig returns a AWKConfig with default values.
func NewAWKConfig() AWKConfig {
	return AWKConfig{
		Parts:   []int{},
		Codec:   "none",
		Program: "{ print $0 }",
	}
}

//------------------------------------------------------------------------------

// AWK is a processor that executes AWK programs on a message part and replaces
// the contents with the result.
type AWK struct {
	parts   []int
	program *parser.Program

	conf  AWKConfig
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewAWK returns a AWK processor.
func NewAWK(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	program, err := parser.ParseProgram([]byte(conf.AWK.Program), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to compile AWK program: %v", err)
	}
	switch conf.AWK.Codec {
	case "none":
	case "json":
	default:
		return nil, fmt.Errorf("unrecognised codec: %v", conf.AWK.Codec)
	}
	a := &AWK{
		parts:   conf.AWK.Parts,
		program: program,
		conf:    conf.AWK,
		log:     log,
		stats:   stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	return a, nil
}

//------------------------------------------------------------------------------

func flattenForAWK(path string, data interface{}) map[string]string {
	m := map[string]string{}

	switch t := data.(type) {
	case map[string]interface{}:
		for k, v := range t {
			newPath := k
			if len(path) > 0 {
				newPath = path + "." + k
			}
			for k2, v2 := range flattenForAWK(newPath, v) {
				m[k2] = v2
			}
		}
	case []interface{}:
		for _, ele := range t {
			for k, v := range flattenForAWK(path, ele) {
				m[k] = v
			}
		}
	default:
		m[path] = fmt.Sprintf("%v", t)
	}

	return m
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (a *AWK) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	a.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int) {
		var outBuf, errBuf bytes.Buffer

		config := &interp.Config{
			Stdin:  bytes.NewReader(newMsg.Get(index).Get()),
			Output: &outBuf,
			Error:  &errBuf,
		}

		if a.conf.Codec == "json" {
			jsonPart, err := newMsg.Get(index).JSON()
			if err != nil {
				a.mErr.Incr(1)
				a.log.Errorf("Failed to parse part into json: %v\n", err)
				FlagFail(newMsg.Get(index))
				return
			}

			for k, v := range flattenForAWK("", jsonPart) {
				config.Vars = append(config.Vars, varInvalidRegexp.ReplaceAllString(k, "_"), v)
			}
		}

		newMsg.Get(index).Metadata().Iter(func(k, v string) error {
			config.Vars = append(config.Vars, varInvalidRegexp.ReplaceAllString(k, "_"), v)
			return nil
		})

		if _, err := interp.ExecProgram(a.program, config); err != nil {
			a.mErr.Incr(1)
			a.log.Errorf("Non-fatal execution error: %v\n", err)
			FlagFail(newMsg.Get(index))
			return
		}

		if errMsg, err := ioutil.ReadAll(&errBuf); err != nil {
			a.log.Errorf("Read err error: %v\n", err)
		} else if len(errMsg) > 0 {
			a.mErr.Incr(1)
			a.log.Errorf("Execution error: %s\n", errMsg)
			FlagFail(newMsg.Get(index))
		}

		resMsg, err := ioutil.ReadAll(&outBuf)
		if err != nil {
			a.mErr.Incr(1)
			a.log.Errorf("Read output error: %v\n", err)
			FlagFail(newMsg.Get(index))
		}

		// Remove trailing line break
		if resMsg[len(resMsg)-1] == '\n' {
			resMsg = resMsg[:len(resMsg)-1]
		}
		newMsg.Get(index).Set(resMsg)
	}

	if len(a.parts) == 0 {
		for i := 0; i < newMsg.Len(); i++ {
			proc(i)
		}
	} else {
		for _, i := range a.parts {
			proc(i)
		}
	}

	msgs := [1]types.Message{newMsg}

	a.mBatchSent.Incr(1)
	a.mSent.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (a *AWK) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (a *AWK) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
