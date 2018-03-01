// Copyright (c) 2014 Ashley Jeffs
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

package input

import (
	"bufio"
	"io"
	"os"

	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["stdin"] = typeSpec{
		constructor: NewSTDIN,
		description: `
The stdin input simply reads any data piped to stdin as messages. By default the
messages are assumed single part and are line delimited. If the multipart option
is set to true then lines are interpretted as message parts, and an empty line
indicates the end of the message.

Alternatively, a custom delimiter can be set that is used instead of line
breaks.`,
	}
}

//------------------------------------------------------------------------------

// STDINConfig contains config fields for the STDIN input type.
type STDINConfig struct {
	Multipart   bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer   int    `json:"max_buffer" yaml:"max_buffer"`
	CustomDelim string `json:"custom_delimiter" yaml:"custom_delimiter"`
}

// NewSTDINConfig creates a STDINConfig populated with default values.
func NewSTDINConfig() STDINConfig {
	return STDINConfig{
		Multipart:   false,
		MaxBuffer:   bufio.MaxScanTokenSize,
		CustomDelim: "",
	}
}

//------------------------------------------------------------------------------

// NewSTDIN creates a new STDIN input type.
func NewSTDIN(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	delim := "\n"
	if len(conf.STDIN.CustomDelim) > 0 {
		delim = conf.STDIN.CustomDelim
	}

	stdin := os.Stdin
	rdr, err := reader.NewLines(
		func() (io.Reader, error) {
			// Swap so this only works once since we don't want to read stdin
			// multiple times.
			if stdin == nil {
				return nil, io.EOF
			}
			sendStdin := stdin
			stdin = nil
			return sendStdin, nil
		},
		func() {},
		reader.OptLinesSetDelimiter(delim),
		reader.OptLinesSetMaxBuffer(conf.STDIN.MaxBuffer),
		reader.OptLinesSetMultipart(conf.STDIN.Multipart),
	)
	if err != nil {
		return nil, err
	}
	return NewReader(
		"stdin",
		reader.NewCutOff(reader.NewPreserver(rdr)),
		log, stats,
	)
}

//------------------------------------------------------------------------------
