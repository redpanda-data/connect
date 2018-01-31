// Copyright (c) 2017 Ashley Jeffs
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

package text

import (
	"bytes"
	"os"
	"regexp"
)

//------------------------------------------------------------------------------

var envRegex *regexp.Regexp

func init() {
	var err error
	envRegex, err = regexp.Compile(`\${[0-9A-Za-z_]+(:[^}]+)?}`)
	if err != nil {
		panic(err)
	}
}

// ContainsEnvVariables returns true if inBytes contains environment variable
// replace patterns.
func ContainsEnvVariables(inBytes []byte) bool {
	return envRegex.Find(inBytes) != nil
}

// ReplaceEnvVariables will search a blob of data for the pattern `${FOO:bar}`,
// where `FOO` is an environment variable name and `bar` is a default value. The
// `bar` section (including the colon) can be left out if there is no
// appropriate default value for the field.
//
// For each aforementioned pattern found in the blob the contents of the
// respective environment variable will be read and will replace the pattern. If
// the environment variable is empty or does not exist then either the default
// value is used or the field will be left empty.
func ReplaceEnvVariables(inBytes []byte) []byte {
	return envRegex.ReplaceAllFunc(inBytes, func(content []byte) []byte {
		var value string
		if len(content) > 3 {
			if colonIndex := bytes.IndexByte(content, ':'); colonIndex == -1 {
				value = os.Getenv(string(content[2 : len(content)-1]))
			} else {
				targetVar := content[2:colonIndex]
				defaultVal := content[colonIndex+1 : len(content)-1]

				value = os.Getenv(string(targetVar))
				if len(value) == 0 {
					value = string(defaultVal)
				}
			}
		}
		return []byte(value)
	})
}

//------------------------------------------------------------------------------
