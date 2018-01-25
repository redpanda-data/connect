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
	"os"
	"regexp"
	"strconv"
	"time"
)

//------------------------------------------------------------------------------

var specialRegex *regexp.Regexp

func init() {
	var err error
	specialRegex, err = regexp.Compile(`\${![a-z_]+}`)
	if err != nil {
		panic(err)
	}
}

var specialVars = map[string]func() []byte{
	"timestamp_unix": func() []byte {
		return []byte(strconv.FormatInt(time.Now().Unix(), 10))
	},
	"hostname": func() []byte {
		hn, _ := os.Hostname()
		return []byte(hn)
	},
}

// ContainsSpecialVariables returns true if inBytes contains special variable
// replace patterns.
func ContainsSpecialVariables(inBytes []byte) bool {
	return specialRegex.Find(inBytes) != nil
}

// ReplaceSpecialVariables will search a blob of data for the pattern
// `${!foo}`, where `foo` is a special variable name.
//
// For each aforementioned pattern found in the blob the contents of the
// respective special variable will be calculated and will replace the pattern.
func ReplaceSpecialVariables(inBytes []byte) []byte {
	return specialRegex.ReplaceAllFunc(inBytes, func(content []byte) []byte {
		if len(content) > 4 {
			if ftor, exists := specialVars[string(content[3:len(content)-1])]; exists {
				return ftor()
			}
		}
		return content
	})
}

//------------------------------------------------------------------------------
