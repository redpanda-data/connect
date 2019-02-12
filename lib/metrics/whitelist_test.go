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

package metrics

import (
	"testing"
)

func TestWhitelistPaths(t *testing.T) {

	// d := &Whitelist{
	// 	paths:    []string{"output", "input", "metrics"},
	// 	patterns: []string{},
	// 	s: &HTTP{
	// 		local:      NewLocal(),
	// 		timestamp:  time.Now(),
	// 		pathPrefix: "",
	// 	},
	// }
	//
	// // acceptable paths
	// acceptable := []string{"output.broker", "input.test", "metrics.status"}
	// for _, v := range acceptable {
	// 	m := d.GetCounter(v)
	// 	if _, ok := m.(LocalStat); !ok {
	// 		t.Errorf("Whitelist should return legitimate stat.")
	// 	}
	// }
	//
	// unacceptable := []string{"processor.value", "logs.info", "test.test"}
	// for _, v := range acceptable {
	// 	m := d.GetCounter(v)
	// 	if DudStat(m) == nil {
	// 		t.Errorf("Whitelist should return dudstat for non-passing stats.")
	// 	}
	// }
}
