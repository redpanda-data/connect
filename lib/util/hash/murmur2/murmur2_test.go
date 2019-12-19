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

package murmur2

import (
	"strconv"
	"testing"
)

func TestMurmur2SanityCheck(t *testing.T) {
	tests := []struct {
		data     []string
		expected int32
	}{
		{[]string{"hello world"}, 1221641059},
		{[]string{"hello" + " " + "world"}, 1221641059},
		// examples from: https://stackoverflow.com/questions/48582589/porting-kafkas-murmur2-implementation-to-go
		{[]string{"21"}, -973932308},
		{[]string{"foobar"}, -790332482},
		{[]string{"a-little-bit-long-string"}, -985981536},
		{[]string{"a-little-bit-longer-string"}, -1486304829},
		{[]string{"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"}, -58897971},
		{[]string{"a", "b", "c"}, 479470107},
	}
	for i, tt := range tests {
		t.Run(strconv.Itoa(i)+". ", func(t *testing.T) {
			mur := New32()
			for _, datum := range tt.data {
				_, _ = mur.Write([]byte(datum))
			}
			calculated := mur.Sum32()
			if int32(calculated) != tt.expected {
				t.Errorf("murmur2 hash failed: is -> %v != %v <- should be", calculated, tt.expected)
				return
			}
		})
	}
}
