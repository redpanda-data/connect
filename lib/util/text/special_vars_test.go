// Copyright (c) 2018 Ashley Jeffs
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
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestSpecialVarDetection(t *testing.T) {
	tests := map[string]bool{
		"foo ${!foo_bar} baz":             true,
		"foo ${!foo_bar} baz ${!foo_baz}": true,
		"foo $!foo} baz $!but_not_this}":  false,
		"foo ${!baz ${!or_this":           false,
		"foo ${baz} ${or_this}":           false,
		"nothing $ here boss {!}":         false,
	}

	for in, exp := range tests {
		act := ContainsSpecialVariables([]byte(in))
		if act != exp {
			t.Errorf("Wrong result for '%v': %v != %v", in, act, exp)
		}
	}
}

func TestSpecialSwapping(t *testing.T) {
	hostname, _ := os.Hostname()

	exp := fmt.Sprintf("foo %v baz", hostname)
	act := string(ReplaceSpecialVariables([]byte("foo ${!hostname} baz")))
	if act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	exp = "foo ${!} baz"
	act = string(ReplaceSpecialVariables([]byte("foo ${!} baz")))
	if act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	exp = "foo ${!does_not_exist} baz"
	act = string(ReplaceSpecialVariables([]byte("foo ${!does_not_exist} baz")))
	if act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	now := time.Now()
	tStamp := string(ReplaceSpecialVariables([]byte("${!timestamp_unix}")))

	seconds, err := strconv.ParseInt(tStamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen := time.Unix(seconds, 0)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}
}
