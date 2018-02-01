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

func TestFunctionVarDetection(t *testing.T) {
	tests := map[string]bool{
		"foo ${!foo_bar} baz":                       true,
		"foo ${!foo_bar} baz ${!foo_baz}":           true,
		"foo $!foo} baz $!but_not_this}":            false,
		"foo ${!baz ${!or_this":                     false,
		"foo ${baz} ${or_this}":                     false,
		"nothing $ here boss {!}":                   false,
		"foo ${!foo_bar:arg1} baz":                  true,
		"foo ${!foo_bar:} baz":                      false,
		"foo ${!foo_bar:arg1} baz ${!foo_baz:arg2}": true,
		"foo $!foo:arg2} baz $!but_not_this:}":      false,
		"nothing $ here boss {!:argnope}":           false,
	}

	for in, exp := range tests {
		act := ContainsFunctionVariables([]byte(in))
		if act != exp {
			t.Errorf("Wrong result for '%v': %v != %v", in, act, exp)
		}
	}
}

func TestFunctionSwapping(t *testing.T) {
	hostname, _ := os.Hostname()

	exp := fmt.Sprintf("foo %v baz", hostname)
	act := string(ReplaceFunctionVariables([]byte("foo ${!hostname} baz")))
	if act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	exp = "foo ${!} baz"
	act = string(ReplaceFunctionVariables([]byte("foo ${!} baz")))
	if act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	exp = "foo ${!does_not_exist} baz"
	act = string(ReplaceFunctionVariables([]byte("foo ${!does_not_exist} baz")))
	if act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	now := time.Now()
	tStamp := string(ReplaceFunctionVariables([]byte("${!timestamp_unix_nano}")))

	nanoseconds, err := strconv.ParseInt(tStamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen := time.Unix(0, nanoseconds)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	tStamp = string(ReplaceFunctionVariables([]byte("${!timestamp_unix}")))

	seconds, err := strconv.ParseInt(tStamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen = time.Unix(seconds, 0)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	tStamp = string(ReplaceFunctionVariables([]byte("${!timestamp_unix:10}")))

	var secondsF float64
	secondsF, err = strconv.ParseFloat(tStamp, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen = time.Unix(int64(secondsF), 0)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	tStamp = string(ReplaceFunctionVariables([]byte("${!timestamp}")))

	tThen, err = time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", tStamp)
	if err != nil {
		t.Fatal(err)
	}

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}
}

func TestEchoFunction(t *testing.T) {
	tests := map[string]string{
		"foo ${!echo:bar}":              "foo bar",
		"foo ${!echo}":                  "foo ",
		"foo ${!echo:bar} ${!echo:baz}": "foo bar baz",
	}

	for input, exp := range tests {
		act := string(ReplaceFunctionVariables([]byte(input)))
		if exp != act {
			t.Errorf("Wrong results for input (%v): %v != %v", input, act, exp)
		}
	}
}

func TestCountersFunction(t *testing.T) {
	tests := [][2]string{
		{"foo1: ${!count:foo}", "foo1: 1"},
		{"bar1: ${!count:bar}", "bar1: 1"},
		{"foo2: ${!count:foo} ${!count:foo}", "foo2: 2 3"},
		{"bar2: ${!count:bar} ${!count:bar}", "bar2: 2 3"},
		{"foo3: ${!count:foo} ${!count:foo}", "foo3: 4 5"},
		{"bar3: ${!count:bar} ${!count:bar}", "bar3: 4 5"},
	}

	for _, test := range tests {
		input := test[0]
		exp := test[1]
		act := string(ReplaceFunctionVariables([]byte(input)))
		if exp != act {
			t.Errorf("Wrong results for input (%v): %v != %v", input, act, exp)
		}
	}
}
