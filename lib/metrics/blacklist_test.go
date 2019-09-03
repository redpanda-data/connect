// Copyright (c) 2019 Daniel Rubenstein
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
	"regexp"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
)

func TestBlacklistPaths(t *testing.T) {

	child := &HTTP{
		local:      NewLocal(),
		timestamp:  time.Now(),
		pathPrefix: "",
	}
	b := &Blacklist{
		paths:    []string{"output", "input", "metrics"},
		patterns: []*regexp.Regexp{},
		s:        child,
		log:      log.Noop(),
	}

	// acceptable paths
	blacklistedStats := []string{"output.broker", "input.test", "metrics.status"}
	for _, v := range blacklistedStats {
		b.GetCounter(v)
		if _, ok := child.local.flatCounters[v]; ok {
			t.Errorf("Blacklist should not set a stat in child for disallowed path: %s", v)
		}
	}

	allowedStats := []string{"processor.value", "logs.info", "test.test"}
	for _, v := range allowedStats {
		b.GetCounter(v)
		if _, ok := child.local.flatCounters[v]; !ok {
			t.Errorf("Blacklist should set a stat in child for allowed path: %s", v)
		}
	}
}

func TestBlacklistPatterns(t *testing.T) {

	child := &HTTP{
		local:      NewLocal(),
		timestamp:  time.Now(),
		pathPrefix: "",
	}
	b := &Blacklist{
		paths: []string{},
		s:     child,
		log:   log.Noop(),
	}

	testPatterns := []string{"^output.broker", "^input$"}
	testExpressions := make([]*regexp.Regexp, len(testPatterns))
	for i, v := range testPatterns {
		re, err := regexp.Compile(v)
		if err != nil {
			t.Errorf("Error setting up regular expression to compile")
			return
		}
		testExpressions[i] = re
	}
	b.patterns = testExpressions

	blacklistedStats := []string{"output.broker.connection", "input", "output.broker"}
	for _, v := range blacklistedStats {
		b.GetCounter(v)
		if _, ok := child.local.flatCounters[v]; ok {
			t.Errorf("Blacklist should not set a stat in child that matches an expression: %s", v)
		}
	}
	allowedStats := []string{"output", "input.connection", "benthos"}
	for _, v := range allowedStats {
		b.GetCounter(v)
		if _, ok := child.local.flatCounters[v]; !ok {
			t.Errorf("Blacklist should set a stat in child that does not match an expression: %s", v)
		}
	}
}
