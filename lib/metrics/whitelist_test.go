package metrics

import (
	"regexp"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
)

func TestWhitelistPaths(t *testing.T) {

	child := &HTTP{
		local:      NewLocal(),
		timestamp:  time.Now(),
		pathPrefix: "",
	}

	w := &Whitelist{
		paths:    []string{"output", "input", "metrics"},
		patterns: []*regexp.Regexp{},
		s:        child,
		log:      log.Noop(),
	}

	whitelistedStats := []string{"output.broker", "input.test", "metrics.status"}
	for _, v := range whitelistedStats {
		w.GetCounter(v)
		if _, ok := child.local.flatCounters[v]; !ok {
			t.Errorf("Whitelist should set a stat in child for allowed path: %s", v)
		}
	}

	rejectedStats := []string{"processor.value", "logs.info", "test.test"}
	for _, v := range rejectedStats {
		w.GetCounter(v)
		if _, ok := child.local.flatCounters[v]; ok {
			t.Errorf("Whitelist should not set a stat in child for unallowed path: %s", v)
		}
	}
}

func TestWhitelistPatterns(t *testing.T) {
	child := &HTTP{
		local:      NewLocal(),
		timestamp:  time.Now(),
		pathPrefix: "",
	}
	w := &Whitelist{
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
	w.patterns = testExpressions

	whitelistedStats := []string{"output.broker.connection", "input", "output.broker"}
	for _, v := range whitelistedStats {
		w.GetCounter(v)
		if _, ok := child.local.flatCounters[v]; !ok {
			t.Errorf("Whitelist should set a stat in child that matches an expression: %s", v)
		}
	}
	rejectedStats := []string{"output", "input.connection", "benthos"}
	for _, v := range rejectedStats {
		w.GetCounter(v)
		if _, ok := child.local.flatCounters[v]; ok {
			t.Errorf("Whitelist should not set a stat in child that does not match an expression: %s", v)
		}
	}
}
