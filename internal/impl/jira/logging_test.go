package jira

import (
	"testing"
)

func noPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	f()
}

func TestLoggingWrappers_NilReceiver_NoPanic(t *testing.T) {
	var j *jiraProc = nil

	noPanic(t, func() { j.debug("hello %s", "world") })
	noPanic(t, func() { j.info("hello %s", "world") })
	noPanic(t, func() { j.warn("hello %s", "world") })
	noPanic(t, func() { j.error("hello %s", "world") })
}

func TestLoggingWrappers_NilLogger_NoPanic(t *testing.T) {
	j := &jiraProc{log: nil}

	noPanic(t, func() { j.debug("debug %d", 1) })
	noPanic(t, func() { j.info("info %d", 2) })
	noPanic(t, func() { j.warn("warn %d", 3) })
	noPanic(t, func() { j.error("error %d", 4) })
}
