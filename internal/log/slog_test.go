//go:build go1.21

package log

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Clear slog's time attribute for easier testing
var clearTimeAttr = func(_ []string, a slog.Attr) slog.Attr {
	if a.Key == "time" {
		return slog.String("time", "")
	}
	return a
}

func TestSlogToBenthosLoggerAdapter(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: clearTimeAttr})
	s := slog.New(h)

	s = s.With("foo", "bar", "count", "10", "thing", "is a string", "iscool", "true")

	var logger Modular = NewBenthosLogAdapter(s)
	require.NotNil(t, logger)

	logger.Warnln("Warning message foo fields")
	logger.Warnf("Warning message root module\n")

	expected := "time=\"\" level=WARN msg=\"Warning message foo fields\" foo=bar count=10 thing=\"is a string\" iscool=true\ntime=\"\" level=WARN msg=\"Warning message root module\\n\" foo=bar count=10 thing=\"is a string\" iscool=true\n"
	assert.Equal(t, expected, buf.String())
}

func TestSlogToBenthosLoggerAdapterMapKV(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: clearTimeAttr})
	s := slog.New(h)

	var logger Modular = NewBenthosLogAdapter(s)
	require.NotNil(t, logger)

	logger = logger.WithFields(map[string]string{
		"foo":   "bar",
		"count": "10",
	})

	logger = logger.With("thing", "is a string", "iscool", "true")

	logger.Warnln("Warning message foo fields")
	logger.Warnf("Warning message root module\n")

	bufStr := buf.String()

	for _, exp := range []string{
		"time=\"\" level=WARN msg=\"Warning message foo fields\"",
		"foo=bar",
		"count=10",
		"thing=\"is a string\" iscool=true",
		"time=\"\" level=WARN msg=\"Warning message root module\\n\"",
	} {
		assert.Contains(t, bufStr, exp)
	}
}

func TestSlogMessageFormatting(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: clearTimeAttr, Level: slog.LevelDebug})
	s := slog.New(h)

	var logger Modular = NewBenthosLogAdapter(s)
	require.NotNil(t, logger)

	logger.Debugf("Hello %s %d", "World", 1)
	logger.Infof("Hello %s %d", "World", 2)
	logger.Warnf("Hello %s %d", "World", 3)
	logger.Errorf("Hello %s %d", "World", 4)

	expected := "time=\"\" level=DEBUG msg=\"Hello World 1\"\ntime=\"\" level=INFO msg=\"Hello World 2\"\ntime=\"\" level=WARN msg=\"Hello World 3\"\ntime=\"\" level=ERROR msg=\"Hello World 4\"\n"
	assert.Equal(t, expected, buf.String())
}
