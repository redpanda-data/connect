package log

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slog"
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
