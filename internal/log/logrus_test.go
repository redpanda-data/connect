package log

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

func TestLoggerWith(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.Format = "logfmt"
	loggerConfig.LogLevel = "WARN"
	loggerConfig.StaticFields = map[string]string{
		"@service": "benthos_service",
		"@system":  "foo",
	}

	var buf bytes.Buffer

	logger, err := New(&buf, ifs.OS(), loggerConfig)
	require.NoError(t, err)

	logger.Warn("Warning message root module")

	logger2 := logger.WithFields(map[string]string{
		"foo": "bar", "count": "10", "thing": "is a string", "iscool": "true",
	})
	require.NoError(t, err)
	logger2.Warn("Warning message foo fields")

	logger.Warn("Warning message root module\n")

	expected := `level=warning msg="Warning message root module" @service=benthos_service @system=foo
level=warning msg="Warning message foo fields" @service=benthos_service @system=foo count=10 foo=bar iscool=true thing="is a string"
level=warning msg="Warning message root module" @service=benthos_service @system=foo
`

	assert.Equal(t, expected, buf.String())
}

func TestLoggerWithOddArgs(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.Format = "logfmt"
	loggerConfig.LogLevel = "WARN"
	loggerConfig.StaticFields = map[string]string{
		"@service": "benthos_service",
		"@system":  "foo",
	}

	var buf bytes.Buffer

	logger, err := New(&buf, ifs.OS(), loggerConfig)
	require.NoError(t, err)

	logger = logger.WithFields(map[string]string{
		"foo": "bar", "count": "10", "thing": "is a string", "iscool": "true",
	})
	require.NoError(t, err)

	logger.Warn("Warning message foo fields")

	expected := `level=warning msg="Warning message foo fields" @service=benthos_service @system=foo count=10 foo=bar iscool=true thing="is a string"
`

	assert.Equal(t, expected, buf.String())
}

func TestLoggerWithNonStringKeys(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.Format = "logfmt"
	loggerConfig.LogLevel = "WARN"
	loggerConfig.StaticFields = map[string]string{
		"@service": "benthos_service",
		"@system":  "foo",
	}

	var buf bytes.Buffer

	logger, err := New(&buf, ifs.OS(), loggerConfig)
	require.NoError(t, err)

	logger = logger.WithFields(map[string]string{
		"component": "meow",
		"foo":       "bar",
		"thing":     "is a string",
		"iscool":    "true",
	})

	logger.Warn("Warning message foo fields")

	expected := `level=warning msg="Warning message foo fields" @service=benthos_service @system=foo component=meow foo=bar iscool=true thing="is a string"
`

	assert.Equal(t, expected, buf.String())
}

func TestLoggerWithOtherNames(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.Format = "json"
	loggerConfig.LogLevel = "WARN"
	loggerConfig.StaticFields = map[string]string{
		"@service": "benthos_service",
		"@system":  "foo",
	}
	loggerConfig.LevelName = "severity"
	loggerConfig.MessageName = "message"

	var buf bytes.Buffer

	logger, err := New(&buf, ifs.OS(), loggerConfig)
	require.NoError(t, err)

	logger = logger.WithFields(map[string]string{
		"foo": "bar",
	})
	require.NoError(t, err)

	logger.Warn("Warning message foo fields")

	expected := `{"@service":"benthos_service","@system":"foo","foo":"bar","message":"Warning message foo fields","severity":"warning"}
`

	require.JSONEq(t, expected, buf.String())
}

type logCounter struct {
	count int
}

func (l *logCounter) Write(p []byte) (n int, err error) {
	l.count++
	return len(p), nil
}

func TestLogLevels(t *testing.T) {
	for i, lvl := range []string{
		"FATAL",
		"ERROR",
		"WARN",
		"INFO",
		"DEBUG",
		"TRACE",
	} {
		loggerConfig := NewConfig()
		loggerConfig.LogLevel = lvl

		buf := logCounter{}

		logger, err := New(&buf, ifs.OS(), loggerConfig)
		require.NoError(t, err)

		logger.Error("error test")
		logger.Warn("warn test")
		logger.Info("info test")
		logger.Debug("info test")
		logger.Trace("trace test")

		if i != buf.count {
			t.Errorf("Wrong log count for [%v], %v != %v", loggerConfig.LogLevel, i, buf.count)
		}
	}
}
