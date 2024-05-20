package log

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

func TestLoggerTee(t *testing.T) {
	var bufA, bufB bytes.Buffer

	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.Format = "logfmt"
	loggerConfig.LogLevel = "WARN"
	loggerConfig.StaticFields = map[string]string{
		"@service": "benthos_service",
		"@system":  "foo",
	}

	loggerA, err := New(&bufA, ifs.OS(), loggerConfig)
	require.NoError(t, err)

	loggerConfig = NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.Format = "logfmt"
	loggerConfig.LogLevel = "DEBUG"
	loggerConfig.StaticFields = map[string]string{
		"@service": "benthos_service",
		"@system":  "bar",
	}

	loggerB, err := New(&bufB, ifs.OS(), loggerConfig)
	require.NoError(t, err)

	logger := TeeLogger(loggerA, loggerB)

	logger.Warn("Warning message root module")
	logger.Debug("Debug log root module")

	logger2 := logger.WithFields(map[string]string{
		"foo": "bar", "count": "10", "thing": "is a string", "iscool": "true",
	})
	require.NoError(t, err)
	logger2.Debug("Debug log foo fields")
	logger2.Warn("Warning message foo fields")

	logger.Warn("Warning message root module\n")
	logger.Debug("Debug message root again")

	expectedA := `level=warning msg="Warning message root module" @service=benthos_service @system=foo
level=warning msg="Warning message foo fields" @service=benthos_service @system=foo count=10 foo=bar iscool=true thing="is a string"
level=warning msg="Warning message root module" @service=benthos_service @system=foo
`
	assert.Equal(t, expectedA, bufA.String())

	expectedB := `level=warning msg="Warning message root module" @service=benthos_service @system=bar
level=debug msg="Debug log root module" @service=benthos_service @system=bar
level=debug msg="Debug log foo fields" @service=benthos_service @system=bar count=10 foo=bar iscool=true thing="is a string"
level=warning msg="Warning message foo fields" @service=benthos_service @system=bar count=10 foo=bar iscool=true thing="is a string"
level=warning msg="Warning message root module" @service=benthos_service @system=bar
level=debug msg="Debug message root again" @service=benthos_service @system=bar
`
	assert.Equal(t, expectedB, bufB.String())
}
