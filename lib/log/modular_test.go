package log

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModules(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.JSONFormat = false
	loggerConfig.Prefix = "root"
	loggerConfig.LogLevel = "WARN"

	var buf bytes.Buffer

	logger := New(&buf, loggerConfig)
	logger.Warnln("Warning message root module")

	logger2 := logger.NewModule(".foo")
	logger2.Warnln("Warning message root.foo module")

	logger3 := logger.NewModule(".foo2")
	logger3.Warnln("Warning message root.foo2 module")

	logger4 := logger2.NewModule(".bar")
	logger4.Warnln("Warning message root.foo.bar module")

	expected := "WARN | root | Warning message root module\n" +
		"WARN | root.foo | Warning message root.foo module\n" +
		"WARN | root.foo2 | Warning message root.foo2 module\n" +
		"WARN | root.foo.bar | Warning message root.foo.bar module\n"

	assert.Equal(t, expected, buf.String())
}

func TestStaticFields(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.JSONFormat = true
	loggerConfig.Prefix = "root"
	loggerConfig.LogLevel = "WARN"
	loggerConfig.StaticFields = map[string]string{
		"@service": "benthos_service",
		"@system":  "foo",
	}

	var buf bytes.Buffer

	logger := New(&buf, loggerConfig)
	logger.Warnln("Warning message root module")
	logger.Warnf("Warning message root module\n")

	logger2 := logger.NewModule(".foo")
	logger2.Warnln("Warning message root.foo module")

	expected := `{"@service":"benthos_service","@system":"foo","component":"root","level":"WARN","message":"Warning message root module"}
{"@service":"benthos_service","@system":"foo","component":"root","level":"WARN","message":"Warning message root module"}
{"@service":"benthos_service","@system":"foo","component":"root.foo","level":"WARN","message":"Warning message root.foo module"}
`

	assert.Equal(t, expected, buf.String())
}

func TestStaticFieldsOverride(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.JSONFormat = true
	loggerConfig.Prefix = "root"
	loggerConfig.LogLevel = "WARN"
	loggerConfig.StaticFields = map[string]string{
		"@service": "benthos_service",
		"@system":  "foo",
	}

	var buf bytes.Buffer

	logger := New(&buf, loggerConfig)
	logger.Warnf("Warning message root module")

	logger2 := WithFields(logger, map[string]string{"foo": "bar", "@service": "fooserve"})
	logger2.Warnln("Warning message foo fields")

	logger.Warnf("Warning message root module\n")

	expected := `{"@service":"benthos_service","@system":"foo","component":"root","level":"WARN","message":"Warning message root module"}
{"@service":"fooserve","@system":"foo","component":"root","foo":"bar","level":"WARN","message":"Warning message foo fields"}
{"@service":"benthos_service","@system":"foo","component":"root","level":"WARN","message":"Warning message root module"}
`

	assert.Equal(t, expected, buf.String())
}

func TestStaticFieldsEmpty(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.JSONFormat = true
	loggerConfig.Prefix = "root"
	loggerConfig.LogLevel = "WARN"
	loggerConfig.StaticFields = map[string]string{}

	var buf bytes.Buffer

	logger := New(&buf, loggerConfig)
	logger.Warnln("Warning message root module")
	logger.Warnf("Warning message root module\n")

	logger2 := logger.NewModule(".foo")
	logger2.Warnln("Warning message root.foo module")

	expected := `{"component":"root","level":"WARN","message":"Warning message root module"}
{"component":"root","level":"WARN","message":"Warning message root module"}
{"component":"root.foo","level":"WARN","message":"Warning message root.foo module"}
`

	assert.Equal(t, expected, buf.String())
}

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

	logger, err := NewV2(&buf, loggerConfig)
	require.NoError(t, err)

	logger.Warnf("Warning message root module")

	logger2, err := With(logger, "foo", "bar", "count", 10, "thing", "is a string", "iscool", true)
	require.NoError(t, err)
	logger2.Warnln("Warning message foo fields")

	logger.Warnf("Warning message root module\n")

	expected := `@service=benthos_service @system=foo component=benthos level=WARN msg="Warning message root module"
@service=benthos_service @system=foo component=benthos count=10 foo=bar iscool=true thing="is a string" level=WARN msg="Warning message foo fields"
@service=benthos_service @system=foo component=benthos level=WARN msg="Warning message root module"
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

	logger, err := NewV2(&buf, loggerConfig)
	require.NoError(t, err)

	logger, err = With(logger, "foo", "bar", "count", 10, "thing", "is a string", "iscool", true, "woops")
	require.NoError(t, err)

	logger.Warnln("Warning message foo fields")

	expected := `@service=benthos_service @system=foo component=benthos count=10 foo=bar iscool=true thing="is a string" level=WARN msg="Warning message foo fields"
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

	logger, err := NewV2(&buf, loggerConfig)
	require.NoError(t, err)

	logger, err = With(logger, "foo", "bar", 10, "count", "thing", "is a string", "iscool", true)
	require.NoError(t, err)

	logger.Warnln("Warning message foo fields")

	expected := `@service=benthos_service @system=foo component=benthos foo=bar iscool=true thing="is a string" level=WARN msg="Warning message foo fields"
`

	assert.Equal(t, expected, buf.String())
}

func TestFormattedLogging(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.JSONFormat = false
	loggerConfig.Prefix = "test"
	loggerConfig.LogLevel = "WARN"

	var buf bytes.Buffer

	logger := New(&buf, loggerConfig)
	logger.Fatalf("fatal test %v\n", 1)
	logger.Errorf("error test %v\n", 2)
	logger.Warnf("warn test %v\n", 3)
	logger.Infof("info test %v\n", 4)
	logger.Debugf("info test %v\n", 5)
	logger.Tracef("trace test %v\n", 6)

	expected := "FATAL | test | fatal test 1\nERROR | test | error test 2\nWARN | test | warn test 3\n"

	assert.Equal(t, expected, buf.String())
}

func TestLineLogging(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.JSONFormat = false
	loggerConfig.Prefix = "test"
	loggerConfig.LogLevel = "WARN"

	var buf bytes.Buffer

	logger := New(&buf, loggerConfig)
	logger.Fatalln("fatal test")
	logger.Errorln("error test")
	logger.Warnln("warn test")
	logger.Infoln("info test")
	logger.Debugln("info test")
	logger.Traceln("trace test")

	expected := "FATAL | test | fatal test\nERROR | test | error test\nWARN | test | warn test\n"

	assert.Equal(t, expected, buf.String())
}

type logCounter struct {
	count int
}

func (l *logCounter) Write(p []byte) (n int, err error) {
	l.count++
	return len(p), nil
}

func TestLogLevels(t *testing.T) {
	for i := 0; i < LogAll; i++ {
		loggerConfig := NewConfig()
		loggerConfig.JSONFormat = false
		loggerConfig.LogLevel = intToLogLevel(i)

		buf := logCounter{}

		logger := New(&buf, loggerConfig)
		logger.Fatalln("fatal test")
		logger.Errorln("error test")
		logger.Warnln("warn test")
		logger.Infoln("info test")
		logger.Debugln("info test")
		logger.Traceln("trace test")

		if i != buf.count {
			t.Errorf("Wrong log count for [%v], %v != %v", loggerConfig.LogLevel, i, buf.count)
		}
	}
}
