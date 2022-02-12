package processor

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//------------------------------------------------------------------------------

type mockLog struct {
	traces        []string
	debugs        []string
	infos         []string
	warns         []string
	errors        []string
	fields        []map[string]string
	mappingFields []interface{}
}

func (m *mockLog) NewModule(prefix string) log.Modular { return m }
func (m *mockLog) WithFields(fields map[string]string) log.Modular {
	m.fields = append(m.fields, fields)
	return m
}
func (m *mockLog) With(args ...interface{}) log.Modular {
	m.mappingFields = append(m.mappingFields, args...)
	return m
}

func (m *mockLog) Fatalf(format string, v ...interface{}) {}
func (m *mockLog) Errorf(format string, v ...interface{}) {
	m.errors = append(m.errors, fmt.Sprintf(format, v...))
}
func (m *mockLog) Warnf(format string, v ...interface{}) {
	m.warns = append(m.warns, fmt.Sprintf(format, v...))

}
func (m *mockLog) Infof(format string, v ...interface{}) {
	m.infos = append(m.infos, fmt.Sprintf(format, v...))
}
func (m *mockLog) Debugf(format string, v ...interface{}) {
	m.debugs = append(m.debugs, fmt.Sprintf(format, v...))
}
func (m *mockLog) Tracef(format string, v ...interface{}) {
	m.traces = append(m.traces, fmt.Sprintf(format, v...))
}

func (m *mockLog) Fatalln(message string) {}
func (m *mockLog) Errorln(message string) {
	m.errors = append(m.errors, message)
}
func (m *mockLog) Warnln(message string) {
	m.warns = append(m.warns, message)
}
func (m *mockLog) Infoln(message string) {
	m.infos = append(m.infos, message)
}
func (m *mockLog) Debugln(message string) {
	m.debugs = append(m.debugs, message)
}
func (m *mockLog) Traceln(message string) {
	m.traces = append(m.traces, message)
}

//------------------------------------------------------------------------------

func TestLogBadLevel(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeLog
	conf.Log.Level = "does not exist"

	if _, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop()); err == nil {
		t.Error("expected err from bad log level")
	}
}

func TestLogLevelTrace(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeLog
	conf.Log.Message = "${!json(\"foo\")}"

	logMock := &mockLog{}

	levels := []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR"}
	for _, level := range levels {
		conf.Log.Level = level
		l, err := New(conf, mock.NewManager(), logMock, metrics.Noop())
		if err != nil {
			t.Fatal(err)
		}

		input := message.QuickBatch([][]byte{[]byte(fmt.Sprintf(`{"foo":"%v"}`, level))})
		expMsgs := []*message.Batch{input}
		actMsgs, res := l.ProcessMessage(input)
		if res != nil {
			t.Fatal(res)
		}
		if !reflect.DeepEqual(expMsgs, actMsgs) {
			t.Errorf("Wrong message passthrough: %v != %v", actMsgs, expMsgs)
		}
	}

	if exp, act := []string{"TRACE"}, logMock.traces; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong log for trace: %v != %v", act, exp)
	}
	if exp, act := []string{"DEBUG"}, logMock.debugs; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong log for debug: %v != %v", act, exp)
	}
	if exp, act := []string{"INFO"}, logMock.infos; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong log for info: %v != %v", act, exp)
	}
	if exp, act := []string{"WARN"}, logMock.warns; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong log for warn: %v != %v", act, exp)
	}
	if exp, act := []string{"ERROR"}, logMock.errors; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong log for error: %v != %v", act, exp)
	}
}

func TestLogWithFields(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeLog
	conf.Log.Message = "${!json(\"foo\")}"
	conf.Log.Fields = map[string]string{
		"static":  "foo",
		"dynamic": "${!json(\"bar\")}",
	}

	logMock := &mockLog{}

	conf.Log.Level = "INFO"
	l, err := New(conf, mock.NewManager(), logMock, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	input := message.QuickBatch([][]byte{[]byte(`{"foo":"info message","bar":"with fields"}`)})
	expMsgs := []*message.Batch{input}
	actMsgs, res := l.ProcessMessage(input)
	if res != nil {
		t.Fatal(res)
	}
	if !reflect.DeepEqual(expMsgs, actMsgs) {
		t.Errorf("Wrong message passthrough: %v != %v", actMsgs, expMsgs)
	}

	if exp, act := []string{"info message"}, logMock.infos; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong log output: %v != %v", act, exp)
	}
	t.Logf("Checking %v\n", logMock.fields)
	if exp, act := 1, len(logMock.fields); exp != act {
		t.Fatalf("Wrong count of fields: %v != %v", act, exp)
	}
	if exp, act := map[string]string{"dynamic": "with fields", "static": "foo"}, logMock.fields[0]; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong field output: %v != %v", act, exp)
	}

	input = message.QuickBatch([][]byte{[]byte(`{"foo":"info message 2","bar":"with fields 2"}`)})
	expMsgs = []*message.Batch{input}
	actMsgs, res = l.ProcessMessage(input)
	if res != nil {
		t.Fatal(res)
	}
	if !reflect.DeepEqual(expMsgs, actMsgs) {
		t.Errorf("Wrong message passthrough: %v != %v", actMsgs, expMsgs)
	}

	if exp, act := []string{"info message", "info message 2"}, logMock.infos; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong log output: %v != %v", act, exp)
	}
	t.Logf("Checking %v\n", logMock.fields)
	if exp, act := 2, len(logMock.fields); exp != act {
		t.Fatalf("Wrong count of fields: %v != %v", act, exp)
	}
	if exp, act := map[string]string{"dynamic": "with fields", "static": "foo"}, logMock.fields[0]; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong field output: %v != %v", act, exp)
	}
	if exp, act := map[string]string{"dynamic": "with fields 2", "static": "foo"}, logMock.fields[1]; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong field output: %v != %v", act, exp)
	}
}

func TestLogWithFieldsMapping(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeLog
	conf.Log.Message = "hello world"
	conf.Log.FieldsMapping = `
root.static = "static value"
root.age = this.age + 2
root.is_cool = this.is_cool`

	logMock := &mockLog{}

	conf.Log.Level = "INFO"
	l, err := New(conf, mock.NewManager(), logMock, metrics.Noop())
	require.NoError(t, err)

	input := message.QuickBatch([][]byte{[]byte(
		`{"age":10,"is_cool":true,"ignore":"this value please"}`,
	)})
	expMsgs := []*message.Batch{input}
	actMsgs, res := l.ProcessMessage(input)
	require.Nil(t, res)
	assert.Equal(t, expMsgs, actMsgs)

	assert.Equal(t, []string{"hello world"}, logMock.infos)
	assert.Equal(t, []interface{}{
		"age", int64(12),
		"is_cool", true,
		"static", "static value",
	}, logMock.mappingFields)
}

//------------------------------------------------------------------------------
