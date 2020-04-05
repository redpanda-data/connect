package processor

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type mockLog struct {
	traces []string
	debugs []string
	infos  []string
	warns  []string
	errors []string
	fields []map[string]string
}

func (m *mockLog) NewModule(prefix string) log.Modular { return m }
func (m *mockLog) WithFields(fields map[string]string) log.Modular {
	m.fields = append(m.fields, fields)
	return m
}

func (m *mockLog) Fatalf(format string, v ...interface{}) {}
func (m *mockLog) Errorf(format string, v ...interface{}) {}
func (m *mockLog) Warnf(format string, v ...interface{})  {}
func (m *mockLog) Infof(format string, v ...interface{})  {}
func (m *mockLog) Debugf(format string, v ...interface{}) {}
func (m *mockLog) Tracef(format string, v ...interface{}) {}

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

	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("expected err from bad log level")
	}
}

func TestLogLevelTrace(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeLog
	conf.Log.Message = "${!json_field:foo}"

	logMock := &mockLog{}

	levels := []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR"}
	for _, level := range levels {
		conf.Log.Level = level
		l, err := New(conf, nil, logMock, metrics.Noop())
		if err != nil {
			t.Fatal(err)
		}

		input := message.New([][]byte{[]byte(fmt.Sprintf(`{"foo":"%v"}`, level))})
		expMsgs := []types.Message{input}
		actMsgs, res := l.ProcessMessage(input)
		if res != nil {
			t.Fatal(res.Error())
		}
		if !reflect.DeepEqual(expMsgs, actMsgs) {
			t.Errorf("Wrong message passthrough: %s != %s", actMsgs, expMsgs)
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
	conf.Log.Message = "${!json_field:foo}"
	conf.Log.Fields = map[string]string{
		"static":  "foo",
		"dynamic": "${!json_field:bar}",
	}

	logMock := &mockLog{}

	conf.Log.Level = "INFO"
	l, err := New(conf, nil, logMock, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	input := message.New([][]byte{[]byte(`{"foo":"info message","bar":"with fields"}`)})
	expMsgs := []types.Message{input}
	actMsgs, res := l.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}
	if !reflect.DeepEqual(expMsgs, actMsgs) {
		t.Errorf("Wrong message passthrough: %s != %s", actMsgs, expMsgs)
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

	input = message.New([][]byte{[]byte(`{"foo":"info message 2","bar":"with fields 2"}`)})
	expMsgs = []types.Message{input}
	actMsgs, res = l.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}
	if !reflect.DeepEqual(expMsgs, actMsgs) {
		t.Errorf("Wrong message passthrough: %s != %s", actMsgs, expMsgs)
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

//------------------------------------------------------------------------------
