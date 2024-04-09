package pure_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type mockLog struct {
	traces        []string
	debugs        []string
	infos         []string
	warns         []string
	errors        []string
	fields        []map[string]string
	mappingFields []any
}

func (m *mockLog) NewModule(prefix string) log.Modular { return m }
func (m *mockLog) WithFields(fields map[string]string) log.Modular {
	m.fields = append(m.fields, fields)
	return m
}

func (m *mockLog) With(args ...any) log.Modular {
	m.mappingFields = append(m.mappingFields, args...)
	return m
}

func (m *mockLog) Fatal(format string, v ...any) {}
func (m *mockLog) Error(format string, v ...any) {
	m.errors = append(m.errors, fmt.Sprintf(format, v...))
}

func (m *mockLog) Warn(format string, v ...any) {
	m.warns = append(m.warns, fmt.Sprintf(format, v...))
}

func (m *mockLog) Info(format string, v ...any) {
	m.infos = append(m.infos, fmt.Sprintf(format, v...))
}

func (m *mockLog) Debug(format string, v ...any) {
	m.debugs = append(m.debugs, fmt.Sprintf(format, v...))
}

func (m *mockLog) Trace(format string, v ...any) {
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

func TestLogBadLevel(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
log:
  level: does not exist
`)
	require.NoError(t, err)

	if _, err := mock.NewManager().NewProcessor(conf); err == nil {
		t.Error("expected err from bad log level")
	}
}

func TestLogLevelTrace(t *testing.T) {
	logMock := &mockLog{}

	levels := []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR"}
	for _, level := range levels {
		conf, err := testutil.ProcessorFromYAML(`
log:
  message: '${!json("foo")}'
  level: ` + level + `
`)
		require.NoError(t, err)

		mgr := mock.NewManager()
		mgr.L = logMock

		l, err := mgr.NewProcessor(conf)
		if err != nil {
			t.Fatal(err)
		}

		input := message.QuickBatch([][]byte{[]byte(fmt.Sprintf(`{"foo":"%v"}`, level))})
		expMsgs := []message.Batch{input}
		actMsgs, res := l.ProcessBatch(context.Background(), input)
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
	conf, err := testutil.ProcessorFromYAML(`
log:
  message: '${!json("foo")}'
  level: INFO
  fields:
    static: foo
    dynamic: '${!json("bar")}'
`)
	require.NoError(t, err)

	logMock := &mockLog{}

	mgr := mock.NewManager()
	mgr.L = logMock

	l, err := mgr.NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	input := message.QuickBatch([][]byte{[]byte(`{"foo":"info message","bar":"with fields"}`)})
	expMsgs := []message.Batch{input}
	actMsgs, res := l.ProcessBatch(context.Background(), input)
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
	expMsgs = []message.Batch{input}
	actMsgs, res = l.ProcessBatch(context.Background(), input)
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
	conf, err := testutil.ProcessorFromYAML(`
log:
  message: 'hello world'
  level: INFO
  fields_mapping: |
    root.static = "static value"
    root.age = this.age + 2
    root.is_cool = this.is_cool
`)
	require.NoError(t, err)

	logMock := &mockLog{}

	mgr := mock.NewManager()
	mgr.L = logMock

	l, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	input := message.QuickBatch([][]byte{[]byte(
		`{"age":10,"is_cool":true,"ignore":"this value please"}`,
	)})
	expMsgs := []message.Batch{input}
	actMsgs, res := l.ProcessBatch(context.Background(), input)
	require.NoError(t, res)
	assert.Equal(t, expMsgs, actMsgs)

	assert.Equal(t, []string{"hello world"}, logMock.infos)
	assert.Equal(t, []any{
		"age", int64(12),
		"is_cool", true,
		"static", "static value",
	}, logMock.mappingFields)
}
