// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package log

import (
	"fmt"
	"testing"
)

type LogBuffer struct {
	data string
}

func (l *LogBuffer) Write(p []byte) (n int, err error) {
	l.data = fmt.Sprintf("%v%v", l.data, string(p))
	return len(p), nil
}

func TestModules(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.JSONFormat = false
	loggerConfig.Prefix = "root"
	loggerConfig.LogLevel = "WARN"

	buf := LogBuffer{data: ""}

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

	if expected != buf.data {
		t.Errorf("%v != %v", expected, buf.data)
	}
}

func TestFormattedLogging(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.JSONFormat = false
	loggerConfig.Prefix = "test"
	loggerConfig.LogLevel = "WARN"

	buf := LogBuffer{data: ""}

	logger := New(&buf, loggerConfig)
	logger.Fatalf("fatal test %v\n", 1)
	logger.Errorf("error test %v\n", 2)
	logger.Warnf("warn test %v\n", 3)
	logger.Infof("info test %v\n", 4)
	logger.Debugf("info test %v\n", 5)
	logger.Tracef("trace test %v\n", 6)

	expected := "FATAL | test | fatal test 1\nERROR | test | error test 2\nWARN | test | warn test 3\n"

	if expected != buf.data {
		t.Errorf("%v != %v", expected, buf.data)
	}
}

func TestLineLogging(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.JSONFormat = false
	loggerConfig.Prefix = "test"
	loggerConfig.LogLevel = "WARN"

	buf := LogBuffer{data: ""}

	logger := New(&buf, loggerConfig)
	logger.Fatalln("fatal test")
	logger.Errorln("error test")
	logger.Warnln("warn test")
	logger.Infoln("info test")
	logger.Debugln("info test")
	logger.Traceln("trace test")

	expected := "FATAL | test | fatal test\nERROR | test | error test\nWARN | test | warn test\n"

	if expected != buf.data {
		t.Errorf("%v != %v", expected, buf.data)
	}
}

type LogCounter struct {
	count int
}

func (l *LogCounter) Write(p []byte) (n int, err error) {
	l.count++
	return len(p), nil
}

func TestLogLevels(t *testing.T) {
	for i := 0; i < LogAll; i++ {
		loggerConfig := NewConfig()
		loggerConfig.JSONFormat = false
		loggerConfig.LogLevel = intToLogLevel(i)

		buf := LogCounter{count: 0}

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
