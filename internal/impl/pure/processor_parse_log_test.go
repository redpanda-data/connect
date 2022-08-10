package pure_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestParseLogCases(t *testing.T) {
	type testCase struct {
		name    string
		input   string
		output  string
		format  string
		codec   string
		bestEff bool
	}
	tests := []testCase{
		{
			name:    "valid syslog_rfc5424 input, valid json output",
			format:  "syslog_rfc5424",
			codec:   "json",
			bestEff: true,
			input:   `<42>4 2049-10-11T22:14:15.003Z toaster.smarthome myapp - 2 [home01 device_id="43"] failed to make a toast.`,
			output:  `{"appname":"myapp","facility":5,"hostname":"toaster.smarthome","message":"failed to make a toast.","msgid":"2","priority":42,"severity":2,"structureddata":{"home01":{"device_id":"43"}},"timestamp":"2049-10-11T22:14:15.003Z","version":4}`,
		},
		{
			name:    "invalid syslog_rfc5424 input, invalid json output",
			format:  "syslog_rfc5424",
			codec:   "json",
			bestEff: true,
			input:   `not a syslog at all.`,
			output:  `not a syslog at all.`,
		},
		{
			name:    "valid syslog_rfc3164 input, valid json output",
			format:  "syslog_rfc3164",
			codec:   "json",
			bestEff: true,
			input:   `<28>Dec  2 16:49:23 host app[23410]: Test`,
			output:  fmt.Sprintf(`{"appname":"app","facility":3,"hostname":"host","message":"Test","priority":28,"procid":"23410","severity":4,"timestamp":"%v-12-02T16:49:23Z"}`, time.Now().Year()),
		},
	}

	for _, test := range tests {
		conf := processor.NewConfig()
		conf.Type = "parse_log"
		conf.ParseLog.Format = test.format
		conf.ParseLog.Codec = test.codec
		conf.ParseLog.BestEffort = test.bestEff
		proc, err := mock.NewManager().NewProcessor(conf)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(test.name, func(tt *testing.T) {
			msgsOut, res := proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte(test.input)}))
			if res != nil {
				tt.Fatal(res)
			}
			if len(msgsOut) != 1 {
				tt.Fatalf("Wrong count of result messages: %v != 1", len(msgsOut))
			}
			if exp, act := test.output, string(msgsOut[0].Get(0).AsBytes()); exp != act {
				tt.Errorf("Wrong result: %v != %v", act, exp)
			}
		})
	}
}

func TestParseLogRFC5424(t *testing.T) {
	type testCase struct {
		name   string
		input  string
		output string
	}
	tests := []testCase{
		{
			name:   "valid syslog_rfc5424 1",
			input:  `<42>4 2049-10-11T22:14:15.003Z toaster.smarthome myapp - 2 [home01 device_id="43"] failed to make a toast.`,
			output: `{"appname":"myapp","facility":5,"hostname":"toaster.smarthome","message":"failed to make a toast.","msgid":"2","priority":42,"severity":2,"structureddata":{"home01":{"device_id":"43"}},"timestamp":"2049-10-11T22:14:15.003Z","version":4}`,
		},
		{
			name:   "valid syslog_rfc5424 2",
			input:  `<23>4 2032-10-11T22:14:15.003Z foo.bar baz - 10 [home02 device_id="44"] test log.`,
			output: `{"appname":"baz","facility":2,"hostname":"foo.bar","message":"test log.","msgid":"10","priority":23,"severity":7,"structureddata":{"home02":{"device_id":"44"}},"timestamp":"2032-10-11T22:14:15.003Z","version":4}`,
		},
	}

	conf := processor.NewConfig()
	conf.Type = "parse_log"
	conf.ParseLog.Format = "syslog_rfc5424"
	conf.ParseLog.BestEffort = true
	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			msgsOut, res := proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte(test.input)}))
			if res != nil {
				tt.Fatal(res)
			}
			if len(msgsOut) != 1 {
				tt.Fatalf("Wrong count of result messages: %v != 1", len(msgsOut))
			}
			if exp, act := test.output, string(msgsOut[0].Get(0).AsBytes()); exp != act {
				tt.Errorf("Wrong result: %v != %v", act, exp)
			}
		})
	}
}
