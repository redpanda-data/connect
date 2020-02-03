package processor

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestParseLogCases(t *testing.T) {
	type testCase struct {
		name   string
		input  string
		output string
		format string
		codec  string
	}
	tests := []testCase{
		{
			name:   "valid input, valid json output",
			format: "syslog_rfc5424",
			codec:  "json",
			input:  `<42>4 2049-10-11T22:14:15.003Z toaster.smarthome myapp - 2 [home01 device_id="43"] failed to make a toast.`,
			output: `{"appname":"myapp","hostname":"toaster.smarthome","message":"failed to make a toast.","msgid":"2","structureddata":{"home01":{"device_id":"43"}},"timestamp":"2049-10-11T22:14:15.003Z"}`,
		},
		{
			name:   "invalid input, invalid json output",
			format: "syslog_rfc5424",
			codec:  "json",
			input:  `not a syslog at all.`,
			output: `not a syslog at all.`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.ParseLog.Format = test.format
		conf.ParseLog.Codec = test.codec
		proc, err := NewParseLog(conf, nil, log.Noop(), metrics.Noop())
		if err != nil {
			t.Fatal(err)
		}
		t.Run(test.name, func(tt *testing.T) {
			msgsOut, res := proc.ProcessMessage(message.New([][]byte{[]byte(test.input)}))
			if res != nil {
				tt.Fatal(res.Error())
			}
			if len(msgsOut) != 1 {
				tt.Fatalf("Wrong count of result messages: %v != 1", len(msgsOut))
			}
			if exp, act := test.output, string(msgsOut[0].Get(0).Get()); exp != act {
				tt.Errorf("Wrong result: %v != %v", act, exp)
			}
		})
	}
}
