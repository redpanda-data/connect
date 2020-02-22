package processor

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
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
			output:  `{"appname":"app","facility":3,"hostname":"host","message":"Test","priority":28,"procid":"23410","severity":4,"timestamp":"2020-12-02T16:49:23Z"}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.ParseLog.Format = test.format
		conf.ParseLog.Codec = test.codec
		conf.ParseLog.BestEffort = test.bestEff
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
