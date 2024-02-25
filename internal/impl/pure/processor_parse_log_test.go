package pure_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestParseLogCases(t *testing.T) {
	type testCase struct {
		name    string
		input   string
		output  string
		format  string
		bestEff bool
	}
	tests := []testCase{
		{
			name:    "valid syslog_rfc5424 input, valid json output",
			format:  "syslog_rfc5424",
			bestEff: true,
			input:   `<42>4 2049-10-11T22:14:15.003Z toaster.smarthome myapp - 2 [home01 device_id="43"] failed to make a toast.`,
			output:  `{"appname":"myapp","facility":5,"hostname":"toaster.smarthome","message":"failed to make a toast.","msgid":"2","priority":42,"severity":2,"structureddata":{"home01":{"device_id":"43"}},"timestamp":"2049-10-11T22:14:15.003Z","version":4}`,
		},
		{
			name:    "invalid syslog_rfc5424 input, invalid json output",
			format:  "syslog_rfc5424",
			bestEff: true,
			input:   `not a syslog at all.`,
			output:  `not a syslog at all.`,
		},
		{
			name:    "valid syslog_rfc3164 input, valid json output",
			format:  "syslog_rfc3164",
			bestEff: true,
			input:   `<28>Dec  2 16:49:23 host app[23410]: Test`,
			output:  fmt.Sprintf(`{"appname":"app","facility":3,"hostname":"host","message":"Test","priority":28,"procid":"23410","severity":4,"timestamp":"%v-12-02T16:49:23Z"}`, time.Now().Year()),
		},
	}

	for _, test := range tests {
		conf, err := testutil.ProcessorFromYAML(fmt.Sprintf(`
parse_log:
  format: %v
  best_effort: %v
`, test.format, test.bestEff))
		require.NoError(t, err)

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

	conf, err := testutil.ProcessorFromYAML(`
parse_log:
  format: syslog_rfc5424
  best_effort: true
`)
	require.NoError(t, err)

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

			exe, err := bloblang.Parse(`json("structureddata").map_each(i -> if i.value.type() == "unknown" { throw("kaboom!") })`)
			if err != nil {
				tt.Errorf("Failed to parse bloblang: %s", err)
			}
			if _, err := service.NewInternalMessage(msgsOut[0].Get(0)).BloblangQuery(exe); err != nil {
				tt.Errorf("Invalid structureddata field: %s", err)
			}
		})
	}
}
