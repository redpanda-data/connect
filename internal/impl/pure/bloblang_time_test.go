package pure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestTimestampMethods(t *testing.T) {
	tests := []struct {
		name               string
		mapping            string
		input              interface{}
		output             interface{}
		parseErrorContains string
		execErrorContains  string
	}{
		{
			name:    "timestamp_round by hour",
			mapping: `root = this.timestamp_round("1h".parse_duration())`,
			input:   "2020-08-14T05:54:23Z",
			output:  "2020-08-14T06:00:00Z",
		},
		{
			name:    "timestamp_round by minute",
			mapping: `root = this.timestamp_round("1m".parse_duration())`,
			input:   "2020-08-14T05:54:23Z",
			output:  "2020-08-14T05:54:00Z",
		},
		{
			name:              "timestamp_round bad timestamp",
			mapping:           `root = this.timestamp_round("1h".parse_duration())`,
			input:             "not a timestamp",
			execErrorContains: "parsing time \"not a timestamp\" as",
		},
		{
			name:               "timestamp_round bad timestamp static",
			mapping:            `root = "not a timestamp".timestamp_round("1h".parse_duration())`,
			parseErrorContains: "parsing time \"not a timestamp\" as",
		},
		{
			name:    "check parse_timestamp with format",
			mapping: `root = "2020-Aug-14".parse_timestamp("2006-Jan-02")`,
			output:  "2020-08-14T00:00:00Z",
		},
		{
			name:              "check parse_timestamp invalid",
			mapping:           `root = this.parse_timestamp("2006-01-02T15:04:05Z07:00")`,
			input:             "not valid timestamp",
			execErrorContains: `parsing time "not valid timestamp" as "2006-01-02T15:04:05Z07:00": cannot parse "not valid timestamp" as "2006"`,
		},
		{
			name:               "check parse_timestamp invalid static",
			mapping:            `root = "not valid timestamp".parse_timestamp("2006-01-02T15:04:05Z07:00")`,
			parseErrorContains: `parsing time "not valid timestamp" as "2006-01-02T15:04:05Z07:00": cannot parse "not valid timestamp" as "2006"`,
		},
		{
			name:               "check parse_timestamp with invalid format",
			mapping:            `root = "invalid format".parse_timestamp("2006-Jan-02")`,
			parseErrorContains: `parsing time "invalid format" as "2006-Jan-02": cannot parse "invalid format" as "2006"`,
		},
		{
			name:               "check parse_timestamp with invalid literal type",
			mapping:            `root = 1.parse_timestamp("2006-Jan-02")`,
			parseErrorContains: `expected string value, got number (1)`,
		},
		{
			name:    "check parse_timestamp_strptime with format",
			mapping: `root = "2020-Aug-14".parse_timestamp_strptime("%Y-%b-%d")`,
			output:  "2020-08-14T00:00:00Z",
		},
		{
			name:               "check parse_timestamp_strptime invalid",
			mapping:            `root = "not valid timestamp".parse_timestamp_strptime("%Y-%b-%d")`,
			parseErrorContains: `failed to parse "not valid timestamp" with "%Y-%b-%d": cannot parse %Y`,
		},
		{
			name:               "check parse_timestamp_strptime with invalid format",
			mapping:            `root = "invalid format".parse_timestamp_strptime("INVALID_FORMAT")`,
			parseErrorContains: `failed to parse "invalid format" with "INVALID_FORMAT": expected 'I'`,
		},
		{
			name:               "check parse_timestamp_strptime with invalid literal type",
			mapping:            `root = 1.parse_timestamp_strptime("%Y-%b-%d")`,
			parseErrorContains: `expected string value, got number`,
		},
		{
			name:    "check format_timestamp string default",
			mapping: `root = "2020-08-14T11:45:26.371+01:00".format_timestamp("2006-01-02T15:04:05.999999999Z07:00")`,
			output:  "2020-08-14T11:45:26.371+01:00",
		},
		{
			name:    "check format_timestamp string",
			mapping: `root = "2020-08-14T11:45:26.371+00:00".format_timestamp("2006-Jan-02 15:04:05.999999")`,
			output:  "2020-Aug-14 11:45:26.371",
		},
		{
			name:    "check format_timestamp unix float",
			mapping: `root = 1597405526.123456.format_timestamp("2006-Jan-02 15:04:05.999999", "UTC")`,
			output:  "2020-Aug-14 11:45:26.123456",
		},
		{
			name:    "check format_timestamp unix",
			mapping: `root = 1597405526.format_timestamp("2006-Jan-02 15:04:05", "UTC")`,
			output:  "2020-Aug-14 11:45:26",
		},
		{
			name:    "check format_timestamp_unix",
			mapping: `root = "2009-11-10T23:00:00Z".format_timestamp_unix()`,
			output:  int64(1257894000),
		},
		{
			name:    "check format_timestamp_unix_nano",
			mapping: `root = "2009-11-10T23:00:00Z".format_timestamp_unix_nano()`,
			output:  int64(1257894000000000000),
		},
		{
			name:    "check format_timestamp_strftime string",
			mapping: `root = "2020-08-14T11:45:26.371+01:00".format_timestamp_strftime("%Y-%b-%d %H:%M:%S")`,
			output:  "2020-Aug-14 11:45:26",
		},
		{
			name:    "check format_timestamp_strftime float",
			mapping: `root = 1597405526.123456.format_timestamp_strftime("%Y-%b-%d %H:%M:%S", "UTC")`,
			output:  "2020-Aug-14 11:45:26",
		},
		{
			name:    "check format_timestamp_strftime unix",
			mapping: `root = 1597405526.format_timestamp_strftime("%Y-%b-%d %H:%M:%S", "UTC")`,
			output:  "2020-Aug-14 11:45:26",
		},
		{
			name:    "check parse duration ISO-8601",
			mapping: `root = "P3Y6M4DT12H30M5.3S".parse_duration_iso8601()`,
			output:  int64(110839937300000000),
		},
		{
			name:    "check parse duration ISO-8601 ignore more than one decimal place",
			mapping: `root = "P3Y6M4DT12H30M5.33S".parse_duration_iso8601()`,
			output:  int64(110839937300000000),
		},
		{
			name:               "check parse duration ISO-8601 only allow fractions in the last field",
			mapping:            `root = "P2.5YT7.5S".parse_duration_iso8601()`,
			parseErrorContains: "P2.5YT7.5S: 'Y' & 'S' only the last field can have a fraction",
		},
		{
			name:               "check parse duration ISO-8601 with invalid format",
			mapping:            `root = "P3S".parse_duration_iso8601()`,
			parseErrorContains: "P3S: 'S' designator cannot occur here",
		},
		{
			name:               "check parse duration ISO-8601 with bogus format",
			mapping:            `root = "gibberish".parse_duration_iso8601()`,
			parseErrorContains: "gibberish: expected 'P' period mark at the start",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			m, err := bloblang.Parse(test.mapping)
			if test.parseErrorContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.parseErrorContains)
			} else {
				require.NoError(t, err)
				v, err := m.Query(test.input)
				if test.execErrorContains != "" {
					require.Error(t, err)
					assert.Contains(t, err.Error(), test.execErrorContains)
				} else {
					require.NoError(t, err)
					assert.Equal(t, test.output, v)
				}
			}
		})
	}
}
