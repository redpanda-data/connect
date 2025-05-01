// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package questdb

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimestampConversions(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		value        int64
		unit         timestampUnit
		expectedTime time.Time
	}{
		{
			name:         "autoSecondsMin",
			value:        0,
			unit:         auto,
			expectedTime: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:         "autoSecondsMax",
			value:        9999999999,
			unit:         auto,
			expectedTime: time.Date(2286, 11, 20, 17, 46, 39, 0, time.UTC),
		},
		{
			name:         "autoMillisMin",
			value:        10000000000,
			unit:         auto,
			expectedTime: time.Date(1970, 4, 26, 17, 46, 40, 0, time.UTC),
		},
		{
			name:         "autoMillisMax",
			value:        9999999999999,
			unit:         auto,
			expectedTime: time.Date(2286, 11, 20, 17, 46, 39, 999000000, time.UTC),
		},
		{
			name:         "autoMicrosMin",
			value:        10000000000000,
			unit:         auto,
			expectedTime: time.Date(1970, 4, 26, 17, 46, 40, 0, time.UTC),
		},
		{
			name:         "autoMicrosMax",
			value:        9999999999999999,
			unit:         auto,
			expectedTime: time.Date(2286, 11, 20, 17, 46, 39, 999999000, time.UTC),
		},
		{
			name:         "autoNanosMin",
			value:        10000000000000000,
			unit:         auto,
			expectedTime: time.Date(1970, 4, 26, 17, 46, 40, 0, time.UTC),
		},
		{
			name:         "autoNanosMax",
			value:        math.MaxInt64,
			unit:         auto,
			expectedTime: time.Date(2262, 4, 11, 23, 47, 16, 854775807, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedTime, tc.unit.From(tc.value))
		})
	}
}

func TestFromConf(t *testing.T) {
	t.Parallel()

	configSpec := questdbOutputConfig()
	conf := `
table: test
address: "localhost:9000"
designated_timestamp_field: myDesignatedTimestamp
designated_timestamp_unit: nanos
timestamp_string_fields:
  - fieldA
  - fieldB
timestamp_string_format: 2006-01-02T15:04:05Z07:00 # rfc3339
symbols:
  - mySymbolA
  - mySymbolB
`
	parsed, err := configSpec.ParseYAML(conf, nil)
	require.NoError(t, err)

	out, _, _, err := fromConf(parsed, service.MockResources())
	require.NoError(t, err)

	w, ok := out.(*questdbWriter)
	require.True(t, ok)

	assert.Equal(t, "test", w.table)
	assert.Equal(t, "myDesignatedTimestamp", w.designatedTimestampField)
	assert.Equal(t, nanos, w.designatedTimestampUnit)
	assert.Equal(t, map[string]bool{"fieldA": true, "fieldB": true}, w.timestampStringFields)
	assert.Equal(t, time.RFC3339, w.timestampStringFormat)
	assert.Equal(t, map[string]bool{"mySymbolA": true, "mySymbolB": true}, w.symbols)

}

func TestValidationErrorsFromConf(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                string
		conf                string
		expectedErrContains string
	}{
		{
			name:                "no address",
			conf:                "table: test",
			expectedErrContains: "field 'address' is required",
		},
		{
			name:                "no table",
			conf:                `address: "localhost:9000"`,
			expectedErrContains: "field 'table' is required",
		},
		{
			name: "invalid timestamp unit",
			conf: `
address: "localhost:9000"
table: test
designated_timestamp_unit: hello`,
			expectedErrContains: "is not a valid timestamp unit",
		},
	}

	for _, tc := range testCases {
		configSpec := questdbOutputConfig()

		t.Run(tc.name, func(t *testing.T) {
			cfg, err := configSpec.ParseYAML(tc.conf, nil)
			if err != nil {
				assert.ErrorContains(t, err, tc.expectedErrContains)
				return
			}

			_, _, _, err = fromConf(cfg, service.MockResources())
			assert.ErrorContains(t, err, tc.expectedErrContains)

		})
	}
}

func TestOptionsOnWrite(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	sentMsgs := make(chan string, 4) // Arbitrary buffer size, > max number of test messages
	t.Cleanup(func() { close(sentMsgs) })

	// Set up mock QuestDB http server
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	s := http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			scanner := bufio.NewScanner(r.Body)
			for scanner.Scan() {
				sentMsgs <- scanner.Text()
			}
			assert.NoError(t, scanner.Err())
			w.WriteHeader(200)
		}),
	}
	t.Cleanup(func() {
		_ = s.Shutdown(ctx)
	})
	go func() {
		_ = s.Serve(listener)
	}()

	testCases := []struct {
		name          string
		extraConf     string
		payload       []string
		expectedLines []string
	}{
		{
			name:          "withSymbols",
			extraConf:     "symbols: ['hello']",
			payload:       []string{`{"hello": "world", "test": 1}`},
			expectedLines: []string{"withSymbols,hello=world test=1i"},
		},
		{
			name:      "withDesignatedTimestamp",
			extraConf: "designated_timestamp_field: timestamp",
			payload:   []string{`{"hello": "world", "timestamp": 1}`},
			expectedLines: []string{
				`withDesignatedTimestamp hello="world" 1000000000`,
			},
		},
		{
			name:      "withTimestampUnit",
			extraConf: "designated_timestamp_field: timestamp\ndesignated_timestamp_unit: nanos",
			payload:   []string{`{"hello": "world", "timestamp": 1}`},
			expectedLines: []string{
				`withTimestampUnit hello="world" 1`,
			},
		},
		{
			name:      "withTimestampStringFields",
			extraConf: "timestamp_string_fields: ['timestamp']\ntimestamp_string_format: 2006-02-01",
			payload:   []string{`{"timestamp": "1970-01-02"}`},
			expectedLines: []string{
				`withTimestampStringFields timestamp=2678400000000t`,
			},
		},
		{
			name:      "withBoolValue",
			extraConf: "timestamp_string_fields: ['timestamp']\ntimestamp_string_format: 2006-02-01",
			payload:   []string{`{"hello": true}`},
			expectedLines: []string{
				`withBoolValue hello=t`,
			},
		},
		{
			name:      "withDoubles",
			extraConf: "doubles: ['hello']",
			payload:   []string{`{"hello": 1.23}`},
			expectedLines: []string{
				`withDoubles hello=1.23`,
			},
		},
	}

	for _, tc := range testCases {
		conf := fmt.Sprintf("address: 'localhost:%d'\n", listener.Addr().(*net.TCPAddr).Port)
		conf += fmt.Sprintf("table: '%s'\n", tc.name)
		conf += tc.extraConf

		configSpec := questdbOutputConfig()

		cfg, err := configSpec.ParseYAML(conf, nil)
		require.NoError(t, err)
		w, _, _, err := fromConf(cfg, service.MockResources())
		require.NoError(t, err)

		qdbWriter := w.(*questdbWriter)
		batch := service.MessageBatch{}
		for _, msg := range tc.payload {
			batch = append(batch, service.NewMessage([]byte(msg)))
		}
		assert.NoError(t, qdbWriter.WriteBatch(ctx, batch))
		for _, l := range tc.expectedLines {
			assert.Equal(t, l, <-sentMsgs)
		}
	}
}
