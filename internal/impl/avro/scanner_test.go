// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package avro

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestScanner(t *testing.T) {
	tests := []struct {
		name    string
		rawJSON bool
		output  []string
	}{
		{
			name:    "standard JSON",
			rawJSON: false,
			output: []string{
				`{"Price":{"double":12.32},"OrderDate":{"long.timestamp-millis":1687221496000},"OrderStatus":{"string":"Canceled"},"Email":{"string":"elizabeth.brown@example.com"},"Quantity":{"long":5}}`,
				`{"Email":{"string":"james.wilson@example.com"},"Quantity":{"long":5},"Price":{"double":12.35},"OrderDate":{"long.timestamp-millis":1702926589000},"OrderStatus":{"string":"Pending"}}`,
				`{"OrderDate":{"long.timestamp-millis":1708606337000},"OrderStatus":{"string":"Completed"},"Email":{"string":"kristin.walls@example.com"},"Quantity":{"long":6},"Price":{"double":10.3}}`,
			},
		},
		{
			name:    "AVRO JSON",
			rawJSON: true,
			output: []string{
				`{"Email":"elizabeth.brown@example.com","OrderDate":1.687221496e+12,"OrderStatus":"Canceled","Price":12.32,"Quantity":5}`,
				`{"Email":"james.wilson@example.com","OrderDate":1.702926589e+12,"OrderStatus":"Pending","Price":12.35,"Quantity":5}`,
				`{"Email":"kristin.walls@example.com","OrderDate":1.708606337e+12,"OrderStatus":"Completed","Price":10.3,"Quantity":6}`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
			pConf, err := confSpec.ParseYAML(fmt.Sprintf(`
test:
  avro:
    raw_json: %t
`, test.rawJSON), nil)
			require.NoError(t, err)

			rdr, err := pConf.FieldScanner("test")
			require.NoError(t, err)

			b, err := os.ReadFile("./resources/ocf.avro")
			require.NoError(t, err)

			buf := bytes.NewReader(b)
			var acked bool
			strm, err := rdr.Create(io.NopCloser(buf), func(ctx context.Context, err error) error {
				acked = true
				return nil
			}, service.NewScannerSourceDetails())
			require.NoError(t, err)

			for _, s := range test.output {
				m, aFn, err := strm.NextBatch(context.Background())
				require.NoError(t, err)
				require.Len(t, m, 1)
				mBytes, err := m[0].AsBytes()
				require.NoError(t, err)
				assert.JSONEq(t, s, string(mBytes))
				require.NoError(t, aFn(context.Background(), nil))
				assert.False(t, acked)
			}

			_, _, err = strm.NextBatch(context.Background())
			require.Equal(t, io.EOF, err)

			require.NoError(t, strm.Close(context.Background()))
			assert.True(t, acked)
		})
	}
}
