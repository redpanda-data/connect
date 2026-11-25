// Copyright 2026 Redpanda Data, Inc.
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

package sql

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestSQLRawProcessorTraceLogsStatementOnly(t *testing.T) {
	tests := []struct {
		name     string
		level    slog.Level
		contains []string
		expected int
	}{
		{
			name:     "logs the statement and argument count at trace level",
			level:    slog.LevelDebug,
			contains: []string{"INSERT INTO t (v) VALUES (?)", "(1 args)"},
			expected: 1,
		},
		{
			name:     "logs nothing at info level",
			level:    slog.LevelInfo,
			expected: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn := newTraceTestDSN(t)

			lines := traceLines(t, test.level, func(mgr *service.Resources) {
				conf, err := RawProcessorConfig().ParseYAML(fmt.Sprintf(`
driver: sqlite
dsn: %q
query: INSERT INTO t (v) VALUES (?)
args_mapping: 'root = [ this.v ]'
exec_only: true
`, dsn), nil)
				require.NoError(t, err)

				proc, err := NewSQLRawProcessorFromConfig(conf, mgr)
				require.NoError(t, err)
				t.Cleanup(func() { _ = proc.Close(t.Context()) })

				msg := service.NewMessage(nil)
				msg.SetStructured(map[string]any{"v": traceSecret})
				_, err = proc.ProcessBatch(t.Context(), service.MessageBatch{msg})
				require.NoError(t, err)
			})

			require.Len(t, lines, test.expected)
			for _, line := range lines {
				assert.NotContains(t, line, traceSecret)
				for _, c := range test.contains {
					assert.Contains(t, line, c)
				}
			}
		})
	}
}
