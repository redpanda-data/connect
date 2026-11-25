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

func TestSQLSelectProcessorTraceLogsStatementOnly(t *testing.T) {
	dsn := newTraceTestDSN(t)

	lines := traceLines(t, slog.LevelDebug, func(mgr *service.Resources) {
		conf, err := SelectProcessorConfig().ParseYAML(fmt.Sprintf(`
driver: sqlite
dsn: %q
table: t
columns: [ v ]
where: v = ?
args_mapping: 'root = [ this.v ]'
`, dsn), nil)
		require.NoError(t, err)

		proc, err := NewSQLSelectProcessorFromConfig(conf, mgr)
		require.NoError(t, err)
		t.Cleanup(func() { _ = proc.Close(t.Context()) })

		msg := service.NewMessage(nil)
		msg.SetStructured(map[string]any{"v": traceSecret})
		_, err = proc.ProcessBatch(t.Context(), service.MessageBatch{msg})
		require.NoError(t, err)
	})

	require.Len(t, lines, 1)
	assert.NotContains(t, lines[0], traceSecret)
	assert.Contains(t, lines[0], "SELECT v FROM t WHERE v = ?")
	assert.Contains(t, lines[0], "(1 args)")
}
