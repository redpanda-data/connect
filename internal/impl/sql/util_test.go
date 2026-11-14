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
	"bytes"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "modernc.org/sqlite"
)

const (
	traceLogPrefix = "Executing query"
	traceSecret    = "s3cr3t-value"
)

// newTraceTestDSN creates an in-memory sqlite database containing a single
// table "t" with a text column "v", and returns a DSN that connects to it. The
// database is dropped once the test finishes.
func newTraceTestDSN(t *testing.T) string {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	keeper, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = keeper.Close() })

	_, err = keeper.ExecContext(t.Context(), `CREATE TABLE t (v TEXT)`)
	require.NoError(t, err)
	return dsn
}

// traceLines runs fn against a resources object whose logger records at the
// given level, and returns the query log lines it produced.
func traceLines(t *testing.T, level slog.Level, fn func(mgr *service.Resources)) []string {
	t.Helper()

	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: level}))
	fn(service.MockResources(service.MockResourcesOptUseLogger(service.NewLoggerFromSlog(l))))

	var lines []string
	for line := range strings.SplitSeq(buf.String(), "\n") {
		if strings.Contains(line, traceLogPrefix) {
			lines = append(lines, line)
		}
	}
	return lines
}
