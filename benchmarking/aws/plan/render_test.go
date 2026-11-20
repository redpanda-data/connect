package plan

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "rewrite golden files")

func httpToDrop() map[string]any {
	return map[string]any{
		"input":  map[string]any{"http_server": map[string]any{"path": "/post"}},
		"output": map[string]any{"drop": map[string]any{}},
	}
}

func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, got, 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
}

func TestRenderPostgresToSnowflakeGolden(t *testing.T) {
	p := build(t, pgToSnowflake())
	p.ID = "aaaaaaaaaaaa"
	out, err := Render(p)
	require.NoError(t, err)
	checkGolden(t, "bot-aaaaaaaaaaaa.yaml", out)
}

func TestRenderApproximatedToDropGolden(t *testing.T) {
	p := build(t, httpToDrop())
	p.ID = "bbbbbbbbbbbb"
	require.Equal(t, SourceApproximated, p.Source)
	out, err := Render(p)
	require.NoError(t, err)
	checkGolden(t, "bot-bbbbbbbbbbbb.yaml", out)
}

func TestRenderRejectsUnrunnable(t *testing.T) {
	_, err := Render(Plan{ID: "x", Runnable: false, Reason: "no bench stack for oracledb_cdc yet"})
	require.ErrorContains(t, err, "no bench stack for oracledb_cdc yet")
}

func TestRenderDefaultsRowSize(t *testing.T) {
	p := build(t, httpToDrop())
	p.Workload.MessageSizeBytes = 0
	out, err := Render(p)
	require.NoError(t, err)
	require.Contains(t, string(out), "row_size_bytes: 1024")
}

func TestRenderMySQLUsesMySQLBase(t *testing.T) {
	p := build(t, pgToSnowflake())
	p.Source, p.Input = SourceMySQL, "mysql_cdc"
	out, err := Render(p)
	require.NoError(t, err)
	s := string(out)
	require.Contains(t, s, "stack: mysql")
	require.Contains(t, s, "cdc-rows-mysql")
	require.Contains(t, s, "binlog retention hours")
	require.Contains(t, s, "instance_type: c8g.4xlarge")
}

func TestPreloadMessages(t *testing.T) {
	// 300 MB/s * 1020s * 1.5 / 1000 B = 459,000,000
	require.Equal(t, int64(459_000_000), PreloadMessages(1000, 2*time.Minute, 15*time.Minute, 300))
	// peak defaults to 300 when <= 0
	require.Equal(t, int64(459_000_000), PreloadMessages(1000, 2*time.Minute, 15*time.Minute, 0))
	// rounds up: 1 MB/s * 60s * 1.5 / 7 B = 12857142.86
	require.Equal(t, int64(12_857_143), PreloadMessages(7, time.Minute, 0, 1))
}
