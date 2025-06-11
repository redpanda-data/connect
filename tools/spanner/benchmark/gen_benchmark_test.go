package benchmark

import (
	_ "embed"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"text/template"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/changestreamstest"
)

type BenchmarkTableHelper struct {
	changestreamstest.RealHelper
	t *testing.T

	desc    string
	payload []byte
}

func (h BenchmarkTableHelper) CreateTableAndStream() {
	h.RealHelper.CreateTableAndStream(`CREATE TABLE %s (
  Id            STRING(36) NOT NULL,             -- UUIDv4 (36 chars)
  CreatedAt     TIMESTAMP NOT NULL,              -- 12 bytes
  UpdatedAt     TIMESTAMP NOT NULL,              -- 12 bytes
  IsActive      BOOL NOT NULL,                   -- 1 byte
  Status        STRING(10),                      -- ~10 bytes
  Score         FLOAT64,                         -- 8 bytes
  Category      STRING(20),                      -- ~20 bytes
  Description   STRING(200),                     -- ~200 bytes
  Payload       BYTES(512),                      -- 512 bytes (fixed-size payload)
  Note          STRING(128),                     -- ~128 bytes
) PRIMARY KEY(Id)`)
}

func (h BenchmarkTableHelper) InsertRowsInTransaction(n int) time.Time {
	muts := make([]*spanner.Mutation, n)
	for i := 0; i < n; i++ {
		muts[i] = h.insertMut()
	}

	ts, err := h.Client().Apply(h.t.Context(),
		muts,
		spanner.TransactionTag("app=rpcn;action=insert"))
	require.NoError(h.t, err)

	return ts
}

func (h BenchmarkTableHelper) insertMut() *spanner.Mutation {
	score := math.Round(rand.Float64() * 100)
	return spanner.Insert(h.Table(), []string{
		"Id",
		"CreatedAt",
		"UpdatedAt",
		"IsActive",
		"Status",
		"Score",
		"Category",
		"Description",
		"Payload",
		"Note",
	}, []any{
		uuid.New().String(),
		time.Now(),
		time.Now(),
		true,
		fmt.Sprintf("Active"),
		score,
		fmt.Sprintf("Category"),
		h.desc,
		h.payload,
		h.desc[0:int(score)],
	})
}

//go:embed config.tmpl.yml
var configTemplate []byte

var (
	outputConfigFileFlag = flag.String("output-config-file", "./config.yml", "output config file")
	skipCreateTableFlag  = flag.Bool("skip-create-table", false, "skip create table")
)

func TestBenchmarkInsert10MRows(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}

	integration.CheckSkip(t)
	changestreamstest.CheckSkipReal(t)

	h := BenchmarkTableHelper{
		RealHelper: changestreamstest.MakeRealHelperWithTableName(t, "rpcn_benchmark_10m_table", "rpcn_benchmark_10m_stream"),
		t:          t,
		desc:       string(slices.Repeat([]byte("d"), 180)),
		payload:    slices.Repeat([]byte("a"), 500),
	}
	defer h.Close()

	if !*skipCreateTableFlag {
		h.CreateTableAndStream()
	}

	const (
		targetRows      = 10_000_000
		batchSize       = 10
		numWorkers      = 100
		progressReportN = 10_000
	)

	var (
		rowsInserted = rowsCount(t, h)

		wg        sync.WaitGroup
		startTime = time.Now()
	)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				if cur := atomic.LoadInt64(&rowsInserted); cur >= targetRows {
					return
				}

				h.InsertRowsInTransaction(batchSize)

				if cnt := atomic.AddInt64(&rowsInserted, batchSize); cnt%progressReportN < batchSize {
					elapsed := time.Since(startTime)
					rowsPerSec := float64(cnt) / elapsed.Seconds()
					t.Logf("Progress: %d/%d rows (%.2f rows/sec)", cnt, targetRows, rowsPerSec)
				}
			}
		}()
	}

	wg.Wait()
	endTime := time.Now()

	elapsed := endTime.Sub(startTime)
	rowsPerSec := float64(targetRows) / elapsed.Seconds()
	t.Logf("Benchmark completed: inserted %d rows in %v (%.2f rows/sec)",
		targetRows, elapsed, rowsPerSec)
	minCreateTime, maxCreateTime := createdAtRange(t, h)

	if !flag.Parsed() {
		flag.Parse()
	}
	f, err := os.Create(*outputConfigFileFlag)
	require.NoError(t, err)
	defer f.Close()

	t.Logf("Writing config to %s", f.Name())

	tmpl, err := template.New("config").Parse(string(configTemplate))
	require.NoError(t, err)

	require.NoError(t, tmpl.Execute(f, struct {
		ProjectID      string
		InstanceID     string
		DatabaseID     string
		StreamID       string
		StartTimestamp string
		EndTimestamp   string
	}{
		ProjectID:      h.ProjectID(),
		InstanceID:     h.InstanceID(),
		DatabaseID:     h.DatabaseID(),
		StreamID:       h.Stream(),
		StartTimestamp: minCreateTime.Format(time.RFC3339),
		EndTimestamp:   maxCreateTime.Add(time.Second).Format(time.RFC3339), // end timestamp is exclusive
	}))
	require.NoError(t, f.Close())
}

func rowsCount(t *testing.T, h BenchmarkTableHelper) int64 {
	stmt := spanner.Statement{SQL: fmt.Sprintf("SELECT COUNT(*) FROM %s", h.Table())}
	iter := h.Client().Single().Query(t.Context(), stmt)
	defer iter.Stop()

	row, err := iter.Next()
	require.NoError(t, err)

	var count int64
	require.NoError(t, row.Columns(&count))
	return count
}

func createdAtRange(t *testing.T, h BenchmarkTableHelper) (min, max time.Time) {
	stmt := spanner.Statement{SQL: fmt.Sprintf("SELECT MIN(CreatedAt) AS min_created_at, MAX(CreatedAt) AS max_created_at FROM %s", h.Table())}
	iter := h.Client().Single().Query(t.Context(), stmt)
	defer iter.Stop()

	row, err := iter.Next()
	require.NoError(t, err)

	require.NoError(t, row.Columns(&min, &max))
	return min, max
}
