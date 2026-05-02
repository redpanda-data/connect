// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"
)

// BenchmarkWriteBatch measures end-to-end WriteBatch throughput against either
// the goccy bigquery emulator (default) or a real BigQuery project (when the
// BQ_BENCH_PROJECT env var is set).
//
// EMULATOR MODE (default): in-process emulator with no quota and a localhost
// gRPC dial. Useful for catching local-hot-path regressions; absolute numbers
// are NOT representative of production capacity.
//
// REAL-BQ MODE: set the following env vars to hit a real BigQuery project.
// Costs apply: BigQuery Storage Write API charges ~$0.025 per GB ingested,
// so a -benchtime=2s run with batch=1000 (~70 KB / batch, 200 batches) writes
// roughly 14 MB and costs a fraction of a cent. Watch your -benchtime if you
// scale up.
//
//	BQ_BENCH_PROJECT          required: GCP project ID
//	BQ_BENCH_DATASET          required: existing BigQuery dataset
//	BQ_BENCH_CREDENTIALS_JSON optional: JSON service-account key contents.
//	                          If unset, ADC is used (gcloud auth application-default login).
//	BQ_BENCH_TABLE_PREFIX     optional: prefix for the per-batch-size benchmark
//	                          tables (default "bqwa_bench"). Tables are created
//	                          if absent and reused across runs.
//
// Run with (Docker required for emulator mode):
//
//	# Emulator
//	go test -run=^BenchmarkWriteBatch$ -bench=^BenchmarkWriteBatch$ -benchtime=2s \
//	  ./internal/impl/gcp/enterprise/bigquery/
//
//	# Real BigQuery
//	BQ_BENCH_PROJECT=my-proj BQ_BENCH_DATASET=bench \
//	  go test -run=^BenchmarkWriteBatch$ -bench=^BenchmarkWriteBatch$ -benchtime=2s \
//	  ./internal/impl/gcp/enterprise/bigquery/
//
// The -run flag must reference the benchmark name (not ^$) because
// integration.CheckSkip also reads -run; -run=^$ would skip before the
// benchmark starts.
//
// Reported metrics:
//   - rows/s   : message rows successfully written per second (aggregate)
//   - MB/s     : payload throughput (raw JSON bytes / wall time, via b.SetBytes)
//   - ms/batch : average WriteBatch wall-clock latency
//   - p50/p95/p99-ms : per-batch latency percentiles (real-BQ mode only,
//     where network variance is large enough to make tail latency interesting)
//
// Sub-benchmarks fan out across (batch_size × in_flight) so single-threaded
// numbers and concurrent ones (matching `max_in_flight: N` in production) can
// be compared in one run.
func BenchmarkWriteBatch(b *testing.B) {
	integration.CheckSkip(b)

	target := resolveBenchTarget(b)
	defer target.cleanup()

	batchSizes := []int{10, 100, 1000}
	inFlights := []int{1, 4, 16}

	for _, batchSize := range batchSizes {
		// Single output per batch size; in-flight variants share it (the
		// stream cache + resolver are concurrency-safe and that's what
		// production exercises).
		tableID := fmt.Sprintf("%s_%d", target.tablePrefix, batchSize)
		ensureBenchTable(b, target.bqClient, target.datasetID, tableID)

		out := newBenchmarkOutput(b, target, tableID)
		require.NoError(b, out.Connect(b.Context()))
		b.Cleanup(func() {
			if err := out.Close(context.Background()); err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
				b.Logf("output close: %v", err)
			}
		})

		batch := buildBatch(batchSize)
		batchBytes := batchPayloadBytes(b, batch)

		for _, inFlight := range inFlights {
			b.Run(fmt.Sprintf("batch=%d/in_flight=%d", batchSize, inFlight), func(b *testing.B) {
				runWriteBatchBench(b, target, out, batch, batchBytes, inFlight)
			})
		}
	}
}

// runWriteBatchBench drives `b.N` total batches through `out`, dispatched
// across `inFlight` concurrent worker goroutines. inFlight==1 is the original
// serial benchmark.
func runWriteBatchBench(b *testing.B, target *benchTarget, out *bigQueryWriteAPIOutput, batch service.MessageBatch, batchBytes, inFlight int) {
	b.Helper()

	var (
		latencyMu sync.Mutex
		latencies = make([]time.Duration, 0, b.N)
	)

	b.SetBytes(int64(batchBytes))
	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()

	if inFlight <= 1 {
		for i := 0; i < b.N; i++ {
			batchStart := time.Now()
			if err := out.WriteBatch(context.Background(), batch); err != nil {
				b.Fatalf("WriteBatch: %v", err)
			}
			latencies = append(latencies, time.Since(batchStart))
		}
	} else {
		// Round-robin b.N total batches across inFlight workers via an atomic
		// counter; mirrors how benthos pipelines fan out under max_in_flight.
		var idx atomic.Int64
		var wg sync.WaitGroup
		wg.Add(inFlight)
		for range inFlight {
			go func() {
				defer wg.Done()
				localLat := make([]time.Duration, 0, b.N/inFlight+1)
				for int(idx.Add(1))-1 < b.N {
					batchStart := time.Now()
					if err := out.WriteBatch(context.Background(), batch); err != nil {
						b.Errorf("WriteBatch: %v", err)
						return
					}
					localLat = append(localLat, time.Since(batchStart))
				}
				latencyMu.Lock()
				latencies = append(latencies, localLat...)
				latencyMu.Unlock()
			}()
		}
		wg.Wait()
	}

	elapsed := time.Since(start)

	b.StopTimer()

	rows := float64(b.N) * float64(len(batch))
	b.ReportMetric(rows/elapsed.Seconds(), "rows/s")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N)/1e6, "ms/batch")

	if target.realBQ {
		p50, p95, p99 := percentiles(latencies)
		b.ReportMetric(float64(p50.Nanoseconds())/1e6, "p50-ms")
		b.ReportMetric(float64(p95.Nanoseconds())/1e6, "p95-ms")
		b.ReportMetric(float64(p99.Nanoseconds())/1e6, "p99-ms")
	}
}

// benchTarget bundles everything a sub-benchmark needs to talk to either the
// emulator or real BigQuery without re-checking the env.
type benchTarget struct {
	realBQ          bool
	projectID       string
	datasetID       string
	tablePrefix     string
	httpEndpoint    string // emulator only
	grpcEndpoint    string // emulator only
	credentialsJSON string // real-BQ only
	bqClient        *bigquery.Client
	cleanup         func()
}

func resolveBenchTarget(b *testing.B) *benchTarget {
	b.Helper()

	if proj := os.Getenv("BQ_BENCH_PROJECT"); proj != "" {
		dataset := os.Getenv("BQ_BENCH_DATASET")
		if dataset == "" {
			b.Fatal("BQ_BENCH_PROJECT is set but BQ_BENCH_DATASET is empty")
		}
		creds := os.Getenv("BQ_BENCH_CREDENTIALS_JSON")
		prefix := os.Getenv("BQ_BENCH_TABLE_PREFIX")
		if prefix == "" {
			prefix = "bqwa_bench"
		}

		var clientOpts []option.ClientOption
		if creds != "" {
			clientOpts = append(clientOpts, option.WithCredentialsJSON([]byte(creds)))
		}
		client, err := bigquery.NewClient(b.Context(), proj, clientOpts...)
		require.NoError(b, err)

		t := &benchTarget{
			realBQ:          true,
			projectID:       proj,
			datasetID:       dataset,
			tablePrefix:     prefix,
			credentialsJSON: creds,
			bqClient:        client,
			cleanup:         func() { _ = client.Close() },
		}
		b.Logf("real-BQ mode: project=%s dataset=%s table_prefix=%s", proj, dataset, prefix)
		return t
	}

	const (
		emuProject = "test-project"
		emuDataset = "bench_dataset"
	)
	emu := startEmulator(b, emuProject, emuDataset)
	return &benchTarget{
		realBQ:       false,
		projectID:    emuProject,
		datasetID:    emuDataset,
		tablePrefix:  "bench_table",
		httpEndpoint: emu.httpEndpoint,
		grpcEndpoint: emu.grpcEndpoint,
		bqClient:     emu.bqClient,
		cleanup:      func() {}, // emulator container + bqClient are torn down by t.Cleanup hooks in startEmulator
	}
}

// ensureBenchTable creates the benchmark table if it doesn't already exist.
// The schema matches what buildBatch emits.
func ensureBenchTable(b *testing.B, client *bigquery.Client, datasetID, tableID string) {
	b.Helper()
	err := client.Dataset(datasetID).Table(tableID).Create(b.Context(), &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
			{Name: "name", Type: bigquery.StringFieldType, Required: true},
			{Name: "payload", Type: bigquery.StringFieldType, Required: true},
		},
	})
	var apiErr *googleapi.Error
	if err != nil && (!errors.As(err, &apiErr) || apiErr.Code != http.StatusConflict) {
		require.NoError(b, err)
	}
}

// newBenchmarkOutput builds the output config (emulator endpoints in emulator
// mode, real auth in real-BQ mode) and returns an unconnected output.
func newBenchmarkOutput(b *testing.B, target *benchTarget, tableID string) *bigQueryWriteAPIOutput {
	b.Helper()

	var yaml string
	if target.realBQ {
		yaml = fmt.Sprintf(`
project: %s
dataset: %s
table: %s
`, target.projectID, target.datasetID, tableID)
		if target.credentialsJSON != "" {
			yaml += fmt.Sprintf("credentials_json: %q\n", target.credentialsJSON)
		}
	} else {
		yaml = fmt.Sprintf(`
project: %s
dataset: %s
table: %s
endpoint:
  http: %s
  grpc: %s
`, target.projectID, target.datasetID, tableID, target.httpEndpoint, target.grpcEndpoint)
	}

	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(yaml, nil)
	require.NoError(b, err)

	mgr := service.MockResources()
	license.InjectTestService(mgr)
	out, err := bigQueryWriteAPIOutputFromConfig(pConf, mgr)
	require.NoError(b, err)
	return out
}

// batchPayloadBytes sums the raw payload size of every message in batch. Used
// with b.SetBytes so Go's testing harness can report a MB/s metric.
func batchPayloadBytes(b *testing.B, batch service.MessageBatch) int {
	b.Helper()
	total := 0
	for _, m := range batch {
		buf, err := m.AsBytes()
		require.NoError(b, err)
		total += len(buf)
	}
	return total
}

// buildBatch returns a fixed-content MessageBatch of the requested size. The
// same MessageBatch is reused across benchmark iterations, so per-iteration
// cost is dominated by the BQ-write path rather than message construction.
//
// Note: service.Message has internal state, but reusing it inside a single
// benchmark process is safe because WriteBatch only reads bytes via AsBytes.
var buildBatchOnce sync.Map // batch size -> service.MessageBatch (memoised)

func buildBatch(size int) service.MessageBatch {
	if cached, ok := buildBatchOnce.Load(size); ok {
		return cached.(service.MessageBatch)
	}
	batch := make(service.MessageBatch, 0, size)
	for i := range size {
		msg := service.NewMessage(fmt.Appendf(nil,
			`{"id":"%d","name":"row-%d","payload":"%s"}`,
			i, i, "abcdefghijklmnopqrstuvwxyz0123456789",
		))
		batch = append(batch, msg)
	}
	buildBatchOnce.Store(size, batch)
	return batch
}

// percentiles returns p50/p95/p99 from a latency sample. The slice is sorted
// in place.
func percentiles(samples []time.Duration) (p50, p95, p99 time.Duration) {
	if len(samples) == 0 {
		return 0, 0, 0
	}
	slices.Sort(samples)
	pick := func(q float64) time.Duration {
		idx := int(float64(len(samples)-1) * q)
		return samples[idx]
	}
	return pick(0.50), pick(0.95), pick(0.99)
}
