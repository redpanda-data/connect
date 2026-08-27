// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Sink write-path throughput against the containerised MinIO + Iceberg REST
// catalog, driving the Router directly.
//
// Why not the pipeline benchmark under bench/: that runs the assembled
// `iceberg` output, which is an enterprise component and so needs a valid
// licence to initialise. This measures the same write path one layer down —
// JSON decode, shredding, parquet encode, upload, catalog commit — by driving
// the Router the way the integration tests already do, which needs no licence.
//
// What it therefore does NOT include, and what the numbers should not be read
// as covering: benthos input, pipeline and batching overhead, and object
// storage that behaves like a real cloud endpoint rather than a local
// container. It is a comparative instrument — before/after a change, or codec
// against codec — not an absolute throughput figure for a deployment.
var (
	throughputRun = flag.Bool("iceberg.throughput", false,
		"run the write-path throughput measurement (needs Docker; takes minutes)")
	throughputRecords = flag.Int("iceberg.throughput.records", 200000,
		"records to write per run")
	throughputBatch = flag.Int("iceberg.throughput.batch", 5000,
		"records per Route call")
	throughputCodec = flag.String("iceberg.throughput.codec", "",
		"value for the table's write.parquet.compression-codec property; empty leaves it unset")
	throughputLabel = flag.String("iceberg.throughput.label", "run",
		"label to print alongside the result, for telling A/B runs apart")
	throughputColumns = flag.Int("iceberg.throughput.columns", 0,
		"extra string columns beyond the base five; the shredder's per-record cost scales with schema width, so this is the axis its optimisation targets")
	throughputPayload = flag.String("iceberg.throughput.payload", "regular",
		"record shape: 'regular' (~90B, sequential ids and a shared string prefix) or 'high-entropy' (~1.2kB of random text)")
)

// TestWriteThroughput writes a fixed number of records and reports the rate and
// the bytes they occupy, so a change to the write path can be measured
// before/after and so the cost of a compression codec can be quantified.
func TestWriteThroughput(t *testing.T) {
	if !*throughputRun {
		t.Skip("set -iceberg.throughput to run the write-path throughput measurement")
	}

	ctx := t.Context()
	infra := setupTestInfra(t, ctx)

	const namespace = "bench"
	infra.CreateNamespace(t, namespace)

	tableName := fmt.Sprintf("tput_%d", time.Now().UnixNano())

	// Pre-create the table so the measured window contains no CREATE TABLE, and
	// so the compression codec can be set through the property the resolver
	// actually reads. Schema mirrors the localhost bench config's record shape.
	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		{ID: 2, Name: "user_id", Type: iceberg.PrimitiveTypes.Int64},
		{ID: 3, Name: "event_type", Type: iceberg.PrimitiveTypes.String},
		{ID: 4, Name: "value", Type: iceberg.PrimitiveTypes.Int64},
		{ID: 5, Name: "info", Type: iceberg.PrimitiveTypes.String},
	}
	for i := range *throughputColumns {
		fields = append(fields, iceberg.NestedField{
			ID: 100 + i, Name: fmt.Sprintf("col_%d", i), Type: iceberg.PrimitiveTypes.String,
		})
	}
	sc := iceberg.NewSchema(0, fields...)

	client := infra.NewCatalogClient(t, namespace)
	var opts []catalog.CreateTableOpt
	if *throughputCodec != "" {
		opts = append(opts, catalog.WithProperties(iceberg.Properties{
			"write.parquet.compression-codec": *throughputCodec,
		}))
	}
	_, err := client.CreateTable(ctx, tableName, sc, opts...)
	require.NoError(t, err)

	// Log what the table actually carries: whether a property was set here or
	// materialised by the catalog decides which codec the resolver picks.
	if created, err := client.LoadTable(ctx, tableName); err == nil {
		t.Logf("TABLEPROPS %v", created.Properties())
	}

	router := infra.NewRouter(t, namespace, tableName)

	// Build every batch up front so record generation is not inside the timed
	// window — the point of measurement is the sink, not the generator.
	batches := buildThroughputBatches(t, *throughputRecords, *throughputBatch, *throughputPayload, *throughputColumns)

	start := time.Now()
	for _, batch := range batches {
		require.NoError(t, router.Route(ctx, batch))
	}
	elapsed := time.Since(start)

	// Bytes come from the snapshot summary rather than being counted locally, so
	// the figure is what the table actually holds.
	tbl, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)
	var committedRecords int64
	summary := map[string]string{}
	if snap := tbl.CurrentSnapshot(); snap != nil && snap.Summary != nil {
		summary = snap.Summary.Properties
		committedRecords, _ = strconv.ParseInt(summary["total-records"], 10, 64)
	}

	// Size is summed from the manifests rather than taken from the snapshot
	// summary's total-files-size, which this catalog does not populate with the
	// data-file total — it reported 82kB for a table whose string column alone
	// reads back as 528kB, so it cannot be used for a bytes-per-record figure.
	totalBytes, dataFiles, writtenCodec := sumDataFileBytes(t, ctx, tbl)

	// Assert the codec that was actually written, rather than assuming the
	// property took effect. Without this a run that silently ignored the
	// property would produce a plausible-looking size comparison.
	wantCodec := "UNCOMPRESSED"
	if *throughputCodec != "" {
		wantCodec = strings.ToUpper(*throughputCodec)
	}
	require.Equal(t, wantCodec, writtenCodec,
		"data files were written with %s, not the requested %s", writtenCodec, wantCodec)

	// Refuse to report a rate for a run that did not actually land the data. A
	// throughput number from a partial or failed write looks entirely plausible
	// and is worthless, so this is asserted rather than trusted.
	require.EqualValues(t, *throughputRecords, committedRecords,
		"table holds %d records, expected %d — the measured rate would be meaningless (summary: %v)",
		committedRecords, *throughputRecords, summary)
	require.Positive(t, totalBytes, "table reports no file bytes (summary: %v)", summary)

	// Read the data back and confirm the columns carry real content. Row counts
	// alone would not catch a column silently arriving empty, which would make
	// any bytes-per-record figure nonsense.
	type contentRow struct {
		N        int64 `json:"n"`
		InfoLen  int64 `json:"info_len"`
		Distinct int64 `json:"distinct_info"`
	}
	content := querySQL[contentRow](t, ctx, infra, fmt.Sprintf(
		`SELECT count(*) AS n, sum(length(info)) AS info_len, count(DISTINCT info) AS distinct_info FROM iceberg_cat."%s"."%s";`,
		namespace, tableName))
	require.Len(t, content, 1)
	require.EqualValues(t, *throughputRecords, content[0].N, "read-back row count")
	require.Positive(t, content[0].InfoLen, "the info column read back empty")
	t.Logf("READBACK rows=%d info_bytes=%d distinct_info=%d",
		content[0].N, content[0].InfoLen, content[0].Distinct)

	records := int64(*throughputRecords)
	rate := float64(records) / elapsed.Seconds()
	perRecord := 0.0
	if records > 0 {
		perRecord = float64(totalBytes) / float64(records)
	}

	t.Logf("RESULT label=%s payload=%s cols=%d codec=%q written=%s records=%d batch=%d files=%d elapsed=%s rec/s=%.0f bytes=%d bytes/rec=%.1f",
		*throughputLabel, *throughputPayload, 5+*throughputColumns, *throughputCodec, writtenCodec, records, *throughputBatch, dataFiles,
		elapsed.Round(time.Millisecond), rate, totalBytes, perRecord)
}

// sumDataFileBytes totals the on-disk size of every data file the table's
// current snapshot references, and returns that with the file count.
func sumDataFileBytes(t *testing.T, ctx context.Context, tbl *table.Table) (bytes, files int64, codec string) {
	t.Helper()
	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap, "table has no snapshot")

	fs, err := tbl.FS(ctx)
	require.NoError(t, err)

	manifests, err := snap.Manifests(fs)
	require.NoError(t, err)

	for _, m := range manifests {
		for e, err := range m.Entries(fs, true) {
			require.NoError(t, err)
			df := e.DataFile()
			if df.ContentType() != iceberg.EntryContentData {
				continue
			}
			bytes += df.FileSizeBytes()
			files++

			// Every data file should carry the same codec; read one and check
			// the rest agree, so a partially-applied setting cannot hide.
			fileCodec := parquetCodecOf(t, fs, df.FilePath())
			if codec == "" {
				codec = fileCodec
			}
			require.Equal(t, codec, fileCodec,
				"data files disagree on codec: %s vs %s", codec, fileCodec)
		}
	}
	return bytes, files, codec
}

// parquetCodecOf reports the compression codec recorded in a parquet file's
// footer, read from object storage.
func parquetCodecOf(t *testing.T, fs iceio.IO, path string) string {
	t.Helper()
	f, err := fs.Open(path)
	require.NoError(t, err)
	defer f.Close()

	data, err := io.ReadAll(f)
	require.NoError(t, err)

	pf, err := parquet.OpenFile(bytesReaderAt(data), int64(len(data)))
	require.NoError(t, err)

	for _, rg := range pf.Metadata().RowGroups {
		for _, col := range rg.Columns {
			return col.MetaData.Codec.String()
		}
	}
	t.Fatalf("parquet file %s has no column chunks", path)
	return ""
}

// bytesReaderAt adapts a byte slice to the io.ReaderAt parquet needs.
func bytesReaderAt(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// buildThroughputBatches produces batches of JSON records in one of two shapes.
//
// The shape matters a great deal for anything size-related, which is why it is
// selectable rather than fixed. "regular" mirrors the localhost bench config's
// generator: sequential ids and an `info` string sharing a 21-character prefix.
// Parquet's byte-array encoding compresses that prefix away before any codec
// runs, so the same 20k records occupy ~4 bytes each with no compression at all
// — measuring a codec against it would show almost no win and imply, wrongly,
// that compression does not pay. "high-entropy" fills `info` with random text
// instead, which is the regime where a codec earns its cost.
func buildThroughputBatches(t *testing.T, total, perBatch int, payload string, extraColumns int) []service.MessageBatch {
	t.Helper()
	require.Contains(t, []string{"regular", "high-entropy"}, payload,
		"unknown payload shape %q", payload)

	eventTypes := []string{"click", "view", "purchase", "scroll", "hover"}
	const alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// Deterministic, but freshly generated per record rather than sliced from a
	// shared pool. Slicing a 64kB pool 100k times produces values that overlap
	// heavily, and parquet's encoders exploit that redundancy across records —
	// the result looked like 4:1 compression on supposedly random data, which
	// would have made any codec comparison meaningless.
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // benchmark entropy, not crypto
	const highEntropyLen = 1100

	var batches []service.MessageBatch
	for start := 0; start < total; start += perBatch {
		n := min(perBatch, total-start)
		batch := make(service.MessageBatch, n)
		for i := range n {
			id := start + i
			info := fmt.Sprintf("event info for record %d", id)
			if payload == "high-entropy" {
				buf := make([]byte, highEntropyLen)
				for j := range buf {
					buf[j] = alnum[rng.Intn(len(alnum))]
				}
				info = string(buf)
			}
			var extra strings.Builder
			for c := range extraColumns {
				fmt.Fprintf(&extra, `,"col_%d":"v%d_%d"`, c, c, id%97)
			}
			batch[i] = service.NewMessage(fmt.Appendf(nil,
				`{"id":%d,"user_id":%d,"event_type":%q,"value":%d,"info":%q%s}`,
				id, id%10000+1, eventTypes[id%5], id%1000, info, extra.String()))
		}
		batches = append(batches, batch)
	}
	return batches
}
