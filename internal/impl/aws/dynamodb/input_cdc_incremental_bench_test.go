// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// incSnapBenchCase is one backfill run: a table of items rows, read with the
// given snapshot mode and Scan shape, while writeRate UpdateItem calls per
// second (0 for none) bump random keys of the same table.
type incSnapBenchCase struct {
	name      string
	mode      string
	items     int
	segments  int
	batchSize int
	writeRate int
}

// incSnapBenchResult is what one run measured. Values only increase (every
// write is ADD v 1), so a READ below a value already emitted for its key is a
// stale snapshot item that overtook a newer change.
type incSnapBenchResult struct {
	backfill         time.Duration
	events           map[string]int
	readKeys         int
	writes           int64
	safetyViolations int
	orderRegressions int
	finalMismatches  int
	windowDropped    int64
	streamLatency    []time.Duration
}

// BenchmarkIntegrationDynamoDBCDCIncrementalSnapshot measures a backfill with
// and without concurrent writes to the rows being read, for
// snapshot_mode: incremental and the snapshot_and_cdc baseline, against
// DynamoDB Local. Run each case once:
//
//	go test -run BenchmarkIntegrationDynamoDBCDCIncrementalSnapshot -bench BenchmarkIntegrationDynamoDBCDCIncrementalSnapshot -benchtime 1x -timeout 60m ./internal/impl/aws/dynamodb/
//
// DynamoDB Local rounds ApproximateCreationDateTime down to the minute. While
// writes keep arriving, incremental mode releases a page only once a stream
// record from the next minute is seen, so on DynamoDB Local each page round
// under writes takes up to 60s. On AWS the stream timestamps are accurate and
// a page waits about 1s + watermark_margin for the stream.
//
// The same rounding makes the snapshot_and_cdc dedupe skip every change made
// in the minute the snapshot started (it skips stream events stamped before
// the snapshot start), so on DynamoDB Local only that baseline's throughput is
// comparable, not its final values.
func BenchmarkIntegrationDynamoDBCDCIncrementalSnapshot(b *testing.B) {
	integration.CheckSkip(b)

	port, cleanup := setupBenchContainer(b)
	b.Cleanup(cleanup)

	cases := []incSnapBenchCase{
		{name: "incremental/defaults/1k/no_writes", mode: "incremental", items: 1000, segments: 1, batchSize: 100},
		{name: "incremental/tuned/10k/no_writes", mode: "incremental", items: 10000, segments: 10, batchSize: 1000},
		{name: "incremental/tuned/10k/writes_50ps", mode: "incremental", items: 10000, segments: 10, batchSize: 1000, writeRate: 50},
		{name: "incremental/tuned/10k/writes_200ps", mode: "incremental", items: 10000, segments: 10, batchSize: 1000, writeRate: 200},
		{name: "snapshot_and_cdc/tuned/10k/no_writes", mode: "snapshot_and_cdc", items: 10000, segments: 10, batchSize: 1000},
		{name: "snapshot_and_cdc/tuned/10k/writes_200ps", mode: "snapshot_and_cdc", items: 10000, segments: 10, batchSize: 1000, writeRate: 200},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			for b.Loop() {
				r := runIncSnapBenchCase(b, port, c)
				reportIncSnapBench(b, c, r)
			}
		})
	}
}

func reportIncSnapBench(b *testing.B, c incSnapBenchCase, r incSnapBenchResult) {
	b.Helper()
	secs := r.backfill.Seconds()
	b.ReportMetric(secs, "backfill_s")
	b.ReportMetric(float64(r.events["READ"])/secs, "read_items/s")
	b.ReportMetric(float64(r.writes)/secs, "writes/s")
	b.ReportMetric(float64(r.events["READ"]), "read_events")
	b.ReportMetric(float64(r.events["MODIFY"]), "modify_events")
	b.ReportMetric(float64(c.items-r.readKeys), "keys_without_read")
	b.ReportMetric(float64(r.windowDropped), "window_dropped")
	b.ReportMetric(float64(r.safetyViolations), "safety_violations")
	b.ReportMetric(float64(r.orderRegressions), "order_regressions")
	b.ReportMetric(float64(r.finalMismatches), "final_mismatches")
	if n := len(r.streamLatency); n > 0 {
		slices.Sort(r.streamLatency)
		b.ReportMetric(float64(r.streamLatency[n/2].Milliseconds()), "stream_p50_ms")
		b.ReportMetric(float64(r.streamLatency[n*95/100].Milliseconds()), "stream_p95_ms")
		b.ReportMetric(float64(r.streamLatency[n-1].Milliseconds()), "stream_max_ms")
	}
	b.Logf("%s: backfill %.1fs, writes %d, events %v, keys without READ %d, window dropped %d, safety violations %d, order regressions %d, final mismatches %d",
		c.name, secs, r.writes, r.events, c.items-r.readKeys, r.windowDropped, r.safetyViolations, r.orderRegressions, r.finalMismatches)
}

// newBenchResources returns resources whose metrics are recorded by a
// counting exporter, so the run can watch dynamodb_cdc_snapshot_state.
func newBenchResources(tb testing.TB) (*service.Resources, *countingMetricsExporter) {
	tb.Helper()
	exporter := &countingMetricsExporter{counters: map[string]int64{}, gauges: map[string]int64{}}
	env := service.NewEmptyEnvironment()
	require.NoError(tb, env.RegisterMetricsExporter("counting", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Logger) (service.MetricsExporter, error) { return exporter, nil }))
	require.NoError(tb, env.RegisterOtelTracerProvider("none", service.NewConfigSpec(),
		func(*service.ParsedConfig) (trace.TracerProvider, error) { return noop.NewTracerProvider(), nil }))
	builder := env.NewResourceBuilder()
	require.NoError(tb, builder.SetMetricsYAML("counting: {}"))
	res, closeFn, err := builder.Build()
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = closeFn(context.Background()) })
	return res, exporter
}

func runIncSnapBenchCase(b *testing.B, port string, c incSnapBenchCase) incSnapBenchResult {
	b.Helper()
	ctx := b.Context()

	endpoint := "http://localhost:" + port
	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(b, err)
	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	id := benchCounter.Add(1)
	tableName := fmt.Sprintf("bench-incsnap-%d", id)
	checkpointTable := fmt.Sprintf("bench-incsnap-ckpt-%d", id)
	b.Cleanup(func() {
		for _, name := range []string{tableName, checkpointTable} {
			_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(name)})
		}
	})

	createTable := func() error {
		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
			BillingMode: types.BillingModePayPerRequest,
		})
		return err
	}
	// DynamoDB Local can accept connections before it serves requests.
	require.Eventually(b, func() bool { return createTable() == nil }, time.Minute, 500*time.Millisecond)

	padding := strings.Repeat("x", 100)
	for i := 0; i < c.items; i += 25 {
		reqs := make([]types.WriteRequest, 0, 25)
		for j := i; j < min(i+25, c.items); j++ {
			reqs = append(reqs, types.WriteRequest{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: fmt.Sprintf("k%d", j)},
				"v":       &types.AttributeValueMemberN{Value: "0"},
				"payload": &types.AttributeValueMemberS{Value: padding},
			}}})
		}
		_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{tableName: reqs},
		})
		require.NoError(b, err)
	}
	// The stream is enabled after seeding, so trim_horizon starts at the
	// first write made during the backfill and the seed rows only arrive as
	// snapshot items.
	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String(tableName),
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(b, err)

	confStr := fmt.Sprintf(`
tables: [%s]
checkpoint_table: %s
endpoint: %s
region: us-east-1
start_from: trim_horizon
snapshot_mode: %s
snapshot_segments: %d
snapshot_batch_size: %d
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tableName, checkpointTable, endpoint, c.mode, c.segments, c.batchSize)
	parsed, err := dynamoDBCDCInputConfig().ParseYAML(confStr, nil)
	require.NoError(b, err)
	res, metrics := newBenchResources(b)
	input, err := newDynamoDBCDCInputFromConfig(parsed, res)
	require.NoError(b, err)

	// writtenAt records when each (key, value) was written, so the reader
	// can measure how long a MODIFY took to come out of the input while the
	// backfill was running.
	var (
		writtenMu sync.Mutex
		writtenAt = map[string]time.Time{}
		writes    atomic.Int64
	)
	start := time.Now()
	require.NoError(b, input.Connect(ctx))
	defer func() { _ = input.Close(context.Background()) }()

	writerCtx, stopWriters := context.WithCancel(ctx)
	defer stopWriters()
	var writersDone sync.WaitGroup
	if c.writeRate > 0 {
		const writers = 4
		interval := time.Second * writers / time.Duration(c.writeRate)
		for range writers {
			writersDone.Go(func() {
				tick := time.NewTicker(interval)
				defer tick.Stop()
				for {
					select {
					case <-writerCtx.Done():
						return
					case <-tick.C:
					}
					pk := fmt.Sprintf("k%d", rand.IntN(c.items))
					out, err := client.UpdateItem(writerCtx, &dynamodb.UpdateItemInput{
						TableName:                 aws.String(tableName),
						Key:                       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: pk}},
						UpdateExpression:          aws.String("ADD v :one"),
						ExpressionAttributeValues: map[string]types.AttributeValue{":one": &types.AttributeValueMemberN{Value: "1"}},
						ReturnValues:              types.ReturnValueUpdatedNew,
					})
					if err != nil {
						if writerCtx.Err() == nil {
							b.Errorf("update failed: %v", err)
						}
						return
					}
					v := out.Attributes["v"].(*types.AttributeValueMemberN).Value
					writtenMu.Lock()
					writtenAt[pk+"/"+v] = time.Now()
					writtenMu.Unlock()
					writes.Add(1)
				}
			})
		}
	}

	r := incSnapBenchResult{events: map[string]int{}}
	last := map[string]int{}
	maxSeen := map[string]int{}
	readKeys := map[string]struct{}{}
	backfillDone := false
	var lastMsgAt time.Time
	const quiet = 10 * time.Second
	deadline := time.Now().Add(30 * time.Minute)
	for {
		require.True(b, time.Now().Before(deadline), "%s: backfill did not finish within 30m", c.name)
		if !backfillDone && metrics.gauge(metricSnapshotState) == int64(snapshotStateComplete) {
			r.backfill = time.Since(start)
			r.writes = writes.Load()
			backfillDone = true
			stopWriters()
			writersDone.Wait()
			lastMsgAt = time.Now()
		}
		if backfillDone && time.Since(lastMsgAt) >= quiet {
			break
		}

		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		batch, ackFn, err := input.ReadBatch(readCtx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		require.NoError(b, err)
		now := time.Now()
		for _, msg := range batch {
			eventName, _ := msg.MetaGet("dynamodb_event_name")
			r.events[eventName]++
			structured, err := msg.AsStructured()
			require.NoError(b, err)
			img := structured.(map[string]any)["dynamodb"].(map[string]any)["newImage"].(map[string]any)
			pk := img["pk"].(string)
			v, err := strconv.Atoi(fmt.Sprint(img["v"]))
			require.NoError(b, err)

			if eventName == "READ" {
				readKeys[pk] = struct{}{}
				if top, seen := maxSeen[pk]; seen && v < top {
					r.safetyViolations++
				}
			} else if !backfillDone {
				writtenMu.Lock()
				at, ok := writtenAt[pk+"/"+strconv.Itoa(v)]
				writtenMu.Unlock()
				if ok {
					r.streamLatency = append(r.streamLatency, now.Sub(at))
				}
			}
			if prev, seen := last[pk]; seen && v < prev {
				r.orderRegressions++
			}
			if top, seen := maxSeen[pk]; !seen || v > top {
				maxSeen[pk] = v
			}
			last[pk] = v
		}
		if len(batch) > 0 {
			lastMsgAt = now
		}
		if ackFn != nil {
			require.NoError(b, ackFn(ctx, nil))
		}
	}
	r.readKeys = len(readKeys)
	r.windowDropped = metrics.counter(metricSnapshotWindowDropped)

	table := map[string]int{}
	var startKey map[string]types.AttributeValue
	for {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(tableName),
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: startKey,
		})
		require.NoError(b, err)
		for _, item := range out.Items {
			v, err := strconv.Atoi(item["v"].(*types.AttributeValueMemberN).Value)
			require.NoError(b, err)
			table[item["pk"].(*types.AttributeValueMemberS).Value] = v
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	require.Len(b, table, c.items)
	for pk, want := range table {
		if got, ok := last[pk]; !ok || got != want {
			r.finalMismatches++
		}
	}
	return r
}
