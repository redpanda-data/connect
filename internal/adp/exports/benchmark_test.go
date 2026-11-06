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

package exports_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/adp/exports"
)

// Throughput benchmarks for the config. Both report events/s so the numbers
// can be read against a target event rate directly; ns/op is per event.
//
//   - BenchmarkPipelineProcessors needs no Docker. It runs the config's
//     processors in-process (schema_registry_decode against an in-memory
//     registry, the OCSF mapping, the error switch, then the output's batch
//     processors: archive, hash, gzip) and so measures the CPU cost of the
//     config with brokers and S3 taken out of the picture.
//
//   - BenchmarkIntegrationAuditLogExport runs the whole config against a
//     real Redpanda and localstack S3, pre-loading the topic with b.N events
//     and timing the pipeline from start until every event is in S3, once per
//     batch count in benchBatchCounts (memory is set by the batch, so this is
//     the sizing trendline). It is opt-in like the integration test and wants
//     an explicit event count:
//
//     go test ./internal/adp/exports -bench '^BenchmarkIntegration' -benchtime 300000x
//
// Both report peak heap (live, and in use including uncollected garbage) for
// the whole test binary, as upper bounds on what the pipeline holds.
//
// benchBatchSize is the config's default batching.count; the end-to-end
// benchmark's event count should be a multiple of every batch count so no
// partial batch waits out the batch period at the end.
const benchBatchSize = 10000

var benchBatchCounts = []int{benchBatchSize, 2000}

func BenchmarkPipelineProcessors(b *testing.B) {
	md, schemaText := compileAuditEvent(b)
	const schemaID = 1
	srURL := runMockSchemaRegistry(b, schemaID, schemaText)
	encode := auditEventEncoder(b, md, schemaID)
	fixtures := loadFixtures(b, "testdata/events.jsonl")
	encoded := make([][]byte, len(fixtures))
	for i, fx := range fixtures {
		encoded[i] = encode(fx.Event)
	}
	processors := configProcessors(b)
	inputs := map[string]string{
		"REDPANDA_SCHEMA_REGISTRY_URL":            srURL,
		"secrets.AUDIT_LOG_EXPORT_KAFKA_USERNAME": kafkaUser,
		"secrets.AUDIT_LOG_EXPORT_KAFKA_PASSWORD": kafkaPass,
	}

	for _, tc := range []struct {
		name      string
		producers int
	}{
		// serial: one batch in flight at a time, so the per-event cost on
		// one core. parallel: as many batches in flight as there are cores,
		// so how far the pipeline's processor threads scale.
		{"serial", 1},
		{"parallel", runtime.GOMAXPROCS(0)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var outBytes, outRecords atomic.Int64
			sb := service.NewStreamBuilder()
			sb.SetLogger(slog.New(slog.DiscardHandler))
			sb.SetEnvVarLookupFunc(func(key string) (string, bool) {
				v, ok := inputs[key]
				return v, ok
			})
			push, err := sb.AddBatchProducerFunc()
			require.NoError(b, err)
			for _, p := range processors {
				require.NoError(b, sb.AddProcessorYAML(p))
			}
			require.NoError(b, sb.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
				for _, m := range batch {
					if err := m.GetError(); err != nil {
						return fmt.Errorf("message failed in the pipeline: %w", err)
					}
					bts, err := m.AsBytes()
					if err != nil {
						return err
					}
					outBytes.Add(int64(len(bts)))
					// archive keeps the first message's metadata, and the
					// config stamps batch_count on every message before it.
					n, _ := m.MetaGet("batch_count")
					count, err := strconv.Atoi(n)
					if err != nil {
						return fmt.Errorf("batch_count metadata %q: %w", n, err)
					}
					outRecords.Add(int64(count))
				}
				return nil
			}))
			stream, err := sb.Build()
			require.NoError(b, err)
			ctx, cancel := context.WithCancel(b.Context())
			defer cancel()
			runErr := make(chan error, 1)
			go func() { runErr <- stream.Run(ctx) }()

			// Split b.N events into batches of the config's size, dealt to
			// the producers round-robin.
			// Every batch cycles through the fixtures from the start, so the
			// bytes sent are the same per batch; count them the same way.
			batches := make(chan int, tc.producers)
			var inBytes int64
			for sent := 0; sent < b.N; sent += benchBatchSize {
				n := min(benchBatchSize, b.N-sent)
				for i := range n {
					inBytes += int64(len(encoded[i%len(encoded)]))
				}
			}
			var wg sync.WaitGroup
			errs := make(chan error, tc.producers)
			peakHeap := sampleHeap()
			b.ResetTimer()
			for range tc.producers {
				wg.Go(func() {
					for n := range batches {
						batch := make(service.MessageBatch, n)
						for i := range n {
							batch[i] = service.NewMessage(encoded[i%len(encoded)])
						}
						if err := push(ctx, batch); err != nil {
							errs <- err
							return
						}
					}
				})
			}
			for sent := 0; sent < b.N; sent += benchBatchSize {
				batches <- min(benchBatchSize, b.N-sent)
			}
			close(batches)
			wg.Wait()
			b.StopTimer()
			close(errs)
			for err := range errs {
				require.NoError(b, err)
			}
			elapsed := b.Elapsed()
			require.Equal(b, int64(b.N), outRecords.Load(), "records out != events in")
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "events/s")
			b.ReportMetric(float64(inBytes)/elapsed.Seconds()/1e6, "in_MB/s")
			b.ReportMetric(float64(outBytes.Load())/elapsed.Seconds()/1e6, "out_MB/s")
			live, inuse := peakHeap()
			b.ReportMetric(live, "peak_live_MB")
			b.ReportMetric(inuse, "peak_heap_MB")

			require.NoError(b, stream.StopWithin(30*time.Second))
			cancel()
			requireCleanRun(b, <-runErr)
		})
	}
}

func BenchmarkIntegrationAuditLogExport(b *testing.B) {
	integration.CheckSkip(b)
	env := startEnv(b)
	fixtures := loadFixtures(b, "testdata/events.jsonl")
	encode := newAuditEventEncoder(b, env.sr)
	records := make([]*kgo.Record, len(fixtures))
	for i, fx := range fixtures {
		records[i] = &kgo.Record{Topic: topic, Key: []byte(fx.Key), Value: encode(fx.Event)}
	}
	for _, count := range benchBatchCounts {
		b.Run(fmt.Sprintf("batch_count=%d", count), func(b *testing.B) {
			env.benchExport(b, records, count)
		})
	}
}

// benchExport pre-loads b.N events and times the config exporting them with
// the given batch count.
func (e *testEnv) benchExport(b *testing.B, records []*kgo.Record, batchCount int) {
	b.Helper()
	env := e

	// A fresh topic, so this run's consumer group reads exactly b.N events.
	// Round-robin partitioning stands in for the spread of resource ids a
	// real topic's keys would have (16 fixture keys would not fill 3
	// partitions evenly).
	admin := kadm.NewClient(env.kafka)
	_, err := admin.DeleteTopics(b.Context(), topic)
	require.NoError(b, err)
	require.Eventually(b, func() bool {
		resp, err := admin.CreateTopics(b.Context(), 3, 1, nil, topic)
		if err != nil {
			return false
		}
		r, ok := resp[topic]
		return ok && r.Err == nil
	}, 30*time.Second, 200*time.Millisecond, "recreating topic %s", topic)

	producer, err := kgo.NewClient(env.kafkaOpts(kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))...)
	require.NoError(b, err)
	defer producer.Close()
	var inBytes int64
	const chunk = 2000
	for sent := 0; sent < b.N; sent += chunk {
		n := min(chunk, b.N-sent)
		recs := make([]*kgo.Record, n)
		for i := range n {
			src := records[(sent+i)%len(records)]
			recs[i] = &kgo.Record{Topic: topic, Key: src.Key, Value: src.Value}
			inBytes += int64(len(src.Value))
		}
		require.NoError(b, producer.ProduceSync(b.Context(), recs...).FirstErr())
	}

	id := fmt.Sprintf("bench-%d-%d", b.N, time.Now().UnixNano())
	inputs := env.inputs(id, id)
	inputs["secrets.AUDIT_LOG_EXPORT_BATCH_PERIOD"] = "1s"
	inputs["secrets.AUDIT_LOG_EXPORT_BATCH_COUNT"] = strconv.Itoa(batchCount)
	stream, logs := buildStream(b, inputs)
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()
	runErr := make(chan error, 1)

	peakHeap := sampleHeap()
	b.ResetTimer()
	go func() { runErr <- stream.Run(ctx) }()
	deadline := time.Now().Add(10 * time.Minute)
	var exported int
	for exported < b.N {
		if time.Now().After(deadline) {
			b.Fatalf("only %d of %d events exported before the deadline; pipeline logs:\n%s", exported, b.N, logs.String())
		}
		time.Sleep(250 * time.Millisecond)
		exported = env.countRecordsInKeys(b, id)
	}
	b.StopTimer()
	elapsed := b.Elapsed()
	objects := env.listObjects(b, id)
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "events/s")
	b.ReportMetric(float64(inBytes)/elapsed.Seconds()/1e6, "in_MB/s")
	b.ReportMetric(float64(len(objects)), "objects")
	live, inuse := peakHeap()
	b.ReportMetric(live, "peak_live_MB")
	b.ReportMetric(inuse, "peak_heap_MB")
	b.Logf("%d events (%d bytes of protobuf) exported as %d objects in %s", b.N, inBytes, len(objects), elapsed)

	require.NoError(b, stream.StopWithin(30*time.Second), "a healthy pipeline must stop cleanly")
	cancel()
	requireCleanRun(b, <-runErr)
}

// requireCleanRun checks what stream.Run returned after a graceful stop
// followed by a cancelled context.
func requireCleanRun(t testing.TB, err error) {
	t.Helper()
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline run returned: %v", err)
	}
}

// sampleHeap samples the Go heap until stop is called, which returns the
// peaks in MB: live is the heap live after the most recent GC (what the
// pipeline actually holds), inuse is heap in use including not yet collected
// garbage (closer to what the process needs). Both are for the whole test
// binary, so read them as upper bounds.
func sampleHeap() (stop func() (live, inuse float64)) {
	var (
		peakLive, peakInuse uint64
		samples             = []metrics.Sample{{Name: "/gc/heap/live:bytes"}, {Name: "/memory/classes/heap/objects:bytes"}}
		done                = make(chan struct{})
		fin                 = make(chan struct{})
	)
	sample := func() {
		metrics.Read(samples)
		peakLive = max(peakLive, samples[0].Value.Uint64())
		peakInuse = max(peakInuse, samples[1].Value.Uint64())
	}
	go func() {
		defer close(fin)
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				sample()
			case <-done:
				sample()
				return
			}
		}
	}()
	return func() (live, inuse float64) {
		close(done)
		<-fin
		return float64(peakLive) / 1e6, float64(peakInuse) / 1e6
	}
}

// countRecordsInKeys sums the record counts the config writes into object
// keys (-n<count>-), which is enough to know when an export has finished
// without downloading anything.
func (e *testEnv) countRecordsInKeys(t testing.TB, prefix string) int {
	t.Helper()
	n := 0
	for _, key := range e.listObjects(t, prefix) {
		m := objectKeyRE.FindStringSubmatch(key)
		require.NotNil(t, m, "object key %q does not match the config's key scheme", key)
		c, err := strconv.Atoi(m[objectKeyRE.SubexpIndex("count")])
		require.NoError(t, err)
		n += c
	}
	return n
}

// configProcessors returns the config's processors as YAML documents in
// execution order: pipeline.processors followed by the output's
// batching.processors.
func configProcessors(t testing.TB) []string {
	t.Helper()
	var conf struct {
		Pipeline struct {
			Processors []yaml.Node `yaml:"processors"`
		} `yaml:"pipeline"`
		Output struct {
			RejectErrored struct {
				S3 struct {
					Batching struct {
						Processors []yaml.Node `yaml:"processors"`
					} `yaml:"batching"`
				} `yaml:"aws_s3"`
			} `yaml:"reject_errored"`
		} `yaml:"output"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(exports.Config), &conf))
	nodes := append(conf.Pipeline.Processors, conf.Output.RejectErrored.S3.Batching.Processors...)
	require.NotEmpty(t, nodes, "no processors found in the config")
	out := make([]string, len(nodes))
	for i, n := range nodes {
		doc, err := yaml.Marshal(&n)
		require.NoError(t, err)
		out[i] = string(doc)
	}
	return out
}

// runMockSchemaRegistry serves one protobuf schema by id, which is all
// schema_registry_decode asks of the registry.
func runMockSchemaRegistry(t testing.TB, id int, schema string) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"schema": schema, "schemaType": "PROTOBUF"})
	require.NoError(t, err)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != fmt.Sprintf("/schemas/ids/%d", id) {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.Close)
	return ts.URL
}
