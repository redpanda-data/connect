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
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/bufbuild/protocompile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/exports"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/awstest"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/redpandatest"
)

const (
	topic   = "audit-log"
	subject = topic + "-value"

	kafkaUser = "audit-log-export"
	kafkaPass = "scram-password-for-test"
	bucket    = "acme-corp-siem-ingest"
	s3Region  = "eu-west-1" // what the shared localstack helper creates buckets in
)

var objectKeyRE = regexp.MustCompile(`^(?P<prefix>.+/)(?P<date>\d{4}/\d{2}/\d{2})/` + regexp.QuoteMeta(topic) + `-p(?P<partition>\d+)-o(?P<offset>\d+)-n(?P<count>\d+)-(?P<hash>[0-9]+)\.ndjson\.gz$`)

// TestIntegrationAuditLogExport runs the config against a real Redpanda
// (SASL + schema-registry basic auth, as in Cloud) and localstack S3:
//
//   - export: produce the fixtures in the producer's wire format, run the pipeline,
//     check every record lands as OCSF NDJSON matching testdata/expected.ndjson,
//     validates against the OCSF 1.8 schema, and that object keys follow the
//     config's deterministic scheme.
//   - poison: add one undecodable record, run as a new pipeline (new id, so a
//     new consumer group), check all good records still land, the poison one is
//     logged with its position, and the consumer group never commits past it
//     (the pipeline stalls there instead of skipping it).
func TestIntegrationAuditLogExport(t *testing.T) {
	integration.CheckSkip(t)

	env := startEnv(t)

	fixtures := loadFixtures(t, "testdata/events.jsonl")
	expected := loadNDJSON(t, "testdata/expected.ndjson")
	require.Len(t, expected, len(fixtures), "testdata mismatch between events.jsonl and expected.ndjson")

	encode := newAuditEventEncoder(t, env.sr)
	produced := env.produce(t, encode, fixtures)
	t.Logf("produced %d fixtures onto %s (3 partitions)", len(produced), topic)

	// Pair fixtures with their expected records by metadata.uid, not by
	// position, so a reordered testdata file fails loudly here.
	uidToRecord := map[string]*kgo.Record{}
	for i, fx := range fixtures {
		uidToRecord[fixtureUID(t, fx)] = produced[i]
	}
	for _, rec := range expected {
		require.Contains(t, uidToRecord, recordUID(t, rec), "expected.ndjson has a record with no matching fixture in events.jsonl")
	}

	t.Run("export", func(t *testing.T) {
		const prefix = "run1"
		logs, stopErr := env.runPipelineUntil(t, "pipeline-1", prefix, func() bool {
			return env.countRecords(t, prefix) >= len(expected)
		}, 0)
		require.NoError(t, stopErr, "a healthy pipeline must stop cleanly")

		objects := env.listObjects(t, prefix)
		var got []map[string]any
		for _, key := range objects {
			records := env.fetchGzipNDJSON(t, key)
			got = append(got, records...)
			for _, rec := range records {
				requireValidOCSF(t, key, rec)
			}
			m := objectKeyRE.FindStringSubmatch(key)
			if !assert.NotNil(t, m, "object key %q does not match the config's key scheme", key) {
				continue
			}
			assert.Equal(t, prefix+"/", m[objectKeyRE.SubexpIndex("prefix")], "object %q: prefix gets exactly one trailing slash", key)
			assert.Equal(t, fmt.Sprint(len(records)), m[objectKeyRE.SubexpIndex("count")], "object %q: record count in key", key)
			// The partition/offset in the key are the first record's.
			first := uidToRecord[recordUID(t, records[0])]
			if assert.NotNil(t, first, "object %q: first record has an unknown metadata.uid", key) {
				assert.Contains(t, key, fmt.Sprintf("p%d-o%d", first.Partition, first.Offset), "object %q: first record position", key)
			}
		}
		t.Logf("%d objects, %d records: %v", len(objects), len(got), objects)
		assertSameRecords(t, expected, got)
		for line := range strings.SplitSeq(logs, "\n") {
			assert.NotContains(t, line, "level=ERROR", "pipeline logged an error during a clean export")
		}
	})

	t.Run("poison", func(t *testing.T) {
		// One record that is not schema-registry framed, pinned to partition 0,
		// followed by one more good record on the same partition so the
		// poison sits in the middle of a fetched batch: the good records
		// around it must still be exported while the group never commits
		// past it.
		manual, err := kgo.NewClient(env.kafkaOpts(kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
		require.NoError(t, err)
		defer manual.Close()
		poison := &kgo.Record{Topic: topic, Partition: 0, Key: []byte("poison"), Value: []byte("not a protobuf audit event")}
		require.NoError(t, manual.ProduceSync(t.Context(), poison).FirstErr())
		after, afterExpected := withUID(t, fixtures[0], expected[0], "after-poison-"+recordUID(t, expected[0]))
		require.NoError(t, manual.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Partition: 0, Key: []byte(after.Key), Value: encode(after.Event)}).FirstErr())
		t.Logf("poison record at partition %d offset %d, followed by one good record", poison.Partition, poison.Offset)
		expectedWithAfter := append(slices.Clone(expected), afterExpected)

		// A new pipeline id means a new consumer group, so this re-reads
		// everything from the start. New prefix so the objects are
		// distinguishable from the first run's. Linger past commit_period
		// (5s) so the offsets we check have actually been committed.
		const prefix = "run2"
		logs, stopErr := env.runPipelineUntil(t, "pipeline-2", prefix, func() bool {
			return env.countRecords(t, prefix) >= len(expectedWithAfter)
		}, 12*time.Second)
		// The rejected record is still being retried, so the input can't
		// drain and a graceful stop times out. That is the stall, observed
		// from the inside; the offset check below is the same thing from
		// the broker's side.
		t.Logf("graceful stop of the stalled pipeline returned: %v", stopErr)

		var got []map[string]any
		for _, key := range env.listObjects(t, prefix) {
			records := env.fetchGzipNDJSON(t, key)
			got = append(got, records...)
			for _, rec := range records {
				requireValidOCSF(t, key, rec)
			}
		}
		assertSameRecords(t, expectedWithAfter, got)

		assert.Contains(t, logs, "audit event could not be exported", "poison record not logged")
		assert.Regexp(t, fmt.Sprintf(`kafka_offset="?%d"?`, poison.Offset), logs, "poison record not logged with its offset")

		admin := kadm.NewClient(env.kafka)
		committed, err := admin.FetchOffsets(t.Context(), "audit-log-export-pipeline-2")
		require.NoError(t, err)
		ends, err := admin.ListEndOffsets(t.Context(), topic)
		require.NoError(t, err)
		for _, p := range []int32{0, 1, 2} {
			c, ok := committed.Lookup(topic, p)
			end, _ := ends.Lookup(topic, p)
			if p == poison.Partition {
				// The group must never commit past the poison record. (It may
				// not have committed anything for this partition: the input
				// tracks offsets per fetched batch, and a batch with a rejected
				// record in it is never acked as a whole.)
				if ok {
					assert.LessOrEqual(t, c.At, poison.Offset, "partition %d: committed past the poison record", p)
				}
				t.Logf("partition %d: committed=%v (ok=%v), poison at %d, end %d", p, c.At, ok, poison.Offset, end.Offset)
				continue
			}
			if assert.True(t, ok, "partition %d: no committed offset", p) {
				assert.Equal(t, end.Offset, c.At, "partition %d: not committed to the end", p)
			}
		}
	})
}

// --- environment --------------------------------------------------------------

type testEnv struct {
	broker    string
	srURL     string
	s3Port    string
	kafka     *kgo.Client
	sr        *sr.Client
	s3        *s3.Client
	kafkaOpts func(extra ...kgo.Opt) []kgo.Opt
}

func startEnv(t testing.TB) *testEnv {
	t.Helper()

	ctr, err := tcredpanda.Run(t.Context(), "docker.redpanda.com/redpandadata/redpanda:latest",
		tcredpanda.WithEnableSASL(),
		tcredpanda.WithNewServiceAccount(kafkaUser, kafkaPass),
		tcredpanda.WithSuperusers(kafkaUser),
		tcredpanda.WithEnableSchemaRegistryHTTPBasicAuth(),
		redpandatest.WithLowAIOUsage(),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	broker, err := ctr.KafkaSeedBroker(t.Context())
	require.NoError(t, err)
	srURL, err := ctr.SchemaRegistryAddress(t.Context())
	require.NoError(t, err)

	// localstack is shared by every test in the package, so the bucket may
	// already exist (a benchmark's rounds each start an env, for example).
	s3Port := awstest.GetLocalStack(t)
	if err := awstest.CreateBucket(t.Context(), s3Port, bucket); err != nil {
		var owned *s3types.BucketAlreadyOwnedByYou
		require.ErrorAs(t, err, &owned, "creating bucket %s", bucket)
	}
	s3Endpoint := "http://localhost:" + s3Port
	awsConf, err := awsconfig.LoadDefaultConfig(t.Context(),
		awsconfig.WithRegion(s3Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	require.NoError(t, err)
	awsConf.BaseEndpoint = &s3Endpoint
	s3Client := s3.NewFromConfig(awsConf, func(o *s3.Options) { o.UsePathStyle = true })

	kafkaOpts := func(extra ...kgo.Opt) []kgo.Opt {
		return append([]kgo.Opt{
			kgo.SeedBrokers(broker),
			kgo.SASL(scram.Auth{User: kafkaUser, Pass: kafkaPass}.AsSha256Mechanism()),
		}, extra...)
	}
	kc, err := kgo.NewClient(kafkaOpts()...)
	require.NoError(t, err)
	t.Cleanup(kc.Close)
	// Same partition count as the production topic.
	_, err = kadm.NewClient(kc).CreateTopics(t.Context(), 3, 1, nil, topic)
	require.NoError(t, err)

	src, err := sr.NewClient(sr.URLs(srURL), sr.BasicAuth(kafkaUser, kafkaPass))
	require.NoError(t, err)

	t.Logf("redpanda broker=%s schema_registry=%s s3=%s", broker, srURL, s3Endpoint)
	return &testEnv{broker: broker, srURL: srURL, s3Port: s3Port, kafka: kc, sr: src, s3: s3Client, kafkaOpts: kafkaOpts}
}

// inputs is what the pipeline sees: the variables Cloud injects into every
// pipeline pod, plus the dataplane secrets created for this export.
func (e *testEnv) inputs(pipelineID, prefix string) map[string]string {
	return map[string]string{
		"REDPANDA_BROKERS":                               e.broker,
		"REDPANDA_SCHEMA_REGISTRY_URL":                   e.srURL,
		"REDPANDA_TLS_ENABLED":                           "false",
		"REDPANDA_PIPELINE_ID":                           pipelineID,
		"secrets.AUDIT_LOG_EXPORT_TOPIC":                 topic,
		"secrets.AUDIT_LOG_EXPORT_KAFKA_USERNAME":        kafkaUser,
		"secrets.AUDIT_LOG_EXPORT_KAFKA_PASSWORD":        kafkaPass,
		"secrets.AUDIT_LOG_EXPORT_S3_BUCKET":             bucket,
		"secrets.AUDIT_LOG_EXPORT_S3_REGION":             s3Region,
		"secrets.AUDIT_LOG_EXPORT_S3_PREFIX":             prefix,
		"secrets.AUDIT_LOG_EXPORT_S3_ENDPOINT":           "http://localhost:" + e.s3Port,
		"secrets.AUDIT_LOG_EXPORT_S3_FORCE_PATH_STYLE":   "true",
		"secrets.AUDIT_LOG_EXPORT_AWS_ACCESS_KEY_ID":     "xxxxx",
		"secrets.AUDIT_LOG_EXPORT_AWS_SECRET_ACCESS_KEY": "xxxxx",
		"secrets.AUDIT_LOG_EXPORT_BATCH_PERIOD":          "2s",
	}
}

// runPipelineUntil builds the config in-process with the given inputs, runs
// it until done() (or a timeout), lingers for linger, then stops it. Returns
// the pipeline's log output and the result of the graceful stop: a pipeline
// stalled on a rejected record cannot drain, so its stop times out.
func (e *testEnv) runPipelineUntil(t *testing.T, pipelineID, prefix string, done func() bool, linger time.Duration) (logs string, stopErr error) {
	t.Helper()
	stream, logBuf := buildStream(t, e.inputs(pipelineID, prefix))

	runCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- stream.Run(runCtx) }()

	if !assert.Eventually(t, done, 90*time.Second, 500*time.Millisecond, "pipeline did not reach the expected state") {
		t.Fatalf("pipeline logs:\n%s", logBuf.String())
	}
	time.Sleep(linger)

	// A failed graceful stop force-closes the components; cancelling the run
	// context then unblocks Run.
	stopErr = stream.StopWithin(20 * time.Second)
	cancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("pipeline run returned: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Error("pipeline did not return from Run after cancellation")
	}
	return logBuf.String(), stopErr
}

// buildStream builds the config in-process, resolving ${...} references from
// inputs, with the pipeline's logs captured in the returned buffer.
func buildStream(t testing.TB, inputs map[string]string) (*service.Stream, *lockedBuffer) {
	t.Helper()
	logBuf := &lockedBuffer{}
	sb := service.NewStreamBuilder()
	sb.SetLogger(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	sb.SetEnvVarLookupFunc(func(key string) (string, bool) {
		v, ok := inputs[key]
		return v, ok
	})
	require.NoError(t, sb.SetYAML(exports.Config))
	stream, err := sb.Build()
	require.NoError(t, err)
	return stream, logBuf
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// --- fixtures -----------------------------------------------------------------

// fixture is one line of testdata/events.jsonl.
type fixture struct {
	Name  string          `json:"name"`
	Key   string          `json:"key"`
	Event json.RawMessage `json:"event"`
}

func loadFixtures(t testing.TB, path string) []fixture {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var out []fixture
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 8<<20)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var fx fixture
		require.NoError(t, json.Unmarshal(sc.Bytes(), &fx))
		out = append(out, fx)
	}
	require.NoError(t, sc.Err())
	return out
}

func fixtureUID(t *testing.T, fx fixture) string {
	t.Helper()
	var ev map[string]any
	require.NoError(t, json.Unmarshal(fx.Event, &ev), "fixture %s", fx.Name)
	return recordUID(t, ev)
}

// withUID returns copies of a fixture and its expected record with
// metadata.uid replaced, so a second copy of an event can be produced and
// told apart from the original.
func withUID(t *testing.T, fx fixture, exp map[string]any, uid string) (fixture, map[string]any) {
	t.Helper()
	var ev map[string]any
	require.NoError(t, json.Unmarshal(fx.Event, &ev))
	ev["metadata"].(map[string]any)["uid"] = uid
	raw, err := json.Marshal(ev)
	require.NoError(t, err)
	fx.Event = raw
	fx.Name += "+" + uid

	var expCopy map[string]any
	expJSON, err := json.Marshal(exp)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(expJSON, &expCopy))
	expCopy["metadata"].(map[string]any)["uid"] = uid
	return fx, expCopy
}

// newAuditEventEncoder registers the AuditEvent schema under the topic's
// -value subject, as the producer does at start-up, and returns a function
// producing the Confluent wire format for a protojson event: magic byte,
// schema id, protobuf message index, message.
func newAuditEventEncoder(t testing.TB, srClient *sr.Client) func(event json.RawMessage) []byte {
	t.Helper()
	md, schema := compileAuditEvent(t)
	ss, err := srClient.CreateSchema(t.Context(), subject, sr.Schema{Schema: schema, Type: sr.TypeProtobuf})
	require.NoError(t, err)
	return auditEventEncoder(t, md, ss.ID)
}

// compileAuditEvent compiles testdata/audit_event.proto and returns the
// AuditEvent descriptor along with the schema text as registered.
func compileAuditEvent(t testing.TB) (protoreflect.MessageDescriptor, string) {
	t.Helper()
	schema, err := os.ReadFile("testdata/audit_event.proto")
	require.NoError(t, err)

	files, err := (&protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(map[string]string{"audit_event.proto": string(schema)}),
		},
	}).Compile(t.Context(), "audit_event.proto")
	require.NoError(t, err)
	md := files[0].Messages().ByName("AuditEvent")
	require.NotNil(t, md, "AuditEvent message not found in schema")
	return md, string(schema)
}

// auditEventEncoder returns a function producing the Confluent wire format
// for a protojson event under the given schema id.
func auditEventEncoder(t testing.TB, md protoreflect.MessageDescriptor, schemaID int) func(event json.RawMessage) []byte {
	t.Helper()
	serde := sr.NewSerde()
	serde.Register(schemaID, &dynamicpb.Message{},
		sr.EncodeFn(func(v any) ([]byte, error) { return proto.Marshal(v.(proto.Message)) }),
		// AuditEvent is the first top-level message in the schema.
		sr.Index(0),
	)
	return func(event json.RawMessage) []byte {
		msg := dynamicpb.NewMessage(md)
		require.NoError(t, protojson.Unmarshal(event, msg))
		b, err := serde.Encode(msg)
		require.NoError(t, err)
		return b
	}
}

// produce writes every fixture the way the producer does: encoded value, the recorded
// key ("<ResourceType>/<ResourceID>"), default partitioner. Returns the
// records as produced so the test can reason about partitions and offsets.
func (e *testEnv) produce(t testing.TB, encode func(json.RawMessage) []byte, fixtures []fixture) []*kgo.Record {
	t.Helper()
	out := make([]*kgo.Record, 0, len(fixtures))
	for _, fx := range fixtures {
		rec := &kgo.Record{Topic: topic, Key: []byte(fx.Key), Value: encode(fx.Event)}
		require.NoError(t, e.kafka.ProduceSync(t.Context(), rec).FirstErr(), "fixture %s", fx.Name)
		out = append(out, rec)
	}
	return out
}

// --- assertions ---------------------------------------------------------------

func loadNDJSON(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	return decodeNDJSON(t, f, path)
}

func decodeNDJSON(t *testing.T, r io.Reader, name string) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 8<<20)
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) == 0 {
			continue
		}
		var m map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &m), "%s: invalid JSON line: %s", name, sc.Bytes())
		out = append(out, m)
	}
	require.NoError(t, sc.Err())
	return out
}

func recordUID(t *testing.T, rec map[string]any) string {
	t.Helper()
	meta, _ := rec["metadata"].(map[string]any)
	uid, _ := meta["uid"].(string)
	require.NotEmpty(t, uid, "record without metadata.uid: %v", rec)
	return uid
}

// assertSameRecords checks got is exactly the expected set (by metadata.uid)
// and every record is deep-equal to its expected form.
func assertSameRecords(t *testing.T, expected, got []map[string]any) {
	t.Helper()
	byUID := func(recs []map[string]any) map[string]map[string]any {
		out := map[string]map[string]any{}
		for _, r := range recs {
			uid := recordUID(t, r)
			_, dup := out[uid]
			assert.False(t, dup, "duplicate record %s", uid)
			out[uid] = r
		}
		return out
	}
	want, have := byUID(expected), byUID(got)
	for uid, w := range want {
		h, ok := have[uid]
		if assert.True(t, ok, "expected record %s missing from output", uid) {
			assert.Equal(t, w, h, "record %s differs from expected", uid)
		}
	}
	for uid := range have {
		_, ok := want[uid]
		assert.True(t, ok, "unexpected record %s in output", uid)
	}
}

// --- S3 helpers ---------------------------------------------------------------

func (e *testEnv) listObjects(t testing.TB, prefix string) []string {
	t.Helper()
	var keys []string
	p := s3.NewListObjectsV2Paginator(e.s3, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix + "/")})
	for p.HasMorePages() {
		page, err := p.NextPage(t.Context())
		require.NoError(t, err)
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	slices.Sort(keys)
	return keys
}

func (e *testEnv) countRecords(t *testing.T, prefix string) int {
	t.Helper()
	n := 0
	for _, key := range e.listObjects(t, prefix) {
		n += len(e.fetchGzipNDJSON(t, key))
	}
	return n
}

func (e *testEnv) fetchGzipNDJSON(t *testing.T, key string) []map[string]any {
	t.Helper()
	obj, err := e.s3.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err)
	defer obj.Body.Close()
	gz, err := gzip.NewReader(obj.Body)
	require.NoError(t, err, "%s is not gzip", key)
	defer gz.Close()
	return decodeNDJSON(t, gz, key)
}
