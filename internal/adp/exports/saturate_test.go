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
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/adp/exports"
)

// TestSaturateAuditLogExport is the memory sizing harness for the config. It
// reproduces the deployment the config runs in: the real cloud binary, in a
// container with the pod's memory and CPU limits, started against a topic
// that already holds a large backlog (start_from_oldest, so the pipeline
// reads as fast as the limits allow). While the pipeline drains the backlog
// the harness samples the container's cgroup memory and CPU, the pipeline's
// own metrics (Go heap, GC, records in and out), the records that reached
// S3 and the consumer group lag, once a second, and writes heap profiles as
// memory crosses 50/75/90% of the limit. A container the kernel kills is
// recorded as such and, if SATURATE_RESTARTS allows, started again the way
// the pod would be.
//
// It is opt-in: it needs Docker and a few minutes, so it runs only with
// SATURATE set. Everything else is an environment variable with a default
// that matches the config and the production pod:
//
//	SATURATE=1 go test ./internal/adp/exports -run '^TestSaturateAuditLogExport$' -v -timeout 2h
//
//	SATURATE_EVENTS            backlog to pre-load (400000)
//	SATURATE_EVENT_BYTES       mean encoded protobuf size to pad the fixtures to (1600; 0 = as they are)
//	SATURATE_PARTITIONS        topic partitions (3)
//	SATURATE_MEMORY            container memory limit, k8s notation: 400M, 1G, 512Mi (400M)
//	SATURATE_CPUS              container CPU limit (0.1)
//	SATURATE_GOMEMLIMIT        GOMEMLIMIT for the pipeline process, e.g. 360MiB (unset)
//	SATURATE_GOGC              GOGC for the pipeline process (unset)
//	SATURATE_BATCH_COUNT       AUDIT_LOG_EXPORT_BATCH_COUNT (config default)
//	SATURATE_BATCH_PERIOD      AUDIT_LOG_EXPORT_BATCH_PERIOD (config default)
//	SATURATE_BATCH_BYTES       output batching.byte_size (config value)
//	SATURATE_MAX_IN_FLIGHT     output max_in_flight (config value)
//	SATURATE_CHECKPOINT_LIMIT  input unordered_processing.checkpoint_limit (config value)
//	SATURATE_SET               any other field, as comma-separated path=value pairs, e.g.
//	                           input.redpanda.fetch_max_bytes=8MiB,input.redpanda.fetch_max_partition_bytes=256KiB
//	SATURATE_DURATION          stop after this long even if the backlog is not drained (15m)
//	SATURATE_RESTARTS          restarts after a kill before giving up (0)
//	SATURATE_SAMPLE            sample interval (1s)
//	SATURATE_BINARY            linux redpanda-connect-cloud binary to run (built from the working tree)
//	SATURATE_OUT               output directory ($TMPDIR/audit-log-export-saturate/<time>)
//	SATURATE_HTTP_PORT         port for the pipeline's metrics and pprof (a free one)
//
// Output: samples.csv (one row per sample), connect-<life>.log, heap profiles
// (`go tool pprof -top <binary> heap-*.pb.gz`), and report.md with the
// knobs, every container life and its end, and the peaks.
//
// Padding grows api.response.data (API activity) or entity.data (entity
// management) with a JSON payload string, which is where the variable part
// of a real event lives. A string decodes to one Go string, cheaper on the
// heap than the nested maps the rest of an event becomes, so read the
// per-event heap cost as a lower bound for events that are big because they
// have more structure.
func TestSaturateAuditLogExport(t *testing.T) {
	integration.CheckSkip(t)
	if os.Getenv("SATURATE") == "" {
		t.Skip("set SATURATE=1 to run the saturation harness")
	}
	k := loadSaturateKnobs(t)
	require.NoError(t, os.MkdirAll(k.out, 0o755))
	t.Logf("output directory: %s", k.out)

	docker, err := testcontainers.NewDockerClientWithOpts(t.Context())
	require.NoError(t, err)
	info, err := docker.Info(t.Context(), client.InfoOptions{})
	require.NoError(t, err)
	binary := k.binary
	if binary == "" {
		binary = buildCloudBinary(t, info.Info.Architecture, filepath.Join(k.out, "redpanda-connect-cloud"))
	}

	env := startEnv(t)
	seed := env.seedBacklog(t, k)

	id := fmt.Sprintf("saturate-%d", time.Now().Unix())
	inputs := env.inputs(id, id)
	// inputs() shortens the batch period for the functional tests; the
	// harness runs the config's own unless asked otherwise.
	delete(inputs, "secrets.AUDIT_LOG_EXPORT_BATCH_PERIOD")
	if k.batchCount != "" {
		inputs["secrets.AUDIT_LOG_EXPORT_BATCH_COUNT"] = k.batchCount
	}
	if k.batchPeriod != "" {
		inputs["secrets.AUDIT_LOG_EXPORT_BATCH_PERIOD"] = k.batchPeriod
	}
	confPath := filepath.Join(k.out, "connect.yaml")
	require.NoError(t, os.WriteFile(confPath, saturateConfig(t, k), 0o644))

	r := &saturateRun{t: t, k: k, env: env, docker: docker, seed: seed, binary: binary, confPath: confPath, inputs: inputs, prefix: id}
	r.openSamples()
	defer r.samples.Flush()
	r.run()
	r.report()
}

type saturateKnobs struct {
	events, partitions      int
	eventBytes              int
	memory                  int64
	cpus                    float64
	gomemlimit, gogc        string
	batchCount, batchPeriod string
	batchBytes, maxInFlight string
	checkpointLimit         string
	set                     []string
	duration, sample        time.Duration
	restarts                int
	binary, out             string
	httpPort                int
}

func loadSaturateKnobs(t *testing.T) saturateKnobs {
	t.Helper()
	str := func(name, def string) string {
		if v, ok := os.LookupEnv("SATURATE_" + name); ok {
			return v
		}
		return def
	}
	num := func(name string, def int) int {
		v := str(name, "")
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		require.NoError(t, err, "SATURATE_%s", name)
		return n
	}
	dur := func(name string, def time.Duration) time.Duration {
		v := str(name, "")
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		require.NoError(t, err, "SATURATE_%s", name)
		return d
	}
	mem, err := parseK8sQuantity(str("MEMORY", "400M"))
	require.NoError(t, err, "SATURATE_MEMORY")
	cpus, err := strconv.ParseFloat(str("CPUS", "0.1"), 64)
	require.NoError(t, err, "SATURATE_CPUS")
	out := str("OUT", "")
	if out == "" {
		out = filepath.Join(os.TempDir(), "audit-log-export-saturate", time.Now().Format("20060102T150405"))
	}
	return saturateKnobs{
		events:          num("EVENTS", 400000),
		eventBytes:      num("EVENT_BYTES", 1600),
		partitions:      num("PARTITIONS", 3),
		memory:          mem,
		cpus:            cpus,
		gomemlimit:      str("GOMEMLIMIT", ""),
		gogc:            str("GOGC", ""),
		batchCount:      str("BATCH_COUNT", ""),
		batchPeriod:     str("BATCH_PERIOD", ""),
		batchBytes:      str("BATCH_BYTES", ""),
		maxInFlight:     str("MAX_IN_FLIGHT", ""),
		checkpointLimit: str("CHECKPOINT_LIMIT", ""),
		set:             splitNonEmpty(str("SET", "")),
		duration:        dur("DURATION", 15*time.Minute),
		sample:          dur("SAMPLE", time.Second),
		restarts:        num("RESTARTS", 0),
		binary:          str("BINARY", ""),
		out:             out,
		httpPort:        num("HTTP_PORT", freePort(t)),
	}
}

func splitNonEmpty(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// freePort picks a port that is free on this host, for the pipeline's HTTP
// server: with host networking a fixed port could collide.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// parseK8sQuantity reads a memory quantity the way a pod spec does: a plain
// number of bytes, a decimal suffix (k, M, G) or a binary one (Ki, Mi, Gi).
func parseK8sQuantity(s string) (int64, error) {
	s = strings.TrimSpace(s)
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{
		{"Ki", 1 << 10},
		{"Mi", 1 << 20},
		{"Gi", 1 << 30},
		{"k", 1e3},
		{"M", 1e6},
		{"G", 1e9},
	} {
		if rest, ok := strings.CutSuffix(s, u.suffix); ok {
			s, mult = rest, u.mult
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("memory quantity %q: %w", s, err)
	}
	return n * mult, nil
}

// buildCloudBinary cross-compiles the cloud distribution from the working
// tree for the Docker host's architecture.
func buildCloudBinary(t *testing.T, dockerArch, out string) string {
	t.Helper()
	goarch := map[string]string{"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}[dockerArch]
	require.NotEmpty(t, goarch, "unknown docker architecture %q", dockerArch)
	root, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}").Output()
	require.NoError(t, err)
	t.Logf("building redpanda-connect-cloud for linux/%s", goarch)
	start := time.Now()
	cmd := exec.Command("go", "build", "-o", out, "./cmd/redpanda-connect-cloud")
	cmd.Dir = strings.TrimSpace(string(root))
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+goarch, "CGO_ENABLED=0")
	outb, err := cmd.CombinedOutput()
	require.NoError(t, err, "go build: %s", outb)
	t.Logf("built in %s", time.Since(start).Round(time.Second))
	return out
}

// saturateConfig is the shipped config with the harness's overrides applied
// and the observability the harness needs turned on: Go and process metrics
// on the Prometheus endpoint and the pprof endpoints.
func saturateConfig(t *testing.T, k saturateKnobs) []byte {
	t.Helper()
	var doc yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte(exports.Config), &doc))
	root := doc.Content[0]
	set := func(value string, path ...string) {
		if value != "" {
			setYAMLPath(t, root, value, path...)
		}
	}
	set(k.batchBytes, "output", "reject_errored", "aws_s3", "batching", "byte_size")
	set(k.maxInFlight, "output", "reject_errored", "aws_s3", "max_in_flight")
	set(k.checkpointLimit, "input", "redpanda", "unordered_processing", "checkpoint_limit")
	for _, kv := range k.set {
		path, value, ok := strings.Cut(kv, "=")
		require.True(t, ok, "SATURATE_SET entry %q is not path=value", kv)
		set(value, strings.Split(path, ".")...)
	}
	set("0.0.0.0:"+strconv.Itoa(k.httpPort), "http", "address")
	set("true", "http", "debug_endpoints")
	set("true", "metrics", "prometheus", "add_process_metrics")
	set("true", "metrics", "prometheus", "add_go_metrics")
	out, err := yaml.Marshal(&doc)
	require.NoError(t, err)
	return out
}

// setYAMLPath sets a scalar at a path of mapping keys, creating mappings on
// the way. The scalar keeps YAML's own typing (true, 10, 60s stay what they
// look like), which is what the config's existing values rely on.
func setYAMLPath(t *testing.T, node *yaml.Node, value string, path ...string) {
	t.Helper()
	require.Equal(t, yaml.MappingNode, node.Kind, "expected a mapping at %v", path)
	key := path[0]
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value != key {
			continue
		}
		if len(path) == 1 {
			node.Content[i+1] = &yaml.Node{Kind: yaml.ScalarNode, Value: value}
			return
		}
		setYAMLPath(t, node.Content[i+1], value, path[1:]...)
		return
	}
	var child *yaml.Node
	if len(path) == 1 {
		child = &yaml.Node{Kind: yaml.ScalarNode, Value: value}
	} else {
		child = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		setYAMLPath(t, child, value, path[1:]...)
	}
	node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Value: key}, child)
}

type seedStats struct {
	events     int
	bytes      int64
	meanBytes  int
	padBytes   int
	seedTime   time.Duration
	fixtureSet int
}

// seedBacklog recreates the topic with the wanted partition count and loads
// it with the fixtures, padded to the wanted mean size, round-robin across
// partitions.
func (e *testEnv) seedBacklog(t *testing.T, k saturateKnobs) seedStats {
	t.Helper()
	admin := kadm.NewClient(e.kafka)
	_, err := admin.DeleteTopics(t.Context(), topic)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, err := admin.CreateTopics(t.Context(), int32(k.partitions), 1, nil, topic)
		if err != nil {
			return false
		}
		r, ok := resp[topic]
		return ok && r.Err == nil
	}, 30*time.Second, 200*time.Millisecond, "recreating topic %s", topic)

	fixtures := loadFixtures(t, "testdata/events.jsonl")
	encode := newAuditEventEncoder(t, e.sr)
	base := make([]int, len(fixtures))
	var baseTotal int
	for i, fx := range fixtures {
		base[i] = len(encode(fx.Event))
		baseTotal += base[i]
	}
	pad := 0
	if k.eventBytes > 0 && k.eventBytes*len(fixtures) > baseTotal {
		// Every fixture grows by the same amount; the proto string header
		// adds a few bytes, which a second pass corrects.
		pad = k.eventBytes - baseTotal/len(fixtures)
	}
	records := make([]*kgo.Record, len(fixtures))
	var total int64
	for i, fx := range fixtures {
		ev := fx.Event
		if pad > 0 {
			ev = padEvent(t, ev, pad)
		}
		records[i] = &kgo.Record{Topic: topic, Key: []byte(fx.Key), Value: encode(ev)}
		total += int64(len(records[i].Value))
	}
	stats := seedStats{events: k.events, padBytes: pad, fixtureSet: len(fixtures), meanBytes: int(total) / len(fixtures)}
	t.Logf("fixtures: %d, mean encoded size %d bytes before padding, %d after (pad %d)", len(fixtures), baseTotal/len(fixtures), stats.meanBytes, pad)

	producer, err := kgo.NewClient(e.kafkaOpts(kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))...)
	require.NoError(t, err)
	defer producer.Close()
	start := time.Now()
	const chunk = 2000
	for sent := 0; sent < k.events; sent += chunk {
		n := min(chunk, k.events-sent)
		recs := make([]*kgo.Record, n)
		for i := range n {
			src := records[(sent+i)%len(records)]
			recs[i] = &kgo.Record{Topic: topic, Key: src.Key, Value: src.Value}
			stats.bytes += int64(len(src.Value))
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		err := producer.ProduceSync(ctx, recs...).FirstErr()
		cancel()
		require.NoError(t, err, "producing events %d-%d of the backlog", sent, sent+n)
		if done := sent + n; done%50000 < chunk || done == k.events {
			t.Logf("seeded %d/%d", done, k.events)
		}
	}
	stats.seedTime = time.Since(start)
	t.Logf("seeded %d events (%.1f MB of protobuf) on %d partitions in %s", k.events, float64(stats.bytes)/1e6, k.partitions, stats.seedTime.Round(time.Second))
	return stats
}

// padEvent grows an event by about n encoded bytes with a JSON payload in
// the class's data field: api.response.data for API activity, entity.data
// for entity management.
func padEvent(t *testing.T, event json.RawMessage, n int) json.RawMessage {
	t.Helper()
	var ev map[string]any
	require.NoError(t, json.Unmarshal(event, &ev))
	path := []string{"api", "response"}
	if cls, _ := ev["class_uid"].(string); cls == "CLASS_UID_ENTITY_MANAGEMENT" {
		path = []string{"entity"}
	}
	obj := ev
	for _, k := range path {
		child, _ := obj[k].(map[string]any)
		if child == nil {
			child = map[string]any{}
			obj[k] = child
		}
		obj = child
	}
	existing, _ := obj["data"].(string)
	// The payload is a JSON object string, like a serialised request body.
	// Proto framing costs a tag and a varint length per level, hence -8.
	filler := strings.Repeat("x", max(0, n-len(existing)-len(`{"payload":""}`)-8))
	obj["data"] = fmt.Sprintf(`{"payload":"%s"}`, filler)
	out, err := json.Marshal(ev)
	require.NoError(t, err)
	return out
}

// --- the run ----------------------------------------------------------------

type saturateRun struct {
	t        *testing.T
	k        saturateKnobs
	env      *testEnv
	docker   *testcontainers.DockerClient
	seed     seedStats
	binary   string
	confPath string
	inputs   map[string]string
	prefix   string

	samples  *csv.Writer
	sampleF  *os.File
	lives    []saturateLife
	started  time.Time
	exported int
	peak     saturateSample
}

type saturateLife struct {
	n          int
	start, end time.Time
	exitCode   int
	oomKilled  bool
	reason     string
	exportedAt int // records in S3 when the life ended
	peakUsage  uint64
	peakAnon   uint64
	peakHeap   float64
	peakFlight float64
	throttled  float64 // fraction of CPU periods throttled over the life
	profiles   []string
}

type saturateSample struct {
	at        time.Time
	usage     uint64
	anon      uint64
	file      uint64
	cpuPct    float64
	thrPct    float64
	heapInuse float64
	heapAlloc float64
	goSys     float64
	rss       float64
	gcCount   float64
	gorout    float64
	received  float64
	inFlight  float64
	s3Events  int
	lag       int64
	alive     bool

	// cumulative CFS counters since the container started
	cpuPeriods, cpuThrottled uint64
}

var sampleHeader = []string{"t_s", "life", "alive", "mem_usage_mb", "mem_anon_mb", "mem_file_mb", "cpu_pct", "throttled_pct", "heap_inuse_mb", "heap_alloc_mb", "go_sys_mb", "process_rss_mb", "gc_count", "goroutines", "input_received", "in_flight", "s3_events", "group_lag"}

func (r *saturateRun) openSamples() {
	f, err := os.Create(filepath.Join(r.k.out, "samples.csv"))
	require.NoError(r.t, err)
	r.sampleF = f
	r.samples = csv.NewWriter(f)
	require.NoError(r.t, r.samples.Write(sampleHeader))
}

func (r *saturateRun) run() {
	r.started = time.Now()
	deadline := r.started.Add(r.k.duration)
	for life := 0; life <= r.k.restarts; life++ {
		if time.Now().After(deadline) || r.exported >= r.k.events {
			break
		}
		l := r.runLife(life, deadline)
		r.lives = append(r.lives, l)
		if l.reason == "drained" || l.reason == "timeout" {
			break
		}
	}
	r.samples.Flush()
	require.NoError(r.t, r.sampleF.Close())
}

// runLife starts one container and samples it until it stops, drains the
// backlog or the deadline passes.
func (r *saturateRun) runLife(n int, deadline time.Time) saturateLife {
	t := r.t
	life := saturateLife{n: n, start: time.Now()}
	t.Logf("life %d: starting container (memory %s, cpus %g)", n, fmtBytes(uint64(r.k.memory)), r.k.cpus)

	env := make([]string, 0, len(r.inputs)+2)
	for k, v := range r.inputs {
		env = append(env, k+"="+v)
	}
	if r.k.gomemlimit != "" {
		env = append(env, "GOMEMLIMIT="+r.k.gomemlimit)
	}
	if r.k.gogc != "" {
		env = append(env, "GOGC="+r.k.gogc)
	}
	// Copied in rather than bind-mounted: a VM-hosted Docker (colima,
	// Docker Desktop) only shares some host paths with the VM.
	files := []testcontainers.ContainerFile{
		{HostFilePath: r.binary, ContainerFilePath: "/redpanda-connect", FileMode: 0o755},
		{HostFilePath: r.confPath, ContainerFilePath: "/connect.yaml", FileMode: 0o644},
	}
	if _, err := os.Stat("/etc/redpanda/redpanda.license"); err == nil {
		files = append(files, testcontainers.ContainerFile{HostFilePath: "/etc/redpanda/redpanda.license", ContainerFilePath: "/etc/redpanda/redpanda.license", FileMode: 0o644})
	}
	ctx := context.Background()
	req := testcontainers.GenericContainerRequest{
		Image:      "busybox",
		Entrypoint: []string{"/redpanda-connect"},
		Cmd:        []string{"run", "/connect.yaml"},
		Env:        envMap(env),
		Files:      files,
		HostConfigModifier: func(hc *container.HostConfig) {
			// Host networking so the pipeline reaches Redpanda and
			// localstack at the localhost ports they advertise. The memory
			// limit has no swap behind it, as in a pod.
			hc.NetworkMode = "host"
			hc.Memory = r.k.memory
			hc.MemorySwap = r.k.memory
			hc.NanoCPUs = int64(r.k.cpus * 1e9)
		},
		Started: true,
	}
	ctr, err := testcontainers.GenericContainer(ctx, req)
	require.NoError(t, err)
	defer func() {
		logs, lerr := ctr.Logs(ctx)
		if lerr == nil {
			b, _ := io.ReadAll(logs)
			_ = os.WriteFile(filepath.Join(r.k.out, fmt.Sprintf("connect-%d.log", n)), b, 0o644)
		}
		_ = ctr.Terminate(ctx)
	}()

	var (
		prevCPU, prevSys    uint64
		periods, thrPeriods uint64 // last cumulative CFS counters seen
		profiled            = map[int]bool{}
		ticker              = time.NewTicker(r.k.sample)
	)
	defer ticker.Stop()
	for {
		<-ticker.C
		s := r.sample(ctx, ctr, &prevCPU, &prevSys)
		if s.alive && s.cpuPeriods > 0 {
			periods, thrPeriods = s.cpuPeriods, s.cpuThrottled
		}
		r.exported = s.s3Events
		r.writeSample(n, s)
		life.peakUsage = max(life.peakUsage, s.usage)
		life.peakAnon = max(life.peakAnon, s.anon)
		life.peakHeap = math.Max(life.peakHeap, s.heapInuse)
		life.peakFlight = math.Max(life.peakFlight, s.inFlight)
		if s.usage > r.peak.usage {
			r.peak = s
		}
		for _, pct := range []int{50, 75, 90} {
			if !profiled[pct] && s.alive && float64(s.usage) >= float64(r.k.memory)*float64(pct)/100 {
				profiled[pct] = true
				if p := r.heapProfile(ctx, ctr, n, pct); p != "" {
					life.profiles = append(life.profiles, p)
				}
			}
		}

		switch {
		case !s.alive:
			state, err := ctr.State(ctx)
			require.NoError(t, err)
			life.exitCode, life.oomKilled = state.ExitCode, state.OOMKilled
			life.reason = fmt.Sprintf("exited %d", state.ExitCode)
			if state.OOMKilled || state.ExitCode == 137 {
				life.reason = "OOM killed"
			}
		case s.s3Events >= r.k.events:
			life.reason = "drained"
		case time.Now().After(deadline):
			life.reason = "timeout"
		default:
			continue
		}
		break
	}
	life.end = time.Now()
	life.exportedAt = r.exported
	if periods > 0 {
		life.throttled = float64(thrPeriods) / float64(periods)
	}
	t.Logf("life %d: %s after %s; %d records in S3; peak memory %s (anon %s), peak heap in use %.0f MB, peak in flight %.0f, throttled %.0f%% of CPU periods",
		n, life.reason, life.end.Sub(life.start).Round(time.Second), life.exportedAt, fmtBytes(life.peakUsage), fmtBytes(life.peakAnon), life.peakHeap/1e6, life.peakFlight, life.throttled*100)
	return life
}

func envMap(kv []string) map[string]string {
	m := make(map[string]string, len(kv))
	for _, e := range kv {
		k, v, _ := strings.Cut(e, "=")
		m[k] = v
	}
	return m
}

// sample takes one reading of everything the harness watches.
func (r *saturateRun) sample(ctx context.Context, ctr testcontainers.Container, prevCPU, prevSys *uint64) saturateSample {
	t := r.t
	s := saturateSample{at: time.Now()}

	st, err := r.docker.ContainerStats(ctx, ctr.GetContainerID(), client.ContainerStatsOptions{})
	if err == nil {
		var resp container.StatsResponse
		derr := json.NewDecoder(st.Body).Decode(&resp)
		_ = st.Body.Close()
		if derr == nil && resp.Read.After(time.Time{}) && resp.MemoryStats.Limit > 0 {
			s.alive = true
			s.usage = resp.MemoryStats.Usage
			s.anon = resp.MemoryStats.Stats["anon"]
			s.file = resp.MemoryStats.Stats["file"]
			if s.anon == 0 { // cgroup v1
				s.anon = resp.MemoryStats.Stats["rss"]
				s.file = resp.MemoryStats.Stats["cache"]
			}
			cpu, sys := resp.CPUStats.CPUUsage.TotalUsage, resp.CPUStats.SystemUsage
			if *prevSys > 0 && sys > *prevSys {
				s.cpuPct = float64(cpu-*prevCPU) / float64(sys-*prevSys) * float64(resp.CPUStats.OnlineCPUs) * 100
			}
			*prevCPU, *prevSys = cpu, sys
			s.cpuPeriods = resp.CPUStats.ThrottlingData.Periods
			s.cpuThrottled = resp.CPUStats.ThrottlingData.ThrottledPeriods
			if s.cpuPeriods > 0 {
				s.thrPct = float64(s.cpuThrottled) / float64(s.cpuPeriods) * 100
			}
		}
	}
	if s.alive {
		if state, err := ctr.State(ctx); err != nil || !state.Running {
			s.alive = false
		}
	}
	if s.alive {
		r.scrapeMetrics(ctx, ctr, &s)
	}
	s.s3Events = r.env.countRecordsInKeys(t, r.prefix)
	if s.received > 0 {
		s.inFlight = s.received - float64(s.s3Events)
	}
	if lags, err := kadm.NewClient(r.env.kafka).Lag(ctx, "audit-log-export-"+r.inputs["REDPANDA_PIPELINE_ID"]); err == nil {
		if l, ok := lags["audit-log-export-"+r.inputs["REDPANDA_PIPELINE_ID"]]; ok && l.Error() == nil {
			s.lag = l.Lag.Total()
		}
	}
	return s
}

// scrapeMetrics reads the pipeline's Prometheus endpoint from inside the
// container (host networking on a VM-hosted Docker is not reachable from
// the test process) and picks out the series the report uses.
func (r *saturateRun) scrapeMetrics(ctx context.Context, ctr testcontainers.Container, s *saturateSample) {
	body := wget(ctx, ctr, fmt.Sprintf("http://127.0.0.1:%d/metrics", r.k.httpPort))
	if body == nil {
		return
	}
	sc := bufio.NewScanner(strings.NewReader(string(body)))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name, value := metricLine(line)
		switch name {
		case "go_memstats_heap_inuse_bytes":
			s.heapInuse = value
		case "go_memstats_heap_alloc_bytes":
			s.heapAlloc = value
		case "go_memstats_sys_bytes":
			s.goSys = value
		case "process_resident_memory_bytes":
			s.rss = value
		case "go_gc_duration_seconds_count":
			s.gcCount = value
		case "go_goroutines":
			s.gorout = value
		case "input_received":
			s.received += value
		}
	}
}

// metricLine splits a Prometheus text line into the bare metric name and its
// value; labels are dropped so series with several label sets can be summed.
func metricLine(line string) (string, float64) {
	name, raw, ok := strings.CutLast(line, " ")
	if !ok {
		return "", 0
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return "", 0
	}
	name, _, _ = strings.Cut(name, "{")
	return strings.TrimSpace(name), v
}

// wget fetches a URL served by the pipeline. The container shares the
// Docker host's network, so on a Linux host, or a VM that forwards
// listening ports (colima, Docker Desktop), it is reachable from here
// directly; otherwise it falls back to wget inside the container, which
// competes with the pipeline for its CPU limit.
func wget(ctx context.Context, ctr testcontainers.Container, url string) []byte {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil); err == nil {
		if resp, err := http.DefaultClient.Do(req); err == nil {
			b, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err == nil && resp.StatusCode == http.StatusOK {
				return b
			}
		}
	}
	code, out, err := ctr.Exec(ctx, []string{"wget", "-q", "-T", "5", "-O", "-", url}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		return nil
	}
	b, err := io.ReadAll(out)
	if err != nil {
		return nil
	}
	return b
}

func (r *saturateRun) heapProfile(ctx context.Context, ctr testcontainers.Container, life, pct int) string {
	b := wget(ctx, ctr, fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/heap", r.k.httpPort))
	if len(b) == 0 {
		return ""
	}
	p := filepath.Join(r.k.out, fmt.Sprintf("heap-life%d-%d.pb.gz", life, pct))
	if err := os.WriteFile(p, b, 0o644); err != nil {
		return ""
	}
	r.t.Logf("life %d: memory crossed %d%% of the limit, heap profile at %s", life, pct, p)
	return p
}

func (r *saturateRun) writeSample(life int, s saturateSample) {
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }
	mb := func(v float64) string { return f(v / 1e6) }
	require.NoError(r.t, r.samples.Write([]string{
		f(s.at.Sub(r.started).Seconds()), strconv.Itoa(life), strconv.FormatBool(s.alive),
		mb(float64(s.usage)), mb(float64(s.anon)), mb(float64(s.file)), f(s.cpuPct), f(s.thrPct),
		mb(s.heapInuse), mb(s.heapAlloc), mb(s.goSys), mb(s.rss), f(s.gcCount), f(s.gorout),
		f(s.received), f(s.inFlight), strconv.Itoa(s.s3Events), strconv.FormatInt(s.lag, 10),
	}))
	r.samples.Flush()
}

// report writes report.md and logs the headline.
func (r *saturateRun) report() {
	t := r.t
	k := r.k
	elapsed := time.Since(r.started)
	var b strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&b, format+"\n", a...) }
	w("# Audit-log export saturation run")
	w("")
	w("%s, %d events of ~%d bytes (%.0f MB) pre-loaded on %d partitions; pipeline run with memory %s, %g CPUs%s%s.",
		r.started.Format(time.RFC3339), r.seed.events, r.seed.meanBytes, float64(r.seed.bytes)/1e6, k.partitions,
		fmtBytes(uint64(k.memory)), k.cpus, optStr(", GOMEMLIMIT=", k.gomemlimit), optStr(", GOGC=", k.gogc))
	w("")
	w("| knob | value |")
	w("|---|---|")
	w("| batching.count | %s |", orDefault(k.batchCount, "config default"))
	w("| batching.byte_size | %s |", orDefault(k.batchBytes, "config value"))
	w("| batching.period | %s |", orDefault(k.batchPeriod, "config default"))
	w("| max_in_flight | %s |", orDefault(k.maxInFlight, "config value"))
	w("| checkpoint_limit | %s |", orDefault(k.checkpointLimit, "config value"))
	for _, kv := range k.set {
		path, value, _ := strings.Cut(kv, "=")
		w("| %s | %s |", path, value)
	}
	w("| binary | %s |", r.binary)
	w("")
	w("## Result")
	w("")
	ooms := 0
	for _, l := range r.lives {
		if l.oomKilled || l.exitCode == 137 {
			ooms++
		}
	}
	rate := float64(r.exported) / elapsed.Seconds()
	w("%d of %d events exported in %s (%.0f events/s overall); %d container lives, %d OOM kills; peak memory %s.",
		r.exported, k.events, elapsed.Round(time.Second), rate, len(r.lives), ooms, fmtBytes(r.peak.usage))
	w("")
	w("| life | ran for | ended | events in S3 at end | peak memory | peak anon | peak Go heap in use | peak in flight | CPU throttled |")
	w("|---|---|---|---|---|---|---|---|---|")
	for _, l := range r.lives {
		w("| %d | %s | %s | %d | %s | %s | %.0f MB | %.0f | %.0f%% |", l.n, l.end.Sub(l.start).Round(time.Second), l.reason, l.exportedAt,
			fmtBytes(l.peakUsage), fmtBytes(l.peakAnon), l.peakHeap/1e6, l.peakFlight, l.throttled*100)
	}
	w("")
	w("At peak memory (%s at t+%s): Go heap in use %.0f MB, heap allocated %.0f MB, Go sys %.0f MB, process RSS %.0f MB, %.0f records in flight (received %.0f), %d in S3, lag %d.",
		fmtBytes(r.peak.usage), r.peak.at.Sub(r.started).Round(time.Second), r.peak.heapInuse/1e6, r.peak.heapAlloc/1e6, r.peak.goSys/1e6, r.peak.rss/1e6, r.peak.inFlight, r.peak.received, r.peak.s3Events, r.peak.lag)
	w("")
	w("Files: `samples.csv` (one row per %s), `connect-<life>.log`, heap profiles below.", k.sample)
	w("")
	for _, l := range r.lives {
		for _, p := range l.profiles {
			w("- `go tool pprof -top %s %s`", r.binary, p)
		}
	}
	w("")
	w("Memory is the cgroup's usage, what the kernel compares with the limit; it includes page cache (`file`). `anon` is the process's own memory. In flight is the pipeline's `input_received` minus the events in S3: records read from the topic that are not yet in an object. Process RSS also counts pages of the binary mapped from the image, which the cgroup does not charge as anon.")
	path := filepath.Join(k.out, "report.md")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
	t.Logf("%d of %d events exported in %s; %d lives, %d OOM kills; peak memory %s; report at %s",
		r.exported, k.events, elapsed.Round(time.Second), len(r.lives), ooms, fmtBytes(r.peak.usage), path)
}

func optStr(prefix, v string) string {
	if v == "" {
		return ""
	}
	return prefix + v
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func fmtBytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.0f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
