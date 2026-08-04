// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gopkg.in/yaml.v3"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "bench":
		exitOnErr(benchCmd(os.Args[2:]))
	case "validate":
		exitOnErr(validateCmd(os.Args[2:]))
	case "down":
		exitOnErr(downCmd(os.Args[2:]))
	case "cost-check":
		exitOnErr(costCheckCmd(os.Args[2:]))
	case "summary":
		exitOnErr(summaryCmd(os.Args[2:]))
	default:
		usage()
		os.Exit(2)
	}
}

func exitOnErr(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  runner bench --scenario=<path> [--keep] [--keep-on-fail]
  runner validate --scenario=<path>
  runner down --scenario=<path>
  runner cost-check
  runner summary [--repo-root=<path>]`)
}

type benchOpts struct {
	scenarioPath string
	keep         bool
	keepOnFail   bool
	region       string
	repoRoot     string
	licenseFile  string
	engines      []string
}

func benchCmd(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	scenario := fs.String("scenario", "", "path to scenario YAML (e.g. scenarios/postgres/orders-cdc.yaml)")
	keep := fs.Bool("keep", false, "don't tear down infra after the run")
	keepOnFail := fs.Bool("keep-on-fail", false, "keep infra if the bench errors")
	region := fs.String("region", "us-east-2", "AWS region")
	repoRoot := fs.String("repo-root", ".", "path to the connect repo root")
	licenseFile := fs.String("license-file", os.Getenv("REDPANDA_LICENSE_FILEPATH"),
		"path to a Redpanda Enterprise license file (defaults to $REDPANDA_LICENSE_FILEPATH). "+
			"Required for enterprise connectors like postgres_cdc.")
	engines := fs.String("engines", "connect,kafka_connect",
		"Comma-separated engines to sweep at each vCPU point. Default runs both Connect and Kafka Connect side-by-side.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *scenario == "" {
		return fmt.Errorf("--scenario is required")
	}

	engineList := strings.Split(*engines, ",")
	for i, e := range engineList {
		engineList[i] = strings.TrimSpace(e)
	}

	opts := benchOpts{
		scenarioPath: *scenario,
		keep:         *keep,
		keepOnFail:   *keepOnFail,
		region:       *region,
		repoRoot:     *repoRoot,
		licenseFile:  *licenseFile,
		engines:      engineList,
	}
	return runBench(opts)
}

func runBench(opts benchOpts) (errOut error) {
	s, err := LoadScenario(opts.scenarioPath)
	if err != nil {
		return err
	}
	if opts.licenseFile == "" {
		return fmt.Errorf("--license-file is required (or set REDPANDA_LICENSE_FILEPATH); enterprise connectors won't start without one")
	}
	// Actually open the file (not just stat) so macOS TCC / sandbox / permissions
	// failures surface before we provision any AWS infrastructure.
	if f, err := os.Open(opts.licenseFile); err != nil {
		return fmt.Errorf("license file %q: %w", opts.licenseFile, err)
	} else {
		f.Close()
	}
	fmt.Printf("[1/7] loaded scenario %s\n", s.Name)

	// matrix.arms compares Connect launch topologies (one iceberg pipeline vs.
	// N streams-mode pipelines), not engines — Kafka Connect has no notion of
	// streams, so arms require the sweep to be Connect-only. Checked here,
	// before any infra apply / build / render / seed, so an invalid
	// combination fails immediately instead of after minutes of wall-clock
	// and real AWS spend.
	if len(s.Matrix.Arms) > 0 {
		if len(opts.engines) != 1 || opts.engines[0] != "connect" {
			return fmt.Errorf("matrix.arms requires --engines=connect (got %v): arms compare Connect launch topologies, not engines", opts.engines)
		}
	}

	topo, err := topologyFor(s.Direction)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// BackendFile must be absolute: `terraform -chdir=<stack>` changes the
	// working directory before resolving the -backend-config path.
	backendAbs, err := filepath.Abs(filepath.Join(opts.repoRoot, "benchmarking/aws/terraform/backend.hcl"))
	if err != nil {
		return fmt.Errorf("resolve backend.hcl: %w", err)
	}
	tfShared := &Terraform{
		Dir:         SharedDir(opts.repoRoot),
		BackendFile: backendAbs,
		StateKey:    "shared",
	}
	tfStack := &Terraform{
		Dir:         StackDir(opts.repoRoot, s.Stack),
		BackendFile: backendAbs,
		StateKey:    s.Stack,
	}

	if err := tfShared.Init(); err != nil {
		return fmt.Errorf("terraform init shared: %w", err)
	}
	if err := tfStack.Init(); err != nil {
		return fmt.Errorf("terraform init %s: %w", s.Stack, err)
	}
	sessionID := newSessionID()
	names := newBenchNames(sessionID, s.Connector)
	sharedVars := map[string]string{
		"region":               opts.region,
		"runner_instance_type": s.Infra.Runner.InstanceType,
		"bench_session_id":     sessionID,
	}
	stackVars := translateInfraSource(s.Infra.Source, opts.region)

	// Register destroy BEFORE any apply, so a partial apply still gets torn
	// down. terraform destroy is idempotent against a no-op state.
	defer func() {
		if opts.keep {
			fmt.Println("[7/7] keep=true: skipping teardown")
			return
		}
		if errOut != nil && opts.keepOnFail {
			fmt.Println("[7/7] keep-on-fail=true and run errored: skipping teardown")
			return
		}
		fmt.Println("[7/7] terraform destroy")
		_ = tfStack.Destroy(stackVars)
		_ = tfShared.Destroy(sharedVars)
	}()

	if err := tfShared.Apply(sharedVars); err != nil {
		return fmt.Errorf("terraform apply shared: %w", err)
	}
	fmt.Println("[2/7] terraform apply (shared + stack) complete")

	if err := tfStack.Apply(stackVars); err != nil {
		return fmt.Errorf("terraform apply %s: %w", s.Stack, err)
	}

	sharedOuts, err := tfShared.Outputs()
	if err != nil {
		return fmt.Errorf("terraform output shared: %w", err)
	}
	stackOuts, err := tfStack.Outputs()
	if err != nil {
		return fmt.Errorf("terraform output stack: %w", err)
	}
	for k, v := range stackOuts {
		sharedOuts[k] = v
	}
	// The runner-provided session ID is a Terraform input, not output — inject
	// it here so per-engine renderers (renderPipelineConfig, buildKCRenderInputs,
	// combineReset) can read it via outs["bench_session_id"].
	sharedOuts["bench_session_id"] = sessionID
	// aws_region is data the runner already holds (not a TF output). Sink Glue
	// calls (catalog region, glue CLI --region) read it from outs["aws_region"].
	sharedOuts["aws_region"] = opts.region

	binPath, err := buildConnect(opts.repoRoot)
	if err != nil {
		return fmt.Errorf("build connect: %w", err)
	}
	fmt.Println("[3/7] built redpanda-connect")

	plan := buildSweepPlan(s)
	// legacy scenarios (no matrix.arms) share one config across every point,
	// staged at the historical stage/config.yaml -> /opt/bench/config.yaml
	// path, so the six existing scenarios are byte-for-byte unchanged.
	legacy := len(s.Matrix.Arms) == 0
	var sets []renderedPointConfigs
	if legacy {
		set, err := renderPointConfigs(s, sharedOuts, topo, names, plan[0])
		if err != nil {
			return fmt.Errorf("render pipeline config: %w", err)
		}
		sets = []renderedPointConfigs{set}
	} else {
		for _, p := range plan {
			set, err := renderPointConfigs(s, sharedOuts, topo, names, p)
			if err != nil {
				return fmt.Errorf("render pipeline config for %s: %w", p.Key(), err)
			}
			sets = append(sets, set)
		}
	}
	// Upload the iceberg-tablegen binary (sink scenarios only) BEFORE
	// stageArtefacts, whose SSM script downloads it onto the runner — otherwise
	// the object isn't in S3 yet and the best-effort download no-ops.
	if err := stageTableGenForSink(ctx, opts, s, sharedOuts); err != nil {
		return fmt.Errorf("stage iceberg-tablegen: %w", err)
	}
	if err := stageArtefacts(ctx, opts, sharedOuts, binPath, sets, legacy); err != nil {
		return fmt.Errorf("stage artefacts: %w", err)
	}
	fmt.Println("[4/7] staged binary + config on runner")

	if err := runSeeder(ctx, opts, s, sharedOuts, topo, names); err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	fmt.Println("[5/7] seed complete")

	ssmExec, err := NewSSMExecutor(ctx, opts.region)
	if err != nil {
		return err
	}
	logFetcher, err := NewS3LogFetcher(ctx, opts.region)
	if err != nil {
		return err
	}

	var kcConnectorName, kcConfigJSON string
	needsKC := false
	for _, e := range opts.engines {
		if e == "kafka_connect" {
			needsKC = true
			break
		}
	}
	if needsKC {
		res, ok, err := topo.KCConfig(s, sharedOuts, names)
		if err != nil {
			return fmt.Errorf("KC config: %w", err)
		}
		if !ok {
			return fmt.Errorf("engine list includes kafka_connect but direction %q has no KC counterpart", s.Direction)
		}
		kcConnectorName = res.ConnectorName
		kcConfigJSON = res.ConfigJSON
	}

	mr := &MatrixRunner{
		SSM:             ssmExec,
		LogFetcher:      logFetcher,
		RunnerInstance:  sharedOuts["runner_instance_id"],
		LoadGenInstance: sharedOuts["load_gen_instance_id"],
		ConfigPath:      "/opt/bench/config.yaml",
		ConfigPaths: func() map[string]pointConfigPaths {
			if legacy {
				return nil // every point uses ConfigPath
			}
			return runnerConfigPaths(sets)
		}(),
		BinaryPath:               "/opt/bench/redpanda-connect",
		Bucket:                   sharedOuts["results_bucket"],
		SessionID:                sessionID,
		RedpandaMetricsEndpoint:  sharedOuts["redpanda_metrics_endpoint"],
		RedpandaMetricsEndpoints: sharedOuts["redpanda_metrics_endpoints"],
		Engines:                  opts.engines,
		KCConnectorName:          kcConnectorName,
		KCConnectorConfigJSON:    kcConfigJSON,
		Topology:                 topo,
		Names:                    names,
		Topics:                   s.Dataset.Topics,
		Outs:                     sharedOuts,
		Direction:                s.Direction,
	}
	// Reset must cover the union of every arm's tables (planMaxStreams), not
	// just this scenario's own Streams, so one precomputed reset script serves
	// every point in the plan regardless of its stream count.
	reset, err := topo.ResetScript(s, sharedOuts, names.WithStreams(planMaxStreams(plan)))
	if err != nil {
		return err
	}
	workload, err := topo.WorkloadScript(s, sharedOuts, names)
	if err != nil {
		return err
	}
	warmup := time.Duration(0)
	duration := time.Duration(0)
	if s.Workload != nil {
		warmup = s.Workload.Warmup
		duration = s.Workload.Duration
	} else {
		duration = minDuration
	}
	points, err := mr.Run(ctx, plan, s.Matrix.GoMemLimitPerVCPU, warmup, duration, reset, workload)
	if err != nil {
		return err
	}
	fmt.Println("[6/7] sweep complete")

	result := &Result{
		Scenario:     fmt.Sprintf("%s/%s", s.Stack, strings.TrimPrefix(s.Name, s.Stack+"-")),
		ScenarioHash: hashScenario(s),
		GitSHA:       gitSHA(opts.repoRoot),
		StartedAt:    time.Now().Add(-totalDuration(s, len(points))).UTC(),
		FinishedAt:   time.Now().UTC(),
		Infra: ResultInfra{
			RunnerInstanceType:  s.Infra.Runner.InstanceType,
			SourceInstanceClass: asString(s.Infra.Source["instance_class"]),
			SourceStorageGB:     asInt(s.Infra.Source["storage_gb"]),
			Region:              opts.region,
		},
		Dataset: ResultDataset{
			Rows:         s.Dataset.InitialRows,
			RowSizeBytes: s.Dataset.RowSizeBytes,
			TotalBytes:   s.Dataset.InitialRows * int64(s.Dataset.RowSizeBytes),
		},
	}
	for _, p := range points {
		result.Points = append(result.Points, PointResult{
			VCPU:         p.VCPU,
			Engine:       p.Engine,
			Samples:      p.Samples,
			Summary:      p.Summary,
			Anomalies:    p.Anomalies,
			Prom:         p.Prom,
			BrokerSeries: p.BrokerSeries,
			Arm:          p.ArmID,
			GOMAXPROCS:   p.GOMAXPROCS,
			Streams:      p.Streams,
		})
	}
	var connectPts, kcPts []PointResult
	for _, p := range result.Points {
		switch p.Engine {
		case "connect":
			connectPts = append(connectPts, p)
		case "kafka_connect":
			kcPts = append(kcPts, p)
		}
	}
	result.CrossEngineAnomalies = DetectCrossEngineAnomalies(connectPts, kcPts, 2.0)
	resultsDir := filepath.Join(opts.repoRoot, "benchmarking/aws/results")
	jsonPath, err := WriteResultJSON(resultsDir, result)
	if err != nil {
		return err
	}
	mdPath := filepath.Join(opts.repoRoot, "docs/benchmark-results", s.Stack+".md")
	if err := AppendMarkdown(mdPath, result, strings.TrimSpace(s.Description)); err != nil {
		return err
	}
	summaryPath := filepath.Join(opts.repoRoot, "docs/benchmark-results/SUMMARY.md")
	if err := RefreshSummary(summaryPath, resultsDir, time.Now()); err != nil {
		// Non-fatal: a bench run that produced a valid result should not fail
		// because the project-level summary couldn't be rewritten.
		fmt.Fprintf(os.Stderr, "warning: refresh SUMMARY.md: %v\n", err)
	}
	fmt.Printf("\n✓ done — JSON: %s\n           md: %s\n           summary: %s\n", jsonPath, mdPath, summaryPath)
	return nil
}

func hashScenario(s *Scenario) string {
	raw, _ := yaml.Marshal(s)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func gitSHA(repoRoot string) string {
	out, err := exec.Command("git", "-C", repoRoot, "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func totalDuration(s *Scenario, points int) time.Duration {
	if s.Workload == nil {
		return time.Duration(points) * minDuration
	}
	return time.Duration(points) * (s.Workload.Warmup + s.Workload.Duration)
}

func newSessionID() string {
	return fmt.Sprintf("bench-%s", time.Now().UTC().Format("20060102-150405"))
}

func validateCmd(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	scenario := fs.String("scenario", "", "scenario YAML")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *scenario == "" {
		return fmt.Errorf("--scenario is required")
	}
	s, err := LoadScenario(*scenario)
	if err != nil {
		return err
	}
	fmt.Printf("scenario %s OK (%d cpu points, runner %s)\n",
		s.Name, len(s.Matrix.CPUPoints), s.Infra.Runner.InstanceType)
	return nil
}

func downCmd(args []string) error {
	fs := flag.NewFlagSet("down", flag.ExitOnError)
	scenario := fs.String("scenario", "", "scenario YAML")
	region := fs.String("region", "us-east-2", "AWS region")
	repoRoot := fs.String("repo-root", ".", "repo root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *scenario == "" {
		return fmt.Errorf("--scenario is required")
	}
	s, err := LoadScenario(*scenario)
	if err != nil {
		return err
	}
	backendAbs, err := filepath.Abs(filepath.Join(*repoRoot, "benchmarking/aws/terraform/backend.hcl"))
	if err != nil {
		return fmt.Errorf("resolve backend.hcl: %w", err)
	}
	stack := &Terraform{
		Dir:         StackDir(*repoRoot, s.Stack),
		BackendFile: backendAbs,
		StateKey:    s.Stack,
	}
	shared := &Terraform{
		Dir:         SharedDir(*repoRoot),
		BackendFile: backendAbs,
		StateKey:    "shared",
	}
	_ = stack.Init()
	_ = shared.Init()
	if err := stack.Destroy(translateInfraSource(s.Infra.Source, *region)); err != nil {
		return err
	}
	return shared.Destroy(map[string]string{
		"region":               *region,
		"runner_instance_type": s.Infra.Runner.InstanceType,
	})
}

func costCheckCmd(args []string) error {
	fs := flag.NewFlagSet("cost-check", flag.ExitOnError)
	region := fs.String("region", "us-east-2", "AWS region (informational; CE is global)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	ce, err := NewAWSCostExplorer(ctx)
	if err != nil {
		return fmt.Errorf("init cost explorer: %w", err)
	}
	report, err := SummariseCosts(ctx, ce, *region, time.Now())
	if err != nil {
		return err
	}
	Print(os.Stdout, report)
	return nil
}

func summaryCmd(args []string) error {
	fs := flag.NewFlagSet("summary", flag.ExitOnError)
	repoRoot := fs.String("repo-root", ".", "path to the connect repo root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	summaryPath := filepath.Join(*repoRoot, "docs/benchmark-results/SUMMARY.md")
	resultsDir := filepath.Join(*repoRoot, "benchmarking/aws/results")
	if err := RefreshSummary(summaryPath, resultsDir, time.Now()); err != nil {
		return err
	}
	fmt.Printf("refreshed %s\n", summaryPath)
	return nil
}

// translateInfraSource flattens a scenario's infra.source map into terraform
// -var-style strings. Nested maps (e.g. parameters) are JSON-encoded so HCL
// can decode them.
func translateInfraSource(src map[string]any, region string) map[string]string {
	out := map[string]string{"region": region}
	for k, v := range src {
		switch val := v.(type) {
		case string:
			out[k] = val
		case int:
			out[k] = fmt.Sprintf("%d", val)
		case int64:
			out[k] = fmt.Sprintf("%d", val)
		case float64:
			out[k] = fmt.Sprintf("%v", val)
		case []any:
			// YAML sequences (e.g. table_names: [a, b, c]) JSON-encode to a
			// valid HCL list literal — ["a","b","c"] — for a list-typed -var.
			b, _ := json.Marshal(val)
			out[k] = string(b)
		case map[string]any:
			b, _ := json.Marshal(val)
			out[k] = string(b)
		default:
			out[k] = fmt.Sprintf("%v", val)
		}
	}
	return out
}

func asString(v any) string { s, _ := v.(string); return s }
func asInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	}
	return 0
}

func buildConnect(repoRoot string) (string, error) {
	dist := filepath.Join(repoRoot, "benchmarking/aws/runner/dist")
	if err := os.MkdirAll(dist, 0o755); err != nil {
		return "", err
	}
	out := filepath.Join(dist, "redpanda-connect")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/redpanda-connect")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return out, nil
}

// renderedPointConfigs is one sweep point's rendered config set, as local temp
// file paths. Single is set for single-pipeline points; Root plus Streams for
// streams-mode points.
type renderedPointConfigs struct {
	Key     string
	Single  string
	Root    string
	Streams []string
}

// writeTempYAML marshals cfg, resolves ${TF_OUTPUT} placeholders, and writes it
// to a temp file, returning the path.
func writeTempYAML(cfg map[string]any, outs map[string]string, pattern string) (string, error) {
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", err
	}
	defer tmp.Close()
	if _, err := tmp.WriteString(substitutePlaceholders(string(raw), outs)); err != nil {
		return "", err
	}
	return tmp.Name(), nil
}

// rootSections are the observability and service-wide fields. In streams mode
// they live in the -o root config; in run mode they sit alongside the pipeline
// in the single config.
func rootSections(s *Scenario) map[string]any {
	cfg := map[string]any{
		"http": map[string]any{"debug_endpoints": true},
		"redpanda": map[string]any{
			"seed_brokers": []string{"${REDPANDA_BROKER_ENDPOINTS}"},
		},
		"logger": map[string]any{"level": "INFO"},
		"metrics": map[string]any{
			"prometheus": map[string]any{"add_process_metrics": true, "add_go_metrics": true},
		},
	}
	// Connectors that require a persistent checkpoint (e.g. mysql_cdc) declare
	// cache_resources in the scenario's pipeline block. Resources are
	// service-wide, so they belong here in both modes.
	if cr, ok := s.Pipeline["cache_resources"]; ok {
		cfg["cache_resources"] = cr
	}
	return cfg
}

// renderPipelineConfig renders the single-config shape: rootSections plus
// input/output/buffer, exactly as the pre-arms renderer produced. Used both
// for arm-less scenarios and for single-stream arms.
func renderPipelineConfig(s *Scenario, outs map[string]string, topo Topology, names BenchNames) (string, error) {
	input, output, err := topo.Pipeline(s, names)
	if err != nil {
		return "", fmt.Errorf("render pipeline: %w", err)
	}
	cfg := rootSections(s)
	cfg["input"] = input
	cfg["output"] = output
	// A scenario may declare a top-level buffer (e.g. memory) to decouple a
	// fast input from a commit-latency-bound output like the iceberg sink.
	if buf, ok := s.Pipeline["buffer"]; ok {
		cfg["buffer"] = buf
	}
	return writeTempYAML(cfg, outs, "bench-config-*.yaml")
}

// renderPointConfigs renders the launch config(s) for one sweep point. A point
// with Streams <= 1 gets a single config identical in shape to the pre-arms
// renderer. A multi-stream point gets a root config (observability only) plus
// one stream config per pipeline, each writing its own Iceberg table but
// sharing the source topic and consumer group.
func renderPointConfigs(s *Scenario, outs map[string]string, topo Topology, names BenchNames, p sweepPoint) (renderedPointConfigs, error) {
	out := renderedPointConfigs{Key: p.Key()}

	// Arms carry a merged pipeline; arm-less points use the scenario's own.
	armScenario := *s
	if p.Pipeline != nil {
		armScenario.Pipeline = p.Pipeline
	}

	if p.Streams <= 1 {
		path, err := renderPipelineConfig(&armScenario, outs, topo, names.WithStreams(1))
		if err != nil {
			return renderedPointConfigs{}, err
		}
		out.Single = path
		return out, nil
	}

	rootPath, err := writeTempYAML(rootSections(&armScenario), outs, "bench-root-*.yaml")
	if err != nil {
		return renderedPointConfigs{}, fmt.Errorf("render root config for %s: %w", out.Key, err)
	}
	out.Root = rootPath

	// topo.Pipeline (e.g. sinkTopology.Pipeline) mutates and returns a map
	// that ALIASES armScenario.Pipeline["output"][<component>] in place — it
	// does not copy it. Each stream's output map must therefore be marshalled
	// to disk (writeTempYAML, below) BEFORE the next iteration calls
	// topo.Pipeline again and re-mutates that same shared map for the next
	// stream's table name. That ordering holds today because the loop is
	// sequential. Do NOT parallelize this loop: two goroutines mutating the
	// same aliased map concurrently would race, and the most likely visible
	// symptom is two streams silently marshalling the SAME table name and
	// committing to one Iceberg table instead of two — exactly the class of
	// silent-corruption failure this whole review exists to catch, and not
	// something a test would reliably catch either.
	for i := 0; i < p.Streams; i++ {
		streamNames := names.WithStreams(p.Streams).WithStream(i)
		input, output, err := topo.Pipeline(&armScenario, streamNames)
		if err != nil {
			return renderedPointConfigs{}, fmt.Errorf("render stream %d of %s: %w", i, out.Key, err)
		}
		cfg := map[string]any{"input": input, "output": output}
		if buf, ok := armScenario.Pipeline["buffer"]; ok {
			cfg["buffer"] = buf
		}
		path, err := writeTempYAML(cfg, outs, fmt.Sprintf("bench-stream%d-*.yaml", i))
		if err != nil {
			return renderedPointConfigs{}, fmt.Errorf("write stream %d of %s: %w", i, out.Key, err)
		}
		out.Streams = append(out.Streams, path)
	}
	return out, nil
}

// hostCfgDir is the per-point config directory on the runner host. This is
// the single source of truth for that path: runnerConfigPaths (the launch
// path MatrixRunner.Run substitutes into the bench script) and
// buildStagePlan (the download path stageArtefacts' SSM script copies into)
// both call this instead of rebuilding the "/opt/bench/cfg/<key>" literal
// independently, so the two can never drift. A drift would mean the engine
// launches against a path that doesn't exist on the host — which, combined
// with the early-abort guard covering every arm point (see matrix.go), would
// otherwise surface as a silent 0 MB/s rather than a loud failure.
func hostCfgDir(key string) string {
	return "/opt/bench/cfg/" + key
}

// stageCfgPrefix is the S3 key prefix (under the results bucket) that one
// point's config(s) are uploaded to. Mirrors hostCfgDir on the S3 side; see
// its comment for why this is factored out rather than rebuilt per call site.
func stageCfgPrefix(key string) string {
	return "stage/cfg/" + key
}

// runnerConfigPaths maps each point key to where its configs land on the runner
// host after staging.
func runnerConfigPaths(sets []renderedPointConfigs) map[string]pointConfigPaths {
	out := make(map[string]pointConfigPaths, len(sets))
	for _, set := range sets {
		base := hostCfgDir(set.Key)
		var p pointConfigPaths
		if set.Single != "" {
			p.Single = base + "/config.yaml"
		} else {
			p.Root = base + "/root.yaml"
			p.Dir = base + "/streams"
		}
		out[set.Key] = p
	}
	return out
}

// buildKCRenderInputs gathers the values needed to render a Kafka Connect
// connector config from a scenario and the terraform outputs. Postgres engines
// expose a DSN URL output; MySQL exposes discrete host/port/user/pass/db
// outputs — we handle both via engineSpec metadata.
func buildKCRenderInputs(s *Scenario, es engineSpec, outs map[string]string, sessionID string) (kcRenderInputs, error) {
	in := kcRenderInputs{
		TopicPrefix:      fmt.Sprintf("bench_%s_%s_kc", sessionID, s.Connector),
		BootstrapServers: outs["redpanda_broker_endpoints"],
	}
	// Tables come from the scenario's pipeline.input map.
	if inputMap, ok := s.Pipeline["input"].(map[string]any); ok {
		for _, v := range inputMap {
			if connMap, ok := v.(map[string]any); ok {
				if tbls, ok := connMap["tables"].([]any); ok {
					for _, t := range tbls {
						if ts, ok := t.(string); ok {
							in.Tables = append(in.Tables, ts)
						}
					}
				}
			}
		}
	}
	// Fallback: connectors that select tables via a non-"tables" field (e.g.
	// oracledb_cdc uses `include`) leave in.Tables empty above. The canonical
	// table list lives on the scenario dataset, so use that.
	if len(in.Tables) == 0 {
		in.Tables = append(in.Tables, s.Dataset.Tables...)
	}

	// Connection parts.
	if es.ResetHostOutputKey != "" {
		// MySQL-style: discrete TF outputs.
		in.Host = outs[es.ResetHostOutputKey]
		in.Port = outs[es.ResetPortOutputKey]
		in.User = outs[es.ResetUserOutputKey]
		in.Password = outs[es.ResetPassOutputKey]
		in.Database = outs[es.ResetDBOutputKey]
	} else {
		// Postgres-style: parse DSN URL.
		dsn := outs[es.DSNOutputKey]
		u, err := url.Parse(dsn)
		if err != nil {
			return in, fmt.Errorf("parse DSN %q: %w", dsn, err)
		}
		in.Host = u.Hostname()
		in.Port = u.Port()
		if in.Port == "" {
			in.Port = "5432"
		}
		if u.User != nil {
			in.User = u.User.Username()
			pw, _ := u.User.Password()
			in.Password = pw
		}
		in.Database = strings.TrimPrefix(u.Path, "/")
	}

	// SchemaTables formatting depends on engine. Must come AFTER in.Database is
	// populated, since the mysql_cdc branch uses it as the schema prefix.
	switch s.Connector {
	case "postgres_cdc":
		schema := "public"
		if inputMap, ok := s.Pipeline["input"].(map[string]any); ok {
			if pgMap, ok := inputMap["postgres_cdc"].(map[string]any); ok {
				if sc, ok := pgMap["schema"].(string); ok {
					schema = sc
				}
			}
		}
		var sb strings.Builder
		for i, t := range in.Tables {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(schema + "." + t)
		}
		in.SchemaTables = sb.String()
	case "mysql_cdc":
		var sb strings.Builder
		for i, t := range in.Tables {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(in.Database + "." + t)
		}
		in.SchemaTables = sb.String()
	case "oracledb_cdc":
		// Debezium Oracle table.include.list is SCHEMA.TABLE, both upper-cased.
		// The owning schema is the connecting user (RDS master, e.g. BENCH).
		schema := strings.ToUpper(in.User)
		parts := make([]string, 0, len(in.Tables))
		for _, t := range in.Tables {
			parts = append(parts, schema+"."+strings.ToUpper(t))
		}
		in.SchemaTables = strings.Join(parts, ",")
	case "mongodb_cdc":
		// Debezium MongoDB collection.include.list is <db>.<collection>. The db is
		// the connecting database (in.Database, from the mongodb_db output).
		parts := make([]string, 0, len(in.Tables))
		for _, t := range in.Tables {
			parts = append(parts, in.Database+"."+t)
		}
		in.SchemaTables = strings.Join(parts, ",")
	}

	return in, nil
}

func substitutePlaceholders(in string, outs map[string]string) string {
	for k, v := range outs {
		in = strings.ReplaceAll(in, "${"+strings.ToUpper(k)+"}", v)
	}
	return in
}

// upload is one S3 object stageArtefacts uploads: key is the S3 key under the
// results bucket, path is the local file to read the body from.
type upload struct{ key, path string }

// buildStagePlan computes the S3 upload keys and the runner-host download
// commands for one bench's config artefacts, without touching AWS. Split out
// from stageArtefacts so the staged S3 key and the host download path can be
// unit-tested for agreement with runnerConfigPaths' launch path (see
// TestBuildStagePlan_AgreesWithRunnerConfigPaths) — previously these were two
// independent constructions of the same "/opt/bench/cfg/<key>" /
// "stage/cfg/<key>" strings, with nothing asserting they stayed in sync.
//
// legacy is len(s.Matrix.Arms) == 0: arm-less runs keep the historical single
// stage/config.yaml -> /opt/bench/config.yaml path so the six existing
// scenarios are untouched. Arm runs stage each point under its own
// stage/cfg/<key>/ prefix, mirroring runnerConfigPaths.
//
// Each point's streams/ directory is cleared before download (rm -rf then
// mkdir -p), not just created: /opt/bench/cfg/<key>/streams is
// session-independent, so a --keep re-run of the same scenario with a
// different stream count would otherwise leave a stale stream-N.yaml behind
// that nothing overwrites — the engine launches one pipeline per file it
// finds there, so a stale file silently launches an extra pipeline the
// sidecar never polls.
func buildStagePlan(sets []renderedPointConfigs, bucket string, legacy bool) (items []upload, dl []string) {
	if legacy {
		items = append(items, upload{"stage/config.yaml", sets[0].Single})
		dl = append(dl, fmt.Sprintf(`aws s3 cp s3://%s/stage/config.yaml /opt/bench/config.yaml`, bucket))
		return items, dl
	}
	for _, set := range sets {
		base := stageCfgPrefix(set.Key)
		host := hostCfgDir(set.Key)
		dl = append(dl, fmt.Sprintf(`rm -rf %s/streams && mkdir -p %s/streams`, host, host))
		if set.Single != "" {
			items = append(items, upload{base + "/config.yaml", set.Single})
			dl = append(dl, fmt.Sprintf(`aws s3 cp s3://%s/%s/config.yaml %s/config.yaml`, bucket, base, host))
			continue
		}
		items = append(items, upload{base + "/root.yaml", set.Root})
		dl = append(dl, fmt.Sprintf(`aws s3 cp s3://%s/%s/root.yaml %s/root.yaml`, bucket, base, host))
		for i, sp := range set.Streams {
			name := fmt.Sprintf("stream-%d.yaml", i)
			items = append(items, upload{fmt.Sprintf("%s/streams/%s", base, name), sp})
			dl = append(dl, fmt.Sprintf(`aws s3 cp s3://%s/%s/streams/%s %s/streams/%s`, bucket, base, name, host, name))
		}
	}
	return items, dl
}

// stageArtefacts uploads the binary, license, and per-point config(s) to S3 and
// downloads them onto the runner host over SSM. See buildStagePlan for the
// legacy vs. arms path-building logic.
func stageArtefacts(ctx context.Context, opts benchOpts, outs map[string]string, binPath string, sets []renderedPointConfigs, legacy bool) error {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return err
	}
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	bucket := outs["results_bucket"]

	items := []upload{
		{"stage/redpanda-connect", binPath},
		{"stage/license.jwt", opts.licenseFile},
	}
	planItems, dl := buildStagePlan(sets, bucket, legacy)
	items = append(items, planItems...)

	for _, item := range items {
		f, err := os.Open(item.path)
		if err != nil {
			// item.path is an ephemeral /tmp/bench-*.yaml name; item.key (e.g.
			// stage/cfg/2-b-2pipe-gmp4/streams/stream-1.yaml) is what identifies
			// which arm/stream failed.
			return fmt.Errorf("open %s for s3 key %s: %w", item.path, item.key, err)
		}
		_, err = uploader.Upload(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &item.key, Body: f})
		f.Close()
		if err != nil {
			return fmt.Errorf("upload %s to %s: %w", item.path, item.key, err)
		}
	}

	ssmExec, err := NewSSMExecutor(ctx, opts.region)
	if err != nil {
		return err
	}
	script := fmt.Sprintf(`
set -euo pipefail
aws s3 cp s3://%s/stage/redpanda-connect /opt/bench/redpanda-connect
aws s3 cp s3://%s/stage/license.jwt /opt/bench/license.jwt
%s
chmod +x /opt/bench/redpanda-connect
chmod 0600 /opt/bench/license.jwt
aws s3 cp s3://%s/stage/iceberg-tablegen /opt/bench/iceberg-tablegen 2>/dev/null && chmod +x /opt/bench/iceberg-tablegen || true
`, bucket, bucket, strings.Join(dl, "\n"), bucket)
	return ssmExec.Run(ctx, outs["runner_instance_id"], script, streamingOnLine(os.Stdout, "stage"))
}

// stageTableGenForSink builds the iceberg-tablegen binary and uploads it to
// s3://<bucket>/stage/iceberg-tablegen for sink scenarios. The runner downloads
// it in stageArtefacts; sinkTopology.ResetScript invokes it to pre-create tables.
func stageTableGenForSink(ctx context.Context, opts benchOpts, s *Scenario, outs map[string]string) error {
	if s.Direction != DirectionSink {
		return nil
	}
	dist := filepath.Join(opts.repoRoot, "benchmarking/aws/seeders/dist")
	_ = os.MkdirAll(dist, 0o755)
	binOut := filepath.Join(dist, "iceberg-tablegen")
	cmd := exec.Command("go", "build", "-o", binOut, "./benchmarking/aws/seeders/iceberg-tablegen")
	cmd.Dir = opts.repoRoot
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build iceberg-tablegen: %w", err)
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return err
	}
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	bucket := outs["results_bucket"]
	f, err := os.Open(binOut)
	if err != nil {
		return err
	}
	defer f.Close()
	key := "stage/iceberg-tablegen"
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: f})
	return err
}

func runSeeder(ctx context.Context, opts benchOpts, s *Scenario, outs map[string]string, topo Topology, names BenchNames) error {
	if s.Dataset.Seeder == "" {
		return nil
	}
	dist := filepath.Join(opts.repoRoot, "benchmarking/aws/seeders/dist")
	_ = os.MkdirAll(dist, 0o755)
	binOut := filepath.Join(dist, s.Dataset.Seeder)
	cmd := exec.Command("go", "build", "-o", binOut, "./benchmarking/aws/seeders/"+s.Dataset.Seeder)
	cmd.Dir = opts.repoRoot
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build seeder: %w", err)
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return err
	}
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	bucket := outs["results_bucket"]
	f, err := os.Open(binOut)
	if err != nil {
		return err
	}
	defer f.Close()
	key := "stage/" + s.Dataset.Seeder
	if _, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: f,
	}); err != nil {
		return err
	}
	ssmExec, err := NewSSMExecutor(ctx, opts.region)
	if err != nil {
		return err
	}
	script, err := topo.SeedScript(s, outs, names)
	if err != nil {
		return err
	}
	return ssmExec.Run(ctx, outs["load_gen_instance_id"], script, streamingOnLine(os.Stdout, "seed"))
}
