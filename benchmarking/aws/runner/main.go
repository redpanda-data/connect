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

	binPath, err := buildConnect(opts.repoRoot)
	if err != nil {
		return fmt.Errorf("build connect: %w", err)
	}
	fmt.Println("[3/7] built redpanda-connect")

	cfgPath, err := renderPipelineConfig(s, sharedOuts, topo, names)
	if err != nil {
		return fmt.Errorf("render pipeline config: %w", err)
	}
	if err := stageArtefacts(ctx, opts, sharedOuts, binPath, cfgPath); err != nil {
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
		es, ok := engineSpecFor(s.Connector)
		if !ok {
			return fmt.Errorf("no engineSpec for %q", s.Connector)
		}
		in, err := buildKCRenderInputs(s, es, sharedOuts, sessionID)
		if err != nil {
			return fmt.Errorf("build KC render inputs: %w", err)
		}
		cfg, err := renderKCConfig(s, in)
		if err != nil {
			return fmt.Errorf("render KC config: %w", err)
		}
		raw, err := json.Marshal(cfg)
		if err != nil {
			return err
		}
		kcConnectorName = fmt.Sprintf("bench_%s", s.Connector)
		kcConfigJSON = string(raw)
	}

	mr := &MatrixRunner{
		SSM:                      ssmExec,
		LogFetcher:               logFetcher,
		RunnerInstance:           sharedOuts["runner_instance_id"],
		LoadGenInstance:          sharedOuts["load_gen_instance_id"],
		ConfigPath:               "/opt/bench/config.yaml",
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
	}
	reset, err := topo.ResetScript(s, sharedOuts, names)
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
	points, err := mr.Run(ctx, s.Matrix.CPUPoints, s.Matrix.GoMemLimitPerVCPU, warmup, duration, reset, workload)
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

func renderPipelineConfig(s *Scenario, outs map[string]string, topo Topology, names BenchNames) (string, error) {
	input, output, err := topo.Pipeline(s, names)
	if err != nil {
		return "", fmt.Errorf("render pipeline: %w", err)
	}
	cfg := map[string]any{
		"http": map[string]any{"debug_endpoints": true},
		"redpanda": map[string]any{
			"seed_brokers": []string{"${REDPANDA_BROKER_ENDPOINTS}"},
		},
		"input":  input,
		"output": output,
		"logger": map[string]any{"level": "INFO"},
		"metrics": map[string]any{
			"prometheus": map[string]any{"add_process_metrics": true, "add_go_metrics": true},
		},
	}
	// Connectors that require a persistent checkpoint (e.g. mysql_cdc) declare
	// cache_resources in the scenario's pipeline block. Thread them through
	// to the Connect config root when present.
	if cr, ok := s.Pipeline["cache_resources"]; ok {
		cfg["cache_resources"] = cr
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return "", err
	}
	rendered := substitutePlaceholders(string(raw), outs)
	tmp, err := os.CreateTemp("", "bench-config-*.yaml")
	if err != nil {
		return "", err
	}
	defer tmp.Close()
	if _, err := tmp.WriteString(rendered); err != nil {
		return "", err
	}
	return tmp.Name(), nil
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
	}

	return in, nil
}

func substitutePlaceholders(in string, outs map[string]string) string {
	for k, v := range outs {
		in = strings.ReplaceAll(in, "${"+strings.ToUpper(k)+"}", v)
	}
	return in
}

func stageArtefacts(ctx context.Context, opts benchOpts, outs map[string]string, binPath, cfgPath string) error {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return err
	}
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	bucket := outs["results_bucket"]
	for _, item := range []struct{ key, path string }{
		{"stage/redpanda-connect", binPath},
		{"stage/config.yaml", cfgPath},
		{"stage/license.jwt", opts.licenseFile},
	} {
		f, err := os.Open(item.path)
		if err != nil {
			return err
		}
		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &item.key,
			Body:   f,
		})
		f.Close()
		if err != nil {
			return fmt.Errorf("upload %s: %w", item.path, err)
		}
	}
	ssmExec, err := NewSSMExecutor(ctx, opts.region)
	if err != nil {
		return err
	}
	script := fmt.Sprintf(`
set -euo pipefail
aws s3 cp s3://%s/stage/redpanda-connect /opt/bench/redpanda-connect
aws s3 cp s3://%s/stage/config.yaml /opt/bench/config.yaml
aws s3 cp s3://%s/stage/license.jwt /opt/bench/license.jwt
chmod +x /opt/bench/redpanda-connect
chmod 0600 /opt/bench/license.jwt
`, bucket, bucket, bucket)
	return ssmExec.Run(ctx, outs["runner_instance_id"], script, streamingOnLine(os.Stdout, "stage"))
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
