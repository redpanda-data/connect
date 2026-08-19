// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"gopkg.in/yaml.v3"
)

func main() {
	// Operators run the bench as `runner bench ... | tee run.log`. Ctrl-C
	// SIGINTs the whole foreground process group, so tee dies alongside us —
	// and without this, Go's default SIGPIPE disposition kills the process on
	// its next stdout write, ABORTING the deferred terraform destroy and
	// stranding paid infrastructure (observed live 2026-08-12, twice).
	// Ignoring SIGPIPE turns those writes into silently-dropped EPIPE errors
	// so teardown completes even with nowhere to print.
	signal.Ignore(syscall.SIGPIPE)

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
	// licenseSecret is the AWS Secrets Manager secret name or ARN to fall
	// back to when licenseFile is unset or doesn't open — see
	// resolveLicensePath. Scheduled runs (GitHub Actions) have no license
	// file on disk, so this is how they get one.
	licenseSecret string
	// soakArchiveBucket is the persistent S3 bucket a soak run's result and
	// raw artifacts are copied to — see uploadSoakResult. Unlike the
	// session results bucket, this one is NOT force_destroy'd at teardown.
	soakArchiveBucket string
	// preflightOn gates the concurrent-bench-session guard (see
	// preflightCheck). Defaults to true; --preflight=off is the emergency
	// escape hatch for an operator who is certain no other session is live.
	preflightOn bool
	// binaries maps a logical binary name (matrix.arms[].binary) to a local
	// path a PR-triggered workflow already built (e.g. "base"/"pr" for a
	// merge-base vs. PR-head soak comparison — see CON-179 R6 increment 5).
	// nil/empty is the default single-binary path: runBench builds
	// redpanda-connect itself, unchanged from before this field existed.
	binaries map[string]string
}

// defaultSoakArchiveBucket is the persistent soak archive bucket's name,
// created by the persistent terraform stack (`task aws:persistent`) — not
// by any bench session's own apply, and not force_destroy'd alongside it.
const defaultSoakArchiveBucket = "redpanda-connect-bench-soak-archive"

func benchCmd(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	scenario := fs.String("scenario", "", "path to scenario YAML (e.g. scenarios/postgres/orders-cdc.yaml)")
	keep := fs.Bool("keep", false, "don't tear down infra after the run")
	keepOnFail := fs.Bool("keep-on-fail", false, "keep infra if the bench errors")
	region := fs.String("region", "us-east-2", "AWS region")
	repoRoot := fs.String("repo-root", ".", "path to the connect repo root")
	licenseFile := fs.String("license-file", os.Getenv("REDPANDA_LICENSE_FILEPATH"),
		"path to a Redpanda Enterprise license file (defaults to $REDPANDA_LICENSE_FILEPATH). "+
			"Required for enterprise connectors like postgres_cdc, unless --license-secret is set.")
	licenseSecret := fs.String("license-secret", os.Getenv("REDPANDA_LICENSE_SECRET"),
		"name or ARN of an AWS Secrets Manager secret whose SecretString is a Redpanda Enterprise "+
			"license (defaults to $REDPANDA_LICENSE_SECRET). Used when --license-file is unset or "+
			"doesn't open — the case for scheduled runs with no license file on disk.")
	soakArchiveBucket := fs.String("soak-archive-bucket", defaultSoakArchiveBucket,
		"S3 bucket a soak run's result.json + raw artifacts are archived to, since the session results "+
			"bucket is force_destroy'd at teardown. Created by the persistent terraform stack "+
			"(`task aws:persistent`).")
	preflight := &preflightFlag{on: true}
	fs.Var(preflight, "preflight",
		`"on" (default) or "off": guard against a concurrent bench session already holding the shared `+
			`Terraform stack. "off" is an emergency escape hatch only — concurrent sessions destroy `+
			`each other's infrastructure.`)
	binaries := &binaryFlag{}
	fs.Var(binaries, "binary",
		`Repeatable "name=path" mapping a logical binary (matrix.arms[].binary, e.g. "base" or "pr") to a `+
			`pre-built redpanda-connect binary on disk. Used by a PR-triggered soak comparison, which builds `+
			`the merge-base and PR-head binaries itself and passes both in rather than letting the runner `+
			`build one. Every name a scenario's arms reference must be mapped, and every mapping must be `+
			`referenced — an unreferenced mapping is very likely a typo.`)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *scenario == "" {
		return fmt.Errorf("--scenario is required")
	}

	opts := benchOpts{
		scenarioPath:      *scenario,
		keep:              *keep,
		keepOnFail:        *keepOnFail,
		region:            *region,
		repoRoot:          *repoRoot,
		licenseFile:       *licenseFile,
		licenseSecret:     *licenseSecret,
		soakArchiveBucket: *soakArchiveBucket,
		preflightOn:       preflight.on,
		binaries:          binaries.m,
	}
	return runBench(opts)
}

// preflightFlag is a flag.Value accepting "on"/"off" rather than Go's
// default bool string parsing, so --preflight=off reads as the deliberate
// emergency escape hatch it is instead of an easily-fat-fingered
// --preflight=false.
type preflightFlag struct {
	on bool
}

func (f *preflightFlag) String() string {
	if f.on {
		return "on"
	}
	return "off"
}

func (f *preflightFlag) Set(s string) error {
	switch strings.ToLower(s) {
	case "on":
		f.on = true
	case "off":
		f.on = false
	default:
		return fmt.Errorf(`invalid --preflight value %q: must be "on" or "off"`, s)
	}
	return nil
}

// binaryFlag is a flag.Value that accumulates repeated `--binary name=path`
// flags into a map, since flag.FlagSet has no built-in repeatable-flag type.
type binaryFlag struct {
	m map[string]string
}

func (f *binaryFlag) String() string {
	if len(f.m) == 0 {
		return ""
	}
	parts := make([]string, 0, len(f.m))
	for name, path := range f.m {
		parts = append(parts, name+"="+path)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func (f *binaryFlag) Set(s string) error {
	name, path, ok := strings.Cut(s, "=")
	if !ok || name == "" || path == "" {
		return fmt.Errorf(`invalid --binary value %q: must be "name=path"`, s)
	}
	if f.m == nil {
		f.m = map[string]string{}
	}
	if _, exists := f.m[name]; exists {
		return fmt.Errorf("--binary %q specified more than once", name)
	}
	f.m[name] = path
	return nil
}

func runBench(opts benchOpts) (errOut error) {
	s, err := LoadScenario(opts.scenarioPath)
	if err != nil {
		return err
	}
	fmt.Printf("[1/7] loaded scenario %s\n", s.Name)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Every bench session applies and later DESTROYS the same shared
	// Terraform stack, so two concurrent sessions destroy each other's
	// infrastructure mid-run (observed live 2026-08-17: a laptop bench and
	// the scheduled soak collided). Checked before any AWS call this run
	// makes — including license resolution just below — so the conflict is
	// caught before this session touches anything.
	if opts.preflightOn {
		ec2Client, err := NewEC2Client(ctx, opts.region)
		if err != nil {
			return fmt.Errorf("build EC2 client for preflight check: %w", err)
		}
		if err := preflightCheck(ctx, ec2Client); err != nil {
			return err
		}
		fmt.Println("preflight: no concurrent bench session detected")
	} else {
		fmt.Println("⚠ preflight check DISABLED (--preflight=off): a concurrent bench session will destroy this one's infrastructure (or vice versa) with no warning")
	}

	// Resolve the license BEFORE any AWS infrastructure is provisioned, same
	// reasoning as the old file-open check this replaces: surface a bad
	// license source immediately rather than minutes into a paid run.
	// resolveLicensePath tries --license-file first (unchanged local-operator
	// workflow) and falls back to --license-secret (scheduled runs, which
	// have no license file on disk).
	licensePath, cleanupLicense, err := resolveLicensePath(ctx, opts, NewSecretsManagerClient)
	if err != nil {
		return err
	}
	defer cleanupLicense()
	opts.licenseFile = licensePath

	// --binary mappings must cover exactly the logical binaries the
	// scenario's arms reference — no more, no less — before any AWS spend,
	// same reasoning as the checks above.
	if err := validateBinaryFlags(s, opts.binaries); err != nil {
		return err
	}

	warmup, duration := sweepWarmupDuration(s)
	// execTimeout bounds every script this session's SSM executor runs — the
	// staging/seed commands as well as the sweep itself — at the AWS-
	// RunShellScript document level (see NewSSMExecutor). Sized off the
	// sweep's own warmup+duration plus slack rather than a scenario-specific
	// value, since the same executor instance serves every command in the
	// run and a generous cap is harmless for the short ones.
	execTimeout := warmup + duration + execTimeoutSlack

	// A soak point runs far longer than a sweep point, so the fixed cadences
	// a short run tolerates would otherwise silently corrupt the run: the
	// per-minute heartbeat would overflow SSM's ~24KB stdout cap over many
	// hours, the 10s Prometheus scrape would accumulate gigabytes on disk and
	// in S3, and a mid-run crash would lose the entire window's data. See
	// matrix.go's benchScriptArgs for how these three feed the rendered
	// script.
	var heartbeatSec, promScrapeSec, checkpointSec int
	var expectedRecordsPerSec float64
	if s.Soak {
		totalSec := int((warmup + duration).Seconds())
		heartbeatSec = max(soakDefaultHeartbeatSec, totalSec/soakMaxHeartbeats)
		promScrapeSec = soakPromScrapeSec
		checkpointSec = soakCheckpointSec
		if s.Workload != nil {
			expectedRecordsPerSec = float64(s.Workload.WriteRatePerSec)
		}
		fmt.Printf("soak profile: window %s, heartbeat every %ds, prom scrape every %ds, checkpoint upload every %ds\n",
			warmup+duration, heartbeatSec, promScrapeSec, checkpointSec)
	}

	topo, err := topologyFor(s.Direction)
	if err != nil {
		return err
	}

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

	// soakRegressionOnly is set just before runBench's final return, when the
	// only reason errOut is non-nil is the rolling-baseline comparator (see
	// compareSoakRunToBaseline) — never for any earlier, genuine failure.
	// The teardown defer below reads it to distinguish the two: a
	// regression is a verdict about a run that itself SUCCEEDED (its full
	// result is already archived in S3 by the time the comparator even
	// runs), so unlike every other error it must never be treated as a
	// reason to honor --keep-on-fail and strand infrastructure.
	var soakRegressionOnly bool

	// Register destroy BEFORE any apply, so a partial apply still gets torn
	// down. terraform destroy is idempotent against a no-op state.
	defer func() {
		if opts.keep {
			fmt.Println("[7/7] keep=true: skipping teardown")
			return
		}
		if errOut != nil && opts.keepOnFail && !soakRegressionOnly {
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
	// it here so per-engine renderers (renderPipelineConfig, combineReset) can
	// read it via outs["bench_session_id"].
	sharedOuts["bench_session_id"] = sessionID
	// aws_region is data the runner already holds (not a TF output).
	sharedOuts["aws_region"] = opts.region

	// --binary mappings supply pre-built binaries (a PR-triggered soak
	// comparison builds merge-base and PR-head itself), so there is no
	// default binary to build in that case — validateBinaryFlags already
	// confirmed every arm that needs one has a mapping.
	var binPath string
	if len(opts.binaries) == 0 {
		binPath, err = buildConnect(opts.repoRoot)
		if err != nil {
			return fmt.Errorf("build connect: %w", err)
		}
		fmt.Println("[3/7] built redpanda-connect")
	} else {
		fmt.Printf("[3/7] skipping default build: staging provided --binary mappings %v\n", sortedBinaryNames(opts.binaries))
	}

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
	if err := stageArtefacts(ctx, opts, sharedOuts, binPath, sets, legacy, execTimeout); err != nil {
		return fmt.Errorf("stage artefacts: %w", err)
	}
	fmt.Println("[4/7] staged binary + config on runner")

	if err := runSeeder(ctx, opts, s, sharedOuts, topo, names, execTimeout); err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	fmt.Println("[5/7] seed complete")

	ssmExec, err := NewSSMExecutor(ctx, opts.region, execTimeout)
	if err != nil {
		return err
	}
	logFetcher, err := NewS3LogFetcher(ctx, opts.region)
	if err != nil {
		return err
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
		Topology:                 topo,
		Names:                    names,
		Outs:                     sharedOuts,
		HeartbeatSec:             heartbeatSec,
		PromScrapeSec:            promScrapeSec,
		CheckpointSec:            checkpointSec,
		ExpectedRecordsPerSec:    expectedRecordsPerSec,
	}
	// A soak run publishes per-minute CloudWatch metrics as it goes, so an
	// operator (or an alarm) can watch a run that may still have 23 hours
	// left, instead of only finding out something went wrong when the
	// result JSON lands at the end. Deliberately orchestrator-side (see
	// MatrixRunner.Emitter): the runner EC2 instance role is never given
	// cloudwatch:PutMetricData, so a future PR-mode binary running under
	// that role cannot spoof the metrics that judge it.
	if s.Soak {
		emitter, err := NewCloudWatchEmitter(ctx, opts.region, CloudWatchNamespace, s.Connector, s.Name)
		if err != nil {
			return fmt.Errorf("build CloudWatch emitter: %w", err)
		}
		mr.Emitter = emitter
		fmt.Printf("soak profile: publishing metrics to CloudWatch namespace %q, dimensions Connector=%q Scenario=%q\n",
			CloudWatchNamespace, s.Connector, s.Name)
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
			Binary:       p.Binary,
			GOMAXPROCS:   p.GOMAXPROCS,
			Streams:      p.Streams,
			Backlog:      p.Backlog,
		})
	}
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
	// A soak run's result also lands in the PERSISTENT soak archive bucket:
	// the full JSON at a session-scoped key (so it's fetchable without a
	// checkout of this repo), a small index line under soak-index/<scenario>/
	// that the rolling-baseline comparator just below lists without
	// downloading every run's full JSON, and copies of the raw per-point
	// artifacts (sweep log, Prometheus dump, broker scrape) the bench script
	// already wrote to the session results bucket — which is
	// force_destroy'd by the teardown defer just below, minutes from now.
	// The upload itself is non-fatal — a bench run that produced a valid
	// local result must not fail because of an S3 hiccup on the archive
	// upload — but a REGRESSION the comparator finds against a mature
	// baseline, after a successful upload, is not: see soakRegressionErr
	// below.
	//
	// A binary-arm soak (base vs. PR, see IsBinaryArmScenario) is a
	// deliberate one-off A/B, not a nightly baseline sample: uploadSoakResult
	// skips its soak-index entry so it never pollutes the rolling baseline's
	// median, and the rolling-baseline comparator itself is skipped in favor
	// of the two-run comparison markdown below.
	var soakRegressionErr error
	if s.Soak {
		entry, err := uploadSoakResult(ctx, opts.region, sharedOuts["results_bucket"], opts.soakArchiveBucket, sessionID, s, result, jsonPath, topo, logFetcher)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: archive soak result to S3: %v\n", err)
		} else if !s.IsBinaryArmScenario() {
			soakRegressionErr = compareSoakRunToBaseline(ctx, opts, s, sessionID, entry, logFetcher)
		}
		if s.IsBinaryArmScenario() {
			if err := reportSoakComparison(ctx, opts, s, sessionID, result); err != nil {
				fmt.Fprintf(os.Stderr, "warning: soak comparison (non-fatal): %v\n", err)
			}
		}
	}
	fmt.Printf("\n✓ done — JSON: %s\n           md: %s\n           summary: %s\n", jsonPath, mdPath, summaryPath)
	// soakRegressionErr is returned LAST, only after every step above
	// (including the archive upload) has already run. soakRegressionOnly is
	// set here, immediately before the return the deferred teardown above
	// observes — see its own comment for why a regression must never be
	// treated like an ordinary failure for --keep-on-fail purposes.
	if soakRegressionErr != nil {
		soakRegressionOnly = true
	}
	return soakRegressionErr
}

// soakIndexEntry is the small per-run record a rolling-baseline comparator
// (a later increment) can list under soak-index/<scenario>/ without having
// to download every run's full result JSON first.
type soakIndexEntry struct {
	Scenario      string    `json:"scenario"`
	Connector     string    `json:"connector"`
	SessionID     string    `json:"session_id"`
	StartedAt     time.Time `json:"started_at"`
	MedianMBps    float64   `json:"median_mbps"`
	P5MBps        float64   `json:"p5"`
	P95MBps       float64   `json:"p95"`
	RSSMaxBytes   uint64    `json:"rss_max_bytes"`
	BacklogMaxSec float64   `json:"backlog_max_sec"`
	// BuildSHA is the git commit the soaked binary was built from. The
	// nightly workflow's change gate diffs HEAD against the last soaked SHA
	// to skip runs when nothing relevant merged. Empty when git metadata is
	// unavailable (gate then runs the soak — fail open, toward coverage).
	BuildSHA string `json:"build_sha,omitempty"`
}

// soakArchivePlan is the set of S3 keys one soak run's archive upload
// writes (ResultKey, IndexKey) and copies (RawKeys) — computed without
// touching AWS so the destination layout can be unit-tested (see
// TestBuildSoakArchivePlan) independently of any S3 call. RawKeys use the
// IDENTICAL key in both the session results bucket (source — the bench
// script wrote them there) and the archive bucket (destination), so the
// raw evidence lands at exactly the path its S3 key already implies.
type soakArchivePlan struct {
	ResultKey string
	IndexKey  string
	RawKeys   []string
}

// soakRawArtifactKeys returns the raw per-point artifact keys (sweep log,
// Prometheus dump, and — when a Topology supplied one — the broker scrape)
// for ONE measured point, under runs/<sessionID>/. Split out of
// buildSoakArchivePlan so uploadSoakResult can call it once per point of a
// binary-arm soak's multiple measured points (one per arm), not just the
// single key buildSoakArchivePlan itself was designed around.
func soakRawArtifactKeys(sessionID, key, brokerArtifact string) []string {
	if key == "" {
		return nil
	}
	keys := []string{
		fmt.Sprintf("runs/%s/sweep-%s.log", sessionID, key),
		fmt.Sprintf("runs/%s/prom-%s.txt", sessionID, key),
	}
	if brokerArtifact != "" {
		keys = append(keys, fmt.Sprintf("runs/%s/%s", sessionID, brokerArtifact))
	}
	return keys
}

// buildSoakArchivePlan computes soakArchivePlan for one soak run. key is the
// sweepPoint key (see sweepPoint.Key) of the run's single measured point;
// brokerArtifact is Topology.MetricArtifact(key), or "" when no Topology was
// available to compute it (that raw file is then simply skipped, same as
// key == "").
func buildSoakArchivePlan(sessionID, scenarioName, key, brokerArtifact string) soakArchivePlan {
	return soakArchivePlan{
		ResultKey: fmt.Sprintf("runs/%s/result.json", sessionID),
		IndexKey:  fmt.Sprintf("soak-index/%s/%s.json", scenarioName, sessionID),
		RawKeys:   soakRawArtifactKeys(sessionID, key, brokerArtifact),
	}
}

// buildSoakArchivePlanForPoints extends buildSoakArchivePlan to a soak run's
// FULL result.Points rather than just its single measured point: EVERY
// point contributes its own raw artifact keys, not just the first. A
// binary-arm soak (see Scenario.IsBinaryArmScenario) measures multiple
// points in one session — one per arm — and each arm's raw evidence (sweep
// log, Prometheus dump, broker scrape) must be archived, or the PR arm's
// half of the comparison would be silently missing from the archive.
func buildSoakArchivePlanForPoints(sessionID, scenarioName string, points []PointResult, topo Topology) soakArchivePlan {
	plan := soakArchivePlan{
		ResultKey: fmt.Sprintf("runs/%s/result.json", sessionID),
		IndexKey:  fmt.Sprintf("soak-index/%s/%s.json", scenarioName, sessionID),
	}
	for _, p := range points {
		key := sweepPoint{VCPU: p.VCPU, ArmID: p.Arm}.Key()
		var brokerArtifact string
		if topo != nil {
			brokerArtifact = topo.MetricArtifact(key)
		}
		plan.RawKeys = append(plan.RawKeys, soakRawArtifactKeys(sessionID, key, brokerArtifact)...)
	}
	return plan
}

// uploadSoakResult archives a soak run's full result JSON, a soakIndexEntry
// summary, and its raw per-point artifacts (sweep log, Prometheus dump,
// broker scrape) to archiveBucket — the PERSISTENT bucket the terraform
// persistent stack creates (`task aws:persistent`), not the session results
// bucket (sessionBucket), which is force_destroy'd at teardown minutes
// after this returns. sessionBucket + logFetcher are read-only here: they
// are where the bench script already wrote the raw artifacts this function
// copies onward.
//
// The soakIndexEntry is returned even on error (see buildSoakIndexEntry) so
// a caller that only needs the entry for comparison, not the upload
// itself, isn't forced to rebuild it after a failure. The raw-artifact copy
// is non-fatal per file: a missing or unfetchable artifact is logged and
// skipped, so one gap never costs the run its result.json / soak-index
// entry.
//
// A binary-arm soak (see Scenario.IsBinaryArmScenario) measures MULTIPLE
// points in one session — one per arm — unlike the single-point soak this
// function was originally built for: every point's raw artifacts are
// archived (not just the first), and the soak-index entry is skipped
// entirely, since a one-off base-vs-PR A/B must never be mixed into the
// rolling baseline's median (see compareSoakBaseline).
func uploadSoakResult(ctx context.Context, region, sessionBucket, archiveBucket, sessionID string, s *Scenario, result *Result, jsonPath string, topo Topology, logFetcher LogFetcher) (soakIndexEntry, error) {
	entry := buildSoakIndexEntry(sessionID, s, result)
	entry.BuildSHA = gitHeadSHA()
	if archiveBucket == "" {
		return entry, fmt.Errorf("no soak archive bucket configured (--soak-archive-bucket)")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return entry, err
	}
	client := s3.NewFromConfig(cfg)

	plan := buildSoakArchivePlanForPoints(sessionID, s.Name, result.Points, topo)

	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		return entry, fmt.Errorf("read local result JSON %s: %w", jsonPath, err)
	}
	if err := putSoakArchiveObject(ctx, client, archiveBucket, plan.ResultKey, raw); err != nil {
		return entry, err
	}

	// A binary-arm soak's entry must never land in soak-index/ — see the
	// doc comment above.
	if !s.IsBinaryArmScenario() {
		indexRaw, err := json.Marshal(entry)
		if err != nil {
			return entry, fmt.Errorf("marshal soak index entry: %w", err)
		}
		if err := putSoakArchiveObject(ctx, client, archiveBucket, plan.IndexKey, indexRaw); err != nil {
			return entry, err
		}
	}

	if sessionBucket == "" || logFetcher == nil {
		return entry, nil
	}
	for _, rawKey := range plan.RawKeys {
		if err := copySoakArtifact(ctx, logFetcher, client, sessionBucket, archiveBucket, rawKey); err != nil {
			fmt.Fprintf(os.Stderr, "warning: archive raw soak artifact %s (non-fatal): %v\n", rawKey, err)
		}
	}
	return entry, nil
}

// buildSoakIndexEntry computes the small per-run record uploadSoakResult
// archives under soak-index/<scenario>/ from sessionID, the scenario, and
// its Result — pure, so both uploadSoakResult and the rolling-baseline
// comparator's caller (see runBench) can build the SAME entry without a
// round trip through S3.
//
// A soak scenario is validated (see Scenario.Validate) to have exactly one
// cpu_points entry and engines=connect, so result.Points has exactly one
// element in the success path — but this degrades to zero values rather
// than panicking if that ever changes.
// gitHeadSHA returns the working tree's HEAD commit, or "" when git is
// unavailable — the change gate treats "" as "always run".
func gitHeadSHA() string {
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func buildSoakIndexEntry(sessionID string, s *Scenario, result *Result) soakIndexEntry {
	entry := soakIndexEntry{
		Scenario:  s.Name,
		Connector: s.Connector,
		SessionID: sessionID,
		StartedAt: result.StartedAt,
	}
	if len(result.Points) == 0 {
		return entry
	}
	p := result.Points[0]
	entry.MedianMBps = p.Summary.MedianMBPerSec
	entry.P5MBps = p.Summary.P5MBPerSec
	entry.P95MBps = p.Summary.P95MBPerSec
	for _, pp := range p.Prom {
		if pp.RSSBytes > entry.RSSMaxBytes {
			entry.RSSMaxBytes = pp.RSSBytes
		}
	}
	for _, b := range p.Backlog {
		if b.BacklogSec > entry.BacklogMaxSec {
			entry.BacklogMaxSec = b.BacklogSec
		}
	}
	return entry
}

// putSoakArchiveObject uploads body to key in the soak archive bucket,
// rewriting a missing-bucket error into an actionable one via
// wrapSoakArchiveUploadErr.
func putSoakArchiveObject(ctx context.Context, client *s3.Client, bucket, key string, body []byte) error {
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(body),
	}); err != nil {
		return wrapSoakArchiveUploadErr(key, bucket, err)
	}
	return nil
}

// wrapSoakArchiveUploadErr adds an actionable hint to a soak-archive upload
// error when it's specifically a missing bucket: that bucket is created by
// the persistent terraform stack, not by any bench session's own apply, so
// a fresh account (or one where the persistent stack hasn't been applied
// yet) hits this on its very first soak run, and "NoSuchBucket" alone gives
// no clue what to do about it. Split out from putSoakArchiveObject so the
// message can be pinned in a test without a real S3 client (see
// TestWrapSoakArchiveUploadErr_NoSuchBucket).
func wrapSoakArchiveUploadErr(key, bucket string, err error) error {
	var nsb *s3types.NoSuchBucket
	if errors.As(err, &nsb) {
		return fmt.Errorf("upload %s to soak archive bucket %q: bucket does not exist — run `task aws:persistent` to create it: %w", key, bucket, err)
	}
	return fmt.Errorf("upload %s to soak archive bucket %q: %w", key, bucket, err)
}

// copySoakArtifact streams one raw per-point artifact from the session
// results bucket (where the bench script wrote it) into the archive
// bucket, under the identical key.
func copySoakArtifact(ctx context.Context, fetcher LogFetcher, client *s3.Client, sessionBucket, archiveBucket, key string) error {
	body, err := fetcher.Fetch(ctx, sessionBucket, key)
	if err != nil {
		return fmt.Errorf("fetch from session bucket %q: %w", sessionBucket, err)
	}
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	return putSoakArchiveObject(ctx, client, archiveBucket, key, raw)
}

// soakComparisonBaseArm and soakComparisonPRArm are the fixed logical arm
// ids reportSoakComparison compares. A PR-triggered soak workflow always
// supplies exactly these two --binary mappings (see the binary-arm shape
// Scenario.Validate enforces); a scenario using different arm ids simply
// has no base/pr pair to compare, so BuildSoakComparisonMarkdown's own
// "arm not found" error becomes a skip below rather than a failure.
const (
	soakComparisonBaseArm = "base"
	soakComparisonPRArm   = "pr"
)

// reportSoakComparison builds the base-vs-PR markdown for a binary-arm soak
// (see Scenario.IsBinaryArmScenario), prints it to stdout between fixed
// delimiters a calling GitHub Actions workflow greps between to post it as
// a PR comment, and uploads it alongside the run's other soak archive
// artifacts. Non-fatal by design at the call site (runBench): a soak run
// that measured successfully must not fail because this reporting step
// couldn't reach S3.
func reportSoakComparison(ctx context.Context, opts benchOpts, s *Scenario, sessionID string, result *Result) error {
	md, err := BuildSoakComparisonMarkdown(s.Name, result.Points, soakComparisonBaseArm, soakComparisonPRArm)
	if err != nil {
		fmt.Printf("soak comparison: %v (skipping)\n", err)
		return nil
	}
	fmt.Println("---SOAK-COMPARISON-BEGIN---")
	fmt.Println(md)
	fmt.Println("---SOAK-COMPARISON-END---")

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return fmt.Errorf("load AWS config for soak comparison upload: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	key := fmt.Sprintf("runs/%s/comparison.md", sessionID)
	return putSoakArchiveObject(ctx, client, opts.soakArchiveBucket, key, []byte(md))
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
	warmup, duration := sweepWarmupDuration(s)
	return time.Duration(points) * (warmup + duration)
}

// sweepWarmupDuration returns the scenario's per-point warmup and duration,
// applying the workload-less bounded-dataset default (0 warmup, minDuration —
// see Scenario.Validate) so every caller sizing a wall-clock estimate
// (execTimeout, totalDuration) uses the same fallback.
func sweepWarmupDuration(s *Scenario) (warmup, duration time.Duration) {
	if s.Workload == nil {
		return 0, minDuration
	}
	return s.Workload.Warmup, s.Workload.Duration
}

const (
	// execTimeoutSlack is added atop the sweep's warmup+duration when sizing
	// the session's shared SSM execution timeout (see NewSSMExecutor), so
	// scheduling jitter — terraform output propagation, workload startup,
	// the staging/seed commands that run before the sweep on the SAME
	// executor — never trips the SSM agent's executionTimeout right at the
	// wall-clock the sweep itself is expected to take.
	execTimeoutSlack = 30 * time.Minute

	// soakDefaultHeartbeatSec is the sweep's own heartbeat cadence (60s),
	// used as a floor: a soak run's heartbeat only widens past this, it
	// never narrows below the short-sweep behavior.
	soakDefaultHeartbeatSec = 60
	// soakMaxHeartbeats bounds how many heartbeat lines a soak run emits
	// through SSM stdout (~24KB cap). totalSec/soakMaxHeartbeats gives a
	// cadence that keeps a 24h run to ~60 heartbeat lines regardless of
	// window length.
	soakMaxHeartbeats = 60
	// soakPromScrapeSec is the Connect /metrics scrape cadence for a soak
	// run. Widened from the sweep's 10s default so a 24h run's snapshot
	// file stays in the tens-of-MB range instead of the ~4GB a 10s cadence
	// would accumulate.
	soakPromScrapeSec = 60
	// soakCheckpointSec is how often a soak run uploads its in-progress log
	// and Prometheus snapshot to their FINAL S3 keys, overwriting each time.
	// This is what makes a 24h run's data survive a mid-run crash instead of
	// losing the entire window.
	soakCheckpointSec = 600
)

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

// renderPointConfigs renders the launch config(s) for one sweep point. A
// point with Streams <= 1 gets a single config identical in shape to the
// pre-arms renderer. A multi-stream point gets a root config (observability
// only) plus one stream config per pipeline, each stream distinguished only
// by BenchNames (WithStreams/WithStream, _s<i>).
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

	// topo.Pipeline mutates and returns a map that ALIASES
	// armScenario.Pipeline["output"][<component>] in place — it does not
	// copy it. Each stream's output map must therefore be marshalled to disk
	// (writeTempYAML, below) BEFORE the next iteration calls topo.Pipeline
	// again and re-mutates that same shared map for the next stream. That
	// ordering holds today because the loop is sequential. Do NOT
	// parallelize this loop: two goroutines mutating the same aliased map
	// concurrently would race.
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

// runnerBinaryPath is the on-host path a named --binary mapping is staged to
// and launched from. Mirrors hostCfgDir's reasoning: this is the single
// source of truth so stageArtefacts' download/chmod commands and
// MatrixRunner.binaryPathFor's launch path (matrix.go) can never drift
// apart — a drift there would mean the engine launches a path that was
// never downloaded.
func runnerBinaryPath(name string) string {
	return "/opt/bench/redpanda-connect-" + name
}

// stageBinaryKey is the S3 key prefix (under the results bucket) a named
// --binary mapping is uploaded to. Mirrors runnerBinaryPath on the S3 side.
func stageBinaryKey(name string) string {
	return "stage/redpanda-connect-" + name
}

// sortedBinaryNames returns m's keys sorted, for deterministic logging and
// script rendering — Go's map iteration order is randomized.
func sortedBinaryNames(m map[string]string) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// binaryNamesReferenced returns the set of distinct non-empty Arm.Binary
// values s's matrix.arms reference.
func binaryNamesReferenced(s *Scenario) map[string]bool {
	out := map[string]bool{}
	for _, a := range s.Matrix.Arms {
		if a.Binary != "" {
			out[a.Binary] = true
		}
	}
	return out
}

// validateBinaryFlags checks --binary mappings against the scenario's
// referenced logical binary names BEFORE any AWS spend: every name an arm
// references must have a mapping, and every mapping must be referenced by
// some arm. The second half is a typo guard — e.g. --binary bas=/tmp/x
// against a scenario whose arm says "base" would otherwise silently stage a
// binary nothing launches, while the arm that actually needed "base" fails
// the first check anyway; catching both in one pass gives a single clear
// error instead of two confusing ones.
func validateBinaryFlags(s *Scenario, binaries map[string]string) error {
	referenced := binaryNamesReferenced(s)

	var missing []string
	for name := range referenced {
		if _, ok := binaries[name]; !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		return fmt.Errorf("scenario %s's matrix.arms reference binaries %v with no matching --binary mapping", s.Name, missing)
	}

	var unreferenced []string
	for name := range binaries {
		if !referenced[name] {
			unreferenced = append(unreferenced, name)
		}
	}
	sort.Strings(unreferenced)
	if len(unreferenced) > 0 {
		return fmt.Errorf("--binary mapping(s) %v are not referenced by any matrix.arms[].binary in scenario %s (likely a typo)", unreferenced, s.Name)
	}
	return nil
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

// buildBinaryStagePlan computes the S3 upload items and the runner-host
// download+chmod commands for the launch binary (or binaries), without
// touching AWS — mirrors buildStagePlan's split for the config-staging
// case, so the staged key and the download/launch path can be
// unit-tested for agreement independently of any S3 call.
//
// When binaries is empty, this reproduces the historical single-binary
// shape byte-for-byte: binPath is staged at stage/redpanda-connect and
// downloaded to /opt/bench/redpanda-connect. When --binary mappings are
// present, binPath is ignored (runBench skips building it) and each named
// binary is staged/downloaded under its own stageBinaryKey/runnerBinaryPath
// instead, sorted by name for a deterministic script across runs.
func buildBinaryStagePlan(binaries map[string]string, binPath, bucket string) (items []upload, download, chmod string) {
	if len(binaries) == 0 {
		return []upload{{"stage/redpanda-connect", binPath}},
			fmt.Sprintf(`aws s3 cp s3://%s/stage/redpanda-connect /opt/bench/redpanda-connect`, bucket),
			`chmod +x /opt/bench/redpanda-connect`
	}
	names := sortedBinaryNames(binaries)
	items = make([]upload, 0, len(names))
	dlLines := make([]string, 0, len(names))
	chmodLines := make([]string, 0, len(names))
	for _, name := range names {
		items = append(items, upload{stageBinaryKey(name), binaries[name]})
		dlLines = append(dlLines, fmt.Sprintf(`aws s3 cp s3://%s/%s %s`, bucket, stageBinaryKey(name), runnerBinaryPath(name)))
		chmodLines = append(chmodLines, fmt.Sprintf(`chmod +x %s`, runnerBinaryPath(name)))
	}
	return items, strings.Join(dlLines, "\n"), strings.Join(chmodLines, "\n")
}

// stageArtefacts uploads the binary (or binaries), license, and per-point
// config(s) to S3 and downloads them onto the runner host over SSM. See
// buildStagePlan for the legacy vs. arms config path-building logic and
// buildBinaryStagePlan for the single vs. named-binaries path-building
// logic.
func stageArtefacts(ctx context.Context, opts benchOpts, outs map[string]string, binPath string, sets []renderedPointConfigs, legacy bool, execTimeout time.Duration) error {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.region))
	if err != nil {
		return err
	}
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	bucket := outs["results_bucket"]

	binItems, binDownload, binChmod := buildBinaryStagePlan(opts.binaries, binPath, bucket)
	items := append([]upload{}, binItems...)
	items = append(items, upload{"stage/license.jwt", opts.licenseFile})
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

	ssmExec, err := NewSSMExecutor(ctx, opts.region, execTimeout)
	if err != nil {
		return err
	}
	script := fmt.Sprintf(`
set -euo pipefail
%s
aws s3 cp s3://%s/stage/license.jwt /opt/bench/license.jwt
%s
%s
chmod 0600 /opt/bench/license.jwt
`, binDownload, bucket, strings.Join(dl, "\n"), binChmod)
	return ssmExec.Run(ctx, outs["runner_instance_id"], script, streamingOnLine(os.Stdout, "stage"))
}

func runSeeder(ctx context.Context, opts benchOpts, s *Scenario, outs map[string]string, topo Topology, names BenchNames, execTimeout time.Duration) error {
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
	ssmExec, err := NewSSMExecutor(ctx, opts.region, execTimeout)
	if err != nil {
		return err
	}
	script, err := topo.SeedScript(s, outs, names)
	if err != nil {
		return err
	}
	return ssmExec.Run(ctx, outs["load_gen_instance_id"], script, streamingOnLine(os.Stdout, "seed"))
}
