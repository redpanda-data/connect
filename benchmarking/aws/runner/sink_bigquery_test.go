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
	"encoding/json"
	"errors"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/benchmarking/aws/plan"
)

const (
	bqTestProject = "bench-proj-123"
	bqTestDataset = "bench_bench_20261120_101010"
)

type bqCall struct{ Op, Project, Arg, Location string }

// fakeBigQuery records every call and answers COUNT(*) from a queue.
type fakeBigQuery struct {
	calls  []bqCall
	counts []int64
	errs   map[string]error // by op
}

func (f *fakeBigQuery) CreateDataset(_ context.Context, project, dataset, location string) error {
	f.calls = append(f.calls, bqCall{"create", project, dataset, location})
	return f.errs["create"]
}

func (f *fakeBigQuery) DeleteDataset(_ context.Context, project, dataset string) error {
	f.calls = append(f.calls, bqCall{"delete", project, dataset, ""})
	return f.errs["delete"]
}

func (f *fakeBigQuery) QueryInt(_ context.Context, project, location, q string) (int64, error) {
	f.calls = append(f.calls, bqCall{"query", project, q, location})
	if err := f.errs["query"]; err != nil {
		return 0, err
	}
	if len(f.counts) == 0 {
		return 0, errors.New("fakeBigQuery: count queue empty")
	}
	n := f.counts[0]
	if len(f.counts) > 1 {
		f.counts = f.counts[1:]
	}
	return n, nil
}

func (f *fakeBigQuery) ops() []string {
	var o []string
	for _, c := range f.calls {
		o = append(o, c.Op)
	}
	return o
}

func bqTestDeps(f BigQueryAPI, location string) SinkDeps {
	d := sinkTestDeps(&fakeRPK{})
	d.BigQuery = f
	d.BigQueryTarget = BigQueryTarget{Project: bqTestProject, Location: location}
	return d
}

func newTestBigQuerySink(t *testing.T, f BigQueryAPI, location string) (*bigQuerySink, *fakeClock) {
	t.Helper()
	s, err := sinkFor(SinkSpec{Kind: "bigquery"}, bqTestDeps(f, location))
	require.NoError(t, err)
	bq, ok := s.(*bigQuerySink)
	require.True(t, ok)
	clk := &fakeClock{t: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)}
	bq.now, bq.sleep = clk.now, clk.sleep
	return bq, clk
}

func TestSinkBigQueryProvisionDatasetAndPlaceholders(t *testing.T) {
	f := &fakeBigQuery{}
	sink, _ := newTestBigQuerySink(t, f, "EU")
	ph, err := sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	require.Equal(t, []bqCall{{"create", bqTestProject, bqTestDataset, "EU"}}, f.calls,
		"only the dataset: the output creates its own table")
	require.Equal(t, map[string]string{
		"BENCH_BQ_PROJECT":          bqTestProject,
		"BENCH_BQ_DATASET":          bqTestDataset,
		"BENCH_BQ_TABLE":            "t",
		"BENCH_BQ_CREDENTIALS_FILE": "/opt/bench/secrets/bigquery.json",
	}, ph)

	f = &fakeBigQuery{errs: map[string]error{"create": errors.New("denied")}}
	sink, _ = newTestBigQuerySink(t, f, "")
	_, err = sink.Provision(t.Context(), sinkTestSession)
	require.ErrorContains(t, err, "denied")
	require.NotErrorIs(t, err, ErrNothingProvisioned, "a failed create may have half-succeeded: tear down")
}

func TestSinkBigQueryLocationDefaultsToUS(t *testing.T) {
	f := &fakeBigQuery{}
	sink, _ := newTestBigQuerySink(t, f, "")
	_, err := sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	require.Equal(t, "US", f.calls[0].Location)
}

func TestSinkBigQueryNameValidation(t *testing.T) {
	require.NoError(t, validBigQueryDataset("bench_bench_20261120_101010"))
	for _, bad := range []string{"", "Bench_x", "a-b", "a.b", "a`b", "x;y", "ä"} {
		require.Error(t, validBigQueryDataset(bad), bad)
	}
	d := bqTestDeps(&fakeBigQuery{}, "")
	d.BigQueryTarget.Project = "p`; DROP"
	_, err := sinkFor(SinkSpec{Kind: "bigquery"}, d)
	require.Error(t, err)
	d = bqTestDeps(&fakeBigQuery{}, "")
	d.BigQueryTarget.Location = "US; x"
	_, err = sinkFor(SinkSpec{Kind: "bigquery"}, d)
	require.Error(t, err)
	_, err = sinkFor(SinkSpec{Kind: "bigquery"}, SinkDeps{})
	require.ErrorContains(t, err, "no bigquery client")

	// The session is folded into a valid name whatever it holds.
	f := &fakeBigQuery{}
	sink, _ := newTestBigQuerySink(t, f, "")
	_, err = sink.Provision(t.Context(), "Bench-2026/11`x")
	require.NoError(t, err)
	require.Equal(t, "bench_bench_2026_11_x", f.calls[0].Arg)
}

func TestSinkBigQueryCountPollsUntilStable(t *testing.T) {
	f := &fakeBigQuery{counts: []int64{100, 250, 300, 300}}
	sink, clk := newTestBigQuerySink(t, f, "EU")
	_, err := sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	f.calls = nil

	n, err := sink.Count(t.Context(), "1")
	require.NoError(t, err)
	require.Equal(t, int64(300), n)
	require.Equal(t, []time.Duration{10 * time.Second, 10 * time.Second, 10 * time.Second}, clk.sleeps)
	require.Len(t, f.calls, 4)
	for _, c := range f.calls {
		require.Equal(t, bqCall{"query", bqTestProject, "SELECT COUNT(*) FROM `" + bqTestProject + "." + bqTestDataset + ".t`", "EU"}, c)
	}
}

func TestSinkBigQueryCountGivesUpAfter90s(t *testing.T) {
	f := &fakeBigQuery{}
	for i := range 100 {
		f.counts = append(f.counts, int64(i*10))
	}
	sink, clk := newTestBigQuerySink(t, f, "")
	n, err := sink.Count(t.Context(), "1")
	require.NoError(t, err)
	var waited time.Duration
	for _, d := range clk.sleeps {
		waited += d
	}
	require.Equal(t, 90*time.Second, waited)
	require.Equal(t, int64(90), n)
}

// The output creates the table on its first write: missing reads as empty
// until rows have been seen, then it is an error.
func TestSinkBigQueryCountMissingTable(t *testing.T) {
	f := &fakeBigQuery{errs: map[string]error{"query": ErrBigQueryNotFound}}
	sink, _ := newTestBigQuerySink(t, f, "")
	n, err := sink.Count(t.Context(), "1")
	require.NoError(t, err)
	require.Zero(t, n)

	f.errs = nil
	f.counts = []int64{500}
	n, err = sink.Count(t.Context(), "1")
	require.NoError(t, err)
	require.Equal(t, int64(500), n)

	f.errs = map[string]error{"query": ErrBigQueryNotFound}
	_, err = sink.Count(t.Context(), "2")
	require.ErrorIs(t, err, ErrBigQueryNotFound)
	require.ErrorContains(t, err, "after rows were written")

	f = &fakeBigQuery{errs: map[string]error{"query": errors.New("quota")}}
	sink, _ = newTestBigQuerySink(t, f, "")
	_, err = sink.Count(t.Context(), "1")
	require.ErrorContains(t, err, "quota")
}

func TestSinkBigQueryTeardownIdempotentAndNotFoundIsSuccess(t *testing.T) {
	f := &fakeBigQuery{}
	sink, _ := newTestBigQuerySink(t, f, "")
	require.NoError(t, sink.Teardown(t.Context(), sinkTestSession))
	require.NoError(t, sink.Teardown(t.Context(), sinkTestSession))
	require.Equal(t, []bqCall{
		{"delete", bqTestProject, bqTestDataset, ""},
		{"delete", bqTestProject, bqTestDataset, ""},
	}, f.calls)

	f.errs = map[string]error{"delete": ErrBigQueryNotFound}
	require.NoError(t, sink.Teardown(t.Context(), sinkTestSession))

	f.errs = map[string]error{"delete": errors.New("denied")}
	require.ErrorContains(t, sink.Teardown(t.Context(), sinkTestSession), "denied")
}

func TestSinkBigQueryHostSetupAndEnv(t *testing.T) {
	sink, _ := newTestBigQuerySink(t, &fakeBigQuery{}, "")
	script := sink.HostSetup()
	require.Contains(t, script, "mkdir -p /opt/bench/secrets")
	require.Contains(t, script, "chmod 0700 /opt/bench/secrets")
	require.Contains(t, script, `trap 'rm -f /opt/bench/secrets/bigquery.json' EXIT`)
	require.Contains(t, script, `aws secretsmanager get-secret-value --region "us-east-2" --secret-id "redpanda-connect-bench/bigquery" --query SecretString --output text`)
	require.Contains(t, script, "jq -er .credentials_json > /opt/bench/secrets/bigquery.json")
	require.Contains(t, script, "umask 077")
	require.Less(t, strings.Index(script, "rm -f /opt/bench/secrets/bigquery.json\n"), strings.Index(script, "get-secret-value"))
	require.Less(t, strings.Index(script, "trap"), strings.Index(script, "get-secret-value"))

	env := sink.HostEnv()
	require.Equal(t, map[string]string{"GOOGLE_APPLICATION_CREDENTIALS": "/opt/bench/secrets/bigquery.json"}, env)
	require.Equal(t, env, hostEnvFor(sink))
	require.Equal(t, env, hostEnvFor(uncountedSink{sink}), "the count-hiding wrapper forwards it")
	require.Equal(t, script, hostSetupFor(uncountedSink{sink}))
	require.Empty(t, hostEnvFor(dropSink{}))
	require.Empty(t, hostEnvFor(nil))
}

func TestBenchScriptLaunchLineCarriesHostEnv(t *testing.T) {
	a := benchScriptArgs{VCPU: 1, BinaryPath: "/opt/bench/rpc", ConfigPath: "/c"}
	plain := renderBenchScript(a)
	require.NotContains(t, plain, "GOOGLE_APPLICATION_CREDENTIALS")

	a.HostEnv = map[string]string{"Z_VAR": "z", "GOOGLE_APPLICATION_CREDENTIALS": "/opt/bench/secrets/bigquery.json"}
	out := renderBenchScript(a)
	require.Contains(t, out, "REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt GOOGLE_APPLICATION_CREDENTIALS=/opt/bench/secrets/bigquery.json Z_VAR=z /opt/bench/rpc",
		"sorted extra env pairs sit between the license path and the binary")
	// Nothing else in the script changed.
	require.Equal(t, plain, strings.Replace(out, " GOOGLE_APPLICATION_CREDENTIALS=/opt/bench/secrets/bigquery.json Z_VAR=z", "", 1))
}

// bqKeyJSON is a service-account key with distinctive fake material.
func bqKeyJSON(t *testing.T, pemKey string) string {
	t.Helper()
	b, err := json.Marshal(map[string]string{
		"type": "service_account", "project_id": bqTestProject, "private_key_id": "pkid-9f3a77c1d2e4",
		"private_key": pemKey, "client_email": "bench@bench-proj-123.iam.gserviceaccount.com",
	})
	require.NoError(t, err)
	return string(b)
}

func bqSecretJSON(t *testing.T, pemKey, location string) string {
	t.Helper()
	m := map[string]string{"project": bqTestProject, "credentials_json": bqKeyJSON(t, pemKey)}
	if location != "" {
		m["location"] = location
	}
	b, err := json.Marshal(m)
	require.NoError(t, err)
	return string(b)
}

func TestLoadBigQueryParsesSecret(t *testing.T) {
	key := fakeKeyPEM(t)
	sm := &FakeSecretsManagerClient{SecretString: bqSecretJSON(t, key, "EU")}
	var gotJSON []byte
	api, target, err := loadBigQuery(t.Context(), sm, func(_ context.Context, project string, creds []byte) (BigQueryAPI, error) {
		gotJSON = creds
		require.Equal(t, bqTestProject, project)
		return &fakeBigQuery{}, nil
	})
	require.NoError(t, err)
	require.NotNil(t, api)
	require.Equal(t, []string{bigQuerySecretID}, sm.Requests)
	require.Equal(t, BigQueryTarget{Project: bqTestProject, Location: "EU"}, target)
	require.Contains(t, string(gotJSON), "pkid-9f3a77c1d2e4")

	sm = &FakeSecretsManagerClient{SecretString: bqSecretJSON(t, key, "")}
	_, target, err = loadBigQuery(t.Context(), sm, func(context.Context, string, []byte) (BigQueryAPI, error) { return &fakeBigQuery{}, nil })
	require.NoError(t, err)
	require.Equal(t, "US", target.Location, "location is optional")
}

func TestLoadBigQueryErrorsCarryNoKeyMaterial(t *testing.T) {
	key := fakeKeyPEM(t)
	opener := func(context.Context, string, []byte) (BigQueryAPI, error) { return &fakeBigQuery{}, nil }
	failing := func(context.Context, string, []byte) (BigQueryAPI, error) {
		return nil, errors.New("client failed")
	}
	good := bqSecretJSON(t, key, "")
	for name, tc := range map[string]struct {
		secret string
		open   bigQueryOpener
	}{
		"not json":     {"{" + key, opener},
		"no project":   {strings.Replace(good, `"project":"bench-proj-123"`, `"project":""`, 1), opener},
		"bad project":  {strings.Replace(good, `"project":"bench-proj-123"`, `"project":"p\"x"`, 1), opener},
		"bad location": {bqSecretJSON(t, key, "US; x"), opener},
		"no creds":     {`{"project":"bench-proj-123"}`, opener},
		"creds not sa": {`{"project":"bench-proj-123","credentials_json":"` + keyBody(key) + `"}`, opener},
		"open fails":   {good, failing},
	} {
		_, _, err := loadBigQuery(t.Context(), &FakeSecretsManagerClient{SecretString: tc.secret}, tc.open)
		require.Error(t, err, name)
		assertNoKeyMaterial(t, err.Error(), key, name)
		require.NotContains(t, err.Error(), "pkid-9f3a77c1d2e4", name)
	}
	_, _, err := loadBigQuery(t.Context(), &FakeSecretsManagerClient{Err: errors.New("boom")}, opener)
	require.ErrorContains(t, err, bigQuerySecretID)
}

// A whole bigquery run, from the secret to teardown, never puts the service
// account key in the rendered config, any SSM command, any query or any
// logged line. Connect reaches it only through the host-side file.
func TestSinkBigQueryKeyNeverLeaves(t *testing.T) {
	key := fakeKeyPEM(t)
	sm := &FakeSecretsManagerClient{SecretString: bqSecretJSON(t, key, "")}

	cfg := map[string]any{
		"input": map[string]any{"redpanda": map[string]any{}},
		"output": map[string]any{"gcp_bigquery": map[string]any{
			"project": "cust", "dataset": "d", "table": "CUSTOMERS",
			"credentials_json":   bqKeyJSON(t, "-----BEGIN PRIVATE KEY-----custkey-----END PRIVATE KEY-----"),
			"create_disposition": "CREATE_NEVER",
		}},
	}
	swapped, caveats, err := plan.SwapIdentity(cfg, plan.SourceRedpanda, plan.SinkBigQuery)
	require.NoError(t, err)
	require.Contains(t, caveats, plan.CaveatSchemaEvoForced)
	s := &Scenario{
		Direction: DirectionE2E,
		Connector: "redpanda",
		Pipeline:  swapped,
		Sink:      &SinkSpec{Kind: "bigquery"},
		Matrix:    MatrixRunnerTestMatrix,
	}
	outs := map[string]string{"results_bucket": "b", "bench_session_id": sinkTestSession, "aws_region": "us-east-2"}
	names := newBenchNames(sinkTestSession, s.Connector)
	pt := buildSweepPlan(s)[0]

	fbq := &fakeBigQuery{counts: []int64{0, 0, 10_150, 10_150, 10_150}}
	ssm := &FakeSSM{}
	var rendered string
	var runLog bytes.Buffer
	osLog := captureOS(t, func() {
		api, target, err := loadBigQuery(t.Context(), sm, func(context.Context, string, []byte) (BigQueryAPI, error) { return fbq, nil })
		require.NoError(t, err)
		deps := sinkTestDeps(&fakeRPK{})
		deps.BigQuery, deps.BigQueryTarget = api, target
		sink, err := sinkForScenario(s, deps)
		require.NoError(t, err)
		bq := sink.(*bigQuerySink)
		clk := &fakeClock{t: time.Unix(0, 0)}
		bq.now, bq.sleep = clk.now, clk.sleep

		merged := maps.Clone(outs)
		require.NoError(t, withSink(t.Context(), sink, sinkTestSession, func(ph map[string]string) error {
			maps.Copy(merged, ph)
			set, err := renderPointConfigs(s, merged, e2eTopology{}, names, pt)
			require.NoError(t, err)
			require.NoError(t, checkPlaceholdersResolved([]renderedPointConfigs{set}))
			raw, err := os.ReadFile(set.Single)
			require.NoError(t, err)
			rendered = string(raw)

			mr := e2eSinkRunner(t, countableSink(sink, s, e2eSinkPlan), ssm, 10)
			stdout = &runLog
			mr.HostSetup = hostSetupFor(sink)
			mr.HostEnv = hostEnvFor(sink)
			_, err = mr.Run(t.Context(), e2eSinkPlan[:1], 1, 5*time.Second, 5*time.Second, "", "")
			return err
		}))
	})

	require.NotContains(t, rendered, "credentials_json")
	require.Contains(t, rendered, "project: "+bqTestProject)
	require.Contains(t, rendered, "dataset: "+bqTestDataset)
	require.Contains(t, rendered, "create_disposition: CREATE_IF_NEEDED")
	require.NotContains(t, rendered, "custkey")
	for _, secret := range []string{key} {
		assertNoKeyMaterial(t, rendered, secret, "rendered config")
	}
	require.NotContains(t, rendered, "pkid-9f3a77c1d2e4")

	require.NotEmpty(t, ssm.Scripts)
	var sawFetch, sawEnv bool
	for _, sc := range ssm.Scripts {
		assertNoKeyMaterial(t, sc, key, "ssm script")
		require.NotContains(t, sc, "pkid-9f3a77c1d2e4")
		sawFetch = sawFetch || strings.Contains(sc, "secretsmanager get-secret-value")
		sawEnv = sawEnv || strings.Contains(sc, "GOOGLE_APPLICATION_CREDENTIALS=/opt/bench/secrets/bigquery.json ")
	}
	require.True(t, sawFetch, "the bench script fetches the key on the host")
	require.True(t, sawEnv, "the launch line points ADC at the host file")

	for _, c := range fbq.calls {
		assertNoKeyMaterial(t, c.Arg+c.Project+c.Location, key, "bigquery call")
	}
	assertNoKeyMaterial(t, runLog.String(), key, "runner log")
	assertNoKeyMaterial(t, osLog, key, "stdout/stderr")
	require.NotContains(t, osLog+runLog.String(), "pkid-9f3a77c1d2e4")
	require.Equal(t, []string{"create", "query", "query", "query", "query", "delete"}, fbq.ops())
}

func TestSharedVarsBigQuerySecrets(t *testing.T) {
	vars := func(s *Scenario) map[string]string { return sharedStackVars(s, "us-east-2", "bench-x") }
	e2e := func(kind string) *Scenario {
		return &Scenario{Direction: DirectionE2E, Sink: &SinkSpec{Kind: kind}}
	}
	require.Equal(t, "true", vars(e2e("bigquery"))["sink_secrets_enabled"])
	require.Equal(t, "false", vars(e2e("redpanda"))["sink_secrets_enabled"])
	require.Equal(t, "false", vars(&Scenario{Direction: "source", Sink: &SinkSpec{Kind: "bigquery"}})["sink_secrets_enabled"])
	require.Equal(t, "false", vars(&Scenario{Soak: true})["sink_secrets_enabled"])
}
