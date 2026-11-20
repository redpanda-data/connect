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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/plan"
)

const (
	sfTestDB     = "BENCH_DB"
	sfTestWH     = "BENCH_WH"
	sfTestSchema = "BENCH_BENCH_20261120_101010"
)

// fakeSnowflake records every statement and answers COUNT(*) queries from a
// queue. errs fails any statement starting with a key.
type fakeSnowflake struct {
	sql    []string
	counts []int64
	errs   map[string]error
}

func (f *fakeSnowflake) errFor(q string) error {
	for prefix, err := range f.errs {
		if strings.HasPrefix(q, prefix) {
			return err
		}
	}
	return nil
}

func (f *fakeSnowflake) Exec(_ context.Context, q string) error {
	f.sql = append(f.sql, q)
	return f.errFor(q)
}

func (f *fakeSnowflake) QueryInt(_ context.Context, q string) (int64, error) {
	f.sql = append(f.sql, q)
	if err := f.errFor(q); err != nil {
		return 0, err
	}
	if len(f.counts) == 0 {
		return 0, errors.New("fakeSnowflake: count queue empty")
	}
	n := f.counts[0]
	if len(f.counts) > 1 {
		f.counts = f.counts[1:]
	}
	return n, nil
}

// fakeClock is a clock whose sleeps advance it instantly.
type fakeClock struct {
	t      time.Time
	sleeps []time.Duration
}

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) sleep(_ context.Context, d time.Duration) error {
	c.sleeps = append(c.sleeps, d)
	c.t = c.t.Add(d)
	return nil
}

func sfTestDeps(f SnowflakeExec, output any) SinkDeps {
	d := sinkTestDeps(&fakeRPK{})
	d.Snowflake = f
	d.SnowflakeTarget = SnowflakeTarget{
		Account: "BENCHORG-BENCHACCT", User: "BENCH_SVC", Role: "BENCH_ROLE",
		Database: sfTestDB, Warehouse: sfTestWH,
	}
	d.Output = output
	return d
}

func newTestSnowflakeSink(t *testing.T, f SnowflakeExec, spec SinkSpec, output any) (*snowflakeSink, *fakeClock) {
	t.Helper()
	spec.Kind = "snowflake"
	s, err := sinkFor(spec, sfTestDeps(f, output))
	require.NoError(t, err)
	sf, ok := s.(*snowflakeSink)
	require.True(t, ok)
	clk := &fakeClock{t: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)}
	sf.now, sf.sleep = clk.now, clk.sleep
	return sf, clk
}

func streamingOutput(cfg map[string]any) map[string]any {
	return map[string]any{"snowflake_streaming": cfg}
}

func TestSinkSnowflakeProvisionSQLAndPlaceholders(t *testing.T) {
	f := &fakeSnowflake{}
	sink, _ := newTestSnowflakeSink(t, f, SinkSpec{}, streamingOutput(map[string]any{"table": "${BENCH_SNOWFLAKE_TABLE}"}))
	ph, err := sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	require.Equal(t, []string{
		"CREATE SCHEMA IF NOT EXISTS BENCH_DB." + sfTestSchema,
		"ALTER WAREHOUSE BENCH_WH SET WAREHOUSE_SIZE = XSMALL",
		"CREATE TABLE IF NOT EXISTS BENCH_DB." + sfTestSchema + ".T (RECORD VARIANT)",
	}, f.sql)
	require.Equal(t, map[string]string{
		"BENCH_SNOWFLAKE_ACCOUNT":          "BENCHORG-BENCHACCT",
		"BENCH_SNOWFLAKE_USER":             "BENCH_SVC",
		"BENCH_SNOWFLAKE_ROLE":             "BENCH_ROLE",
		"BENCH_SNOWFLAKE_DATABASE":         sfTestDB,
		"BENCH_SNOWFLAKE_SCHEMA":           sfTestSchema,
		"BENCH_SNOWFLAKE_WAREHOUSE":        sfTestWH,
		"BENCH_SNOWFLAKE_TABLE":            "T",
		"BENCH_SNOWFLAKE_STAGE":            "S",
		"BENCH_SNOWFLAKE_PRIVATE_KEY_FILE": "/opt/bench/secrets/snowflake_key.p8",
	}, ph)
}

func TestSinkSnowflakeSizeFromSpec(t *testing.T) {
	f := &fakeSnowflake{}
	sink, _ := newTestSnowflakeSink(t, f, SinkSpec{Size: "MEDIUM"}, nil)
	_, err := sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	require.Contains(t, f.sql, "ALTER WAREHOUSE BENCH_WH SET WAREHOUSE_SIZE = MEDIUM")

	_, err = sinkFor(SinkSpec{Kind: "snowflake", Size: "HUGE"}, sfTestDeps(&fakeSnowflake{}, nil))
	require.ErrorContains(t, err, `"HUGE"`)
}

// With schema evolution the output creates its own table, so Provision does
// not; and Count reads zero until the output has created it.
func TestSinkSnowflakeSchemaEvolutionSkipsTable(t *testing.T) {
	f := &fakeSnowflake{counts: []int64{0}}
	out := map[string]any{"broker": map[string]any{"outputs": []any{
		streamingOutput(map[string]any{"schema_evolution": map[string]any{"enabled": true}}),
	}}}
	sink, _ := newTestSnowflakeSink(t, f, SinkSpec{}, out)
	_, err := sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	for _, q := range f.sql {
		require.NotContains(t, q, "CREATE TABLE")
	}

	f.errs = map[string]error{"SELECT COUNT(*)": ErrSnowflakeNoObject}
	n, err := sink.Count(t.Context(), "1")
	require.NoError(t, err)
	require.Zero(t, n)
}

// snowflake_put stages files and loads no rows, so it gets a stage, no
// table, and no count.
func TestSinkSnowflakePutCreatesStageAndCannotCount(t *testing.T) {
	f := &fakeSnowflake{}
	sink, _ := newTestSnowflakeSink(t, f, SinkSpec{}, map[string]any{"snowflake_put": map[string]any{}})
	_, err := sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	require.Contains(t, f.sql, "CREATE STAGE IF NOT EXISTS BENCH_DB."+sfTestSchema+".S")
	for _, q := range f.sql {
		require.NotContains(t, q, "CREATE TABLE")
	}
	_, err = sink.Count(t.Context(), "1")
	require.ErrorIs(t, err, ErrCountUnsupported)
}

func TestSinkSnowflakeCountPollsUntilStable(t *testing.T) {
	f := &fakeSnowflake{counts: []int64{100, 250, 300, 300}}
	sink, clk := newTestSnowflakeSink(t, f, SinkSpec{}, nil)
	_, err := sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	f.sql = nil

	n, err := sink.Count(t.Context(), "1")
	require.NoError(t, err)
	require.Equal(t, int64(300), n)
	require.Equal(t, []time.Duration{10 * time.Second, 10 * time.Second, 10 * time.Second}, clk.sleeps)
	require.Len(t, f.sql, 4)
	for _, q := range f.sql {
		require.Equal(t, "SELECT COUNT(*) FROM BENCH_DB."+sfTestSchema+".T", q)
	}
}

func TestSinkSnowflakeCountGivesUpAfter90s(t *testing.T) {
	// Never stable: every read is higher than the last.
	f := &fakeSnowflake{}
	for i := range 100 {
		f.counts = append(f.counts, int64(i*10))
	}
	sink, clk := newTestSnowflakeSink(t, f, SinkSpec{}, nil)
	n, err := sink.Count(t.Context(), "1")
	require.NoError(t, err)
	var waited time.Duration
	for _, d := range clk.sleeps {
		waited += d
	}
	require.Equal(t, 90*time.Second, waited)
	require.Equal(t, int64(90), n, "the last value read is returned")
}

func TestSinkSnowflakeCountError(t *testing.T) {
	f := &fakeSnowflake{errs: map[string]error{"SELECT": errors.New("warehouse suspended")}}
	sink, _ := newTestSnowflakeSink(t, f, SinkSpec{}, nil)
	_, err := sink.Count(t.Context(), "1")
	require.ErrorContains(t, err, "warehouse suspended")
}

func TestSinkSnowflakeTeardownSQLAndIdempotent(t *testing.T) {
	f := &fakeSnowflake{}
	sink, _ := newTestSnowflakeSink(t, f, SinkSpec{Size: "LARGE"}, nil)
	want := []string{
		"DROP SCHEMA IF EXISTS BENCH_DB." + sfTestSchema + " CASCADE",
		"ALTER WAREHOUSE BENCH_WH SET WAREHOUSE_SIZE = XSMALL",
	}
	require.NoError(t, sink.Teardown(t.Context(), sinkTestSession))
	require.NoError(t, sink.Teardown(t.Context(), sinkTestSession))
	require.Equal(t, append(append([]string{}, want...), want...), f.sql)

	// A failed drop still resets the warehouse, and both errors surface.
	f = &fakeSnowflake{errs: map[string]error{"DROP": errors.New("drop denied"), "ALTER": errors.New("alter denied")}}
	sink, _ = newTestSnowflakeSink(t, f, SinkSpec{}, nil)
	err := sink.Teardown(t.Context(), sinkTestSession)
	require.ErrorContains(t, err, "drop denied")
	require.ErrorContains(t, err, "alter denied")
	require.Equal(t, want, f.sql)
}

func TestSinkSnowflakeIdentifierValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		mod  func(*SnowflakeTarget)
	}{
		{"database", func(t *SnowflakeTarget) { t.Database = "DB; DROP DATABASE PROD" }},
		{"warehouse", func(t *SnowflakeTarget) { t.Warehouse = "wh-1" }},
		{"empty database", func(t *SnowflakeTarget) { t.Database = "" }},
	} {
		d := sfTestDeps(&fakeSnowflake{}, nil)
		tc.mod(&d.SnowflakeTarget)
		_, err := sinkFor(SinkSpec{Kind: "snowflake"}, d)
		require.Error(t, err, tc.name)
	}
	// Lower-case names are upper-cased, not rejected.
	d := sfTestDeps(&fakeSnowflake{}, nil)
	d.SnowflakeTarget.Database, d.SnowflakeTarget.Warehouse = "bench_db", "bench_wh"
	s, err := sinkFor(SinkSpec{Kind: "snowflake"}, d)
	require.NoError(t, err)
	require.Equal(t, "BENCH_DB", s.(*snowflakeSink).target.Database)

	require.NoError(t, validSnowflakeIdent("BENCH_ABC_123"))
	for _, bad := range []string{"", "a.b", "X Y", "T\"", "ä"} {
		require.Error(t, validSnowflakeIdent(bad), bad)
	}
	_, err = sinkFor(SinkSpec{Kind: "snowflake"}, SinkDeps{})
	require.ErrorContains(t, err, "no snowflake client")
}

// fakeKeyPEM is a freshly generated PKCS#8 RSA key, so nothing secret is
// committed and the absence checks look for real key material.
func fakeKeyPEM(t *testing.T) string {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(k)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

func sfSecretJSON(t *testing.T, key string) string {
	t.Helper()
	b, err := json.Marshal(map[string]string{
		"account": "BENCHORG-BENCHACCT", "user": "BENCH_SVC", "private_key": key,
		"database": sfTestDB, "warehouse": sfTestWH, "role": "BENCH_ROLE",
	})
	require.NoError(t, err)
	return string(b)
}

func TestLoadSnowflakeParsesSecret(t *testing.T) {
	key := fakeKeyPEM(t)
	sm := &FakeSecretsManagerClient{SecretString: sfSecretJSON(t, key)}
	var gotKey *rsa.PrivateKey
	var gotTarget SnowflakeTarget
	exec, target, err := loadSnowflake(t.Context(), sm, func(tg SnowflakeTarget, k *rsa.PrivateKey) (SnowflakeExec, error) {
		gotTarget, gotKey = tg, k
		return &fakeSnowflake{}, nil
	})
	require.NoError(t, err)
	require.NotNil(t, exec)
	require.NotNil(t, gotKey)
	require.Equal(t, []string{snowflakeSecretID}, sm.Requests)
	require.Equal(t, SnowflakeTarget{
		Account: "BENCHORG-BENCHACCT", User: "BENCH_SVC", Role: "BENCH_ROLE",
		Database: sfTestDB, Warehouse: sfTestWH,
	}, target)
	require.Equal(t, target, gotTarget)
}

// No error from loading a bad secret repeats what was in it.
func TestLoadSnowflakeErrorsCarryNoKeyMaterial(t *testing.T) {
	key := fakeKeyPEM(t)
	body := keyBody(key)
	opener := func(SnowflakeTarget, *rsa.PrivateKey) (SnowflakeExec, error) { return &fakeSnowflake{}, nil }
	for name, secret := range map[string]string{
		"not json":     "{" + key,
		"bad key":      sfSecretJSON(t, strings.Replace(key, body[:40], "AAAA", 1)),
		"not pem":      sfSecretJSON(t, body),
		"missing user": strings.Replace(sfSecretJSON(t, key), `"user":"BENCH_SVC"`, `"user":""`, 1),
	} {
		_, _, err := loadSnowflake(t.Context(), &FakeSecretsManagerClient{SecretString: secret}, opener)
		require.Error(t, err, name)
		assertNoKeyMaterial(t, err.Error(), key, name)
	}
}

// keyBody is the base64 body of a PEM key, newlines removed.
func keyBody(pemKey string) string {
	var b strings.Builder
	for l := range strings.SplitSeq(pemKey, "\n") {
		if l != "" && !strings.HasPrefix(l, "-----") {
			b.WriteString(l)
		}
	}
	return b.String()
}

// assertNoKeyMaterial fails when text holds a PEM header or any 16-byte run
// of the key's base64 body, line-wrapped or not.
func assertNoKeyMaterial(t *testing.T, text, pemKey, what string) {
	t.Helper()
	require.NotContains(t, text, "PRIVATE KEY", what)
	require.NotContains(t, text, "-----BEGIN", what)
	flat := strings.ReplaceAll(strings.ReplaceAll(text, "\n", ""), `\n`, "")
	body := keyBody(pemKey)
	for i := 0; i+16 <= len(body); i += 16 {
		require.NotContains(t, flat, body[i:i+16], "%s: key material at offset %d", what, i)
	}
}

// captureOS returns everything written to os.Stdout and os.Stderr during fn.
func captureOS(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	oldOut, oldErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = w, w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	defer func() {
		os.Stdout, os.Stderr = oldOut, oldErr
	}()
	fn()
	os.Stdout, os.Stderr = oldOut, oldErr
	require.NoError(t, w.Close())
	return <-done
}

// A whole snowflake run, from the secret to teardown, never puts the key in
// the rendered config, in any SSM command the runner sends, or in any line
// it logs. The key reaches Connect only through the host-side file.
func TestSinkSnowflakeKeyNeverLeaves(t *testing.T) {
	key := fakeKeyPEM(t)
	sm := &FakeSecretsManagerClient{SecretString: sfSecretJSON(t, key)}

	cfg := map[string]any{
		"input": map[string]any{"redpanda": map[string]any{}},
		"output": map[string]any{"snowflake_streaming": map[string]any{
			"account": "CUST", "user": "u", "private_key": "-----BEGIN PRIVATE KEY-----custkey-----END PRIVATE KEY-----",
			"database": "D", "schema": "S", "table": "CUSTOMERS", "role": "R",
		}},
	}
	swapped, _, err := plan.SwapIdentity(cfg, plan.SourceRedpanda, plan.SinkSnowflake)
	require.NoError(t, err)
	s := &Scenario{
		Direction: DirectionE2E,
		Connector: "redpanda",
		Pipeline:  swapped,
		Sink:      &SinkSpec{Kind: "snowflake", Size: "XSMALL"},
		Matrix:    MatrixRunnerTestMatrix,
	}
	outs := map[string]string{"results_bucket": "b", "bench_session_id": sinkTestSession, "aws_region": "us-east-2"}
	names := newBenchNames(sinkTestSession, s.Connector)
	pt := buildSweepPlan(s)[0]

	fsf := &fakeSnowflake{counts: []int64{0, 0, 10_150, 10_150, 10_150}}
	ssm := &FakeSSM{}
	var rendered string
	var runLog bytes.Buffer
	osLog := captureOS(t, func() {
		exec, target, err := loadSnowflake(t.Context(), sm, func(SnowflakeTarget, *rsa.PrivateKey) (SnowflakeExec, error) {
			return fsf, nil
		})
		require.NoError(t, err)
		deps := sinkTestDeps(&fakeRPK{})
		deps.Snowflake, deps.SnowflakeTarget = exec, target
		sink, err := sinkForScenario(s, deps)
		require.NoError(t, err)
		sf := sink.(*snowflakeSink)
		clk := &fakeClock{t: time.Unix(0, 0)}
		sf.now, sf.sleep = clk.now, clk.sleep

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
			_, err = mr.Run(t.Context(), e2eSinkPlan[:1], 1, 5*time.Second, 5*time.Second, "", "")
			return err
		}))
	})

	require.Contains(t, rendered, "private_key_file: /opt/bench/secrets/snowflake_key.p8")
	require.NotContains(t, rendered, "custkey")
	assertNoKeyMaterial(t, rendered, key, "rendered config")

	require.NotEmpty(t, ssm.Scripts)
	var sawFetch bool
	for _, sc := range ssm.Scripts {
		assertNoKeyMaterial(t, sc, key, "ssm script")
		if strings.Contains(sc, "secretsmanager get-secret-value") {
			sawFetch = true
		}
	}
	require.True(t, sawFetch, "the bench script fetches the key on the host")

	for _, q := range fsf.sql {
		assertNoKeyMaterial(t, q, key, "sql")
	}
	assertNoKeyMaterial(t, runLog.String(), key, "runner log")
	assertNoKeyMaterial(t, osLog, key, "stdout/stderr")
}

// MatrixRunnerTestMatrix is a one-point matrix for scenario fixtures.
var MatrixRunnerTestMatrix = MatrixSpec{CPUPoints: []int{1}}

func TestSnowflakeHostSetupScript(t *testing.T) {
	f := &fakeSnowflake{}
	sink, _ := newTestSnowflakeSink(t, f, SinkSpec{}, nil)
	script := sink.HostSetup()
	require.Contains(t, script, "mkdir -p /opt/bench/secrets")
	require.Contains(t, script, "chmod 0700 /opt/bench/secrets")
	require.Contains(t, script, `trap 'rm -f /opt/bench/secrets/snowflake_key.p8' EXIT`)
	require.Contains(t, script, `aws secretsmanager get-secret-value --region "us-east-2" --secret-id "redpanda-connect-bench/snowflake" --query SecretString --output text`)
	require.Contains(t, script, "jq -er .private_key > /opt/bench/secrets/snowflake_key.p8")
	require.Contains(t, script, "umask 077")
	require.Less(t, strings.Index(script, "trap"), strings.Index(script, "get-secret-value"),
		"the cleanup trap is armed before the key is written")

	require.Empty(t, hostSetupFor(dropSink{}))
	require.Empty(t, hostSetupFor(nil))

	b := renderBenchScript(benchScriptArgs{VCPU: 1, HostSetup: script, BinaryPath: "/opt/bench/rpc", ConfigPath: "/c"})
	require.Less(t, strings.Index(b, "get-secret-value"), strings.Index(b, "taskset"), "the key is on disk before Connect launches")
	require.NotContains(t, renderBenchScript(benchScriptArgs{VCPU: 1}), "secretsmanager")
}
