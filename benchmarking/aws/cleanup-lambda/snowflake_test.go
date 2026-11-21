// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// fakeSnowflakeReaper lists canned schemas and records every statement.
type fakeSnowflakeReaper struct {
	schemas []snowflakeSchemaInfo
	listDB  string
	listErr error
	sql     []string
	execErr map[string]error
	closed  bool
}

func (f *fakeSnowflakeReaper) ListBenchSchemas(_ context.Context, db string) ([]snowflakeSchemaInfo, error) {
	f.listDB = db
	return f.schemas, f.listErr
}

func (f *fakeSnowflakeReaper) Exec(_ context.Context, q string) error {
	f.sql = append(f.sql, q)
	return f.execErr[q]
}

func (f *fakeSnowflakeReaper) Close() error { f.closed = true; return nil }

func openerFor(f *fakeSnowflakeReaper, db string) snowflakeOpener {
	return func(context.Context) (snowflakeReaperAPI, snowflakeCreds, error) {
		return f, snowflakeCreds{Database: db, Warehouse: "BENCH_WH"}, nil
	}
}

func TestSweepSnowflake_SelectsBenchPrefixOlderThanTTL(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	old, young := now.Add(-5*time.Hour), now.Add(-30*time.Minute)
	sf := &fakeSnowflakeReaper{schemas: []snowflakeSchemaInfo{
		{Name: "BENCH_BENCH_20261120_060000", CreatedOn: old},
		{Name: "BENCH_BENCH_20261120_113000", CreatedOn: young},
		{Name: "BENCHMARKS", CreatedOn: old},   // LIKE 'BENCH_%' matches it: _ is a wildcard
		{Name: "PUBLIC", CreatedOn: old},       // never listed by the LIKE, but never touched either
		{Name: "BENCH_X;DROP", CreatedOn: old}, // not a plain identifier
	}}
	report, err := Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", openerFor(sf, "BENCH_DB"))
	require.NoError(t, err)
	require.Equal(t, "BENCH_DB", sf.listDB)
	require.Equal(t, []string{"DROP SCHEMA IF EXISTS BENCH_DB.BENCH_BENCH_20261120_060000 CASCADE"}, sf.sql)
	require.Equal(t, []string{"snowflake-schema:BENCH_DB.BENCH_BENCH_20261120_060000"}, report.Destroyed)
	require.True(t, sf.closed)
}

func TestSweepSnowflake_FailuresAreReportedNotFatal(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	q := "DROP SCHEMA IF EXISTS BENCH_DB.BENCH_A CASCADE"
	sf := &fakeSnowflakeReaper{
		schemas: []snowflakeSchemaInfo{{Name: "BENCH_A", CreatedOn: now.Add(-5 * time.Hour)}},
		execErr: map[string]error{q: errors.New("insufficient privileges")},
	}
	report, err := Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", openerFor(sf, "BENCH_DB"))
	require.NoError(t, err)
	require.Equal(t, 1, report.Errors)
	require.Contains(t, report.Failed[0], "snowflake-schema:BENCH_DB.BENCH_A")

	sf = &fakeSnowflakeReaper{listErr: errors.New("warehouse suspended")}
	report, err = Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", openerFor(sf, "BENCH_DB"))
	require.NoError(t, err)
	require.Equal(t, 1, report.Errors)
	require.Contains(t, report.Failed[0], "snowflake-schemas")

	failOpen := func(context.Context) (snowflakeReaperAPI, snowflakeCreds, error) {
		return nil, snowflakeCreds{}, errors.New("login failed")
	}
	report, err = Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", failOpen)
	require.NoError(t, err)
	require.Equal(t, 1, report.Errors)

	sf = &fakeSnowflakeReaper{}
	report, err = Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", openerFor(sf, "BENCH-DB"))
	require.NoError(t, err)
	require.Equal(t, 1, report.Errors, "a bad database name is refused before any SQL")
	require.Empty(t, sf.sql)
}

// Before the operator creates the secret, or when no secret is configured,
// the Snowflake sweep is skipped without an error.
func TestSweepSnowflake_SkippedWhenSecretMissing(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	// A secret that does not exist yet answers AccessDenied rather than
	// ResourceNotFound when the caller's grant names only the suffixed ARN,
	// so both mean "not set up yet".
	for name, gerr := range map[string]error{
		"not found":     &smtypes.ResourceNotFoundException{Message: strPtr("not found")},
		"access denied": &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized"},
	} {
		sm := &fakeSecrets{err: gerr}
		open := newSnowflakeOpener(sm, "arn:aws:secretsmanager:us-east-2:1:secret:redpanda-connect-bench/snowflake", nil)
		_, _, err := open(t.Context())
		require.ErrorIs(t, err, errSnowflakeSecretMissing, name)

		report, err := Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", open)
		require.NoError(t, err, name)
		require.Zero(t, report.Errors, name)
		require.Empty(t, report.Failed, name)
	}
	// Any other secrets error is a real failure.
	open := newSnowflakeOpener(&fakeSecrets{err: errors.New("throttled")}, "sid", nil)
	_, _, err := open(t.Context())
	require.Error(t, err)
	require.NotErrorIs(t, err, errSnowflakeSecretMissing)

	report, err := Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", nil)
	require.NoError(t, err)
	require.Zero(t, report.Errors)
}

type fakeSecrets struct {
	value string
	err   error
	ids   []string
}

func (f *fakeSecrets) GetSecretValue(_ context.Context, in *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	f.ids = append(f.ids, *in.SecretId)
	if f.err != nil {
		return nil, f.err
	}
	return &secretsmanager.GetSecretValueOutput{SecretString: &f.value}, nil
}

func strPtr(s string) *string { return &s }

func testKeyPEM(t *testing.T) string {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(k)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

func TestSnowflakeOpenerReadsSecret(t *testing.T) {
	key := testKeyPEM(t)
	b, err := json.Marshal(map[string]string{
		"account": "ORG-ACCT", "user": "BENCH_SVC", "private_key": key,
		"database": "bench_db", "warehouse": "BENCH_WH", "role": "BENCH_ROLE",
	})
	require.NoError(t, err)
	sm := &fakeSecrets{value: string(b)}
	var got snowflakeCreds
	var gotKey *rsa.PrivateKey
	fake := &fakeSnowflakeReaper{}
	open := newSnowflakeOpener(sm, "sid", func(c snowflakeCreds, k *rsa.PrivateKey) (snowflakeReaperAPI, error) {
		got, gotKey = c, k
		return fake, nil
	})
	api, creds, err := open(t.Context())
	require.NoError(t, err)
	require.Same(t, fake, api)
	require.Equal(t, "BENCH_DB", creds.Database, "upper-cased")
	require.Equal(t, []string{"sid"}, sm.ids)
	require.NotNil(t, gotKey)
	require.Equal(t, "ORG-ACCT", got.Account)
	require.Equal(t, "BENCH_WH", got.Warehouse)

	// A broken secret fails without echoing its content (the report goes to
	// SNS and Slack).
	sm.value = strings.Replace(string(b), "MII", "@@@", 1)
	_, _, err = open(t.Context())
	require.Error(t, err)
	require.NotContains(t, err.Error(), "PRIVATE KEY")
	sm.value = "{" + key
	_, _, err = open(t.Context())
	require.Error(t, err)
	require.NotContains(t, err.Error(), "PRIVATE KEY")
}

// With no live run left (no BENCH_ schema younger than the TTL), the reaper
// puts the warehouse back to XSMALL after a crashed run left it resized.
func TestSweepSnowflake_ResetsWarehouseOnlyWhenNoLiveRun(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	reset := "ALTER WAREHOUSE BENCH_WH SET WAREHOUSE_SIZE = XSMALL"

	sf := &fakeSnowflakeReaper{schemas: []snowflakeSchemaInfo{
		{Name: "BENCH_OLD", CreatedOn: now.Add(-5 * time.Hour)},
		{Name: "BENCHMARKS", CreatedOn: now}, // not a run schema
	}}
	_, err := Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", openerFor(sf, "BENCH_DB"))
	require.NoError(t, err)
	require.Equal(t, []string{"DROP SCHEMA IF EXISTS BENCH_DB.BENCH_OLD CASCADE", reset}, sf.sql)

	sf = &fakeSnowflakeReaper{}
	_, err = Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", openerFor(sf, "BENCH_DB"))
	require.NoError(t, err)
	require.Equal(t, []string{reset}, sf.sql, "no schemas at all: nothing is running")

	sf = &fakeSnowflakeReaper{schemas: []snowflakeSchemaInfo{
		{Name: "BENCH_OLD", CreatedOn: now.Add(-5 * time.Hour)},
		{Name: "BENCH_LIVE", CreatedOn: now.Add(-10 * time.Minute)},
	}}
	_, err = Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", openerFor(sf, "BENCH_DB"))
	require.NoError(t, err)
	require.NotContains(t, sf.sql, reset, "a live run keeps its warehouse size")

	sf = &fakeSnowflakeReaper{listErr: errors.New("boom")}
	_, err = Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", openerFor(sf, "BENCH_DB"))
	require.NoError(t, err)
	require.Empty(t, sf.sql, "an unknown schema set leaves the warehouse alone")

	bad := func(context.Context) (snowflakeReaperAPI, snowflakeCreds, error) {
		return sf, snowflakeCreds{Database: "BENCH_DB", Warehouse: "WH; DROP"}, nil
	}
	sf = &fakeSnowflakeReaper{}
	report, err := Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", bad)
	require.NoError(t, err)
	require.Equal(t, 1, report.Errors)
	require.Empty(t, sf.sql)
}
