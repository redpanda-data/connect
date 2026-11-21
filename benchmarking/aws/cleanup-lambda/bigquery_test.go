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
	"encoding/json"
	"errors"
	"testing"
	"time"

	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// fakeBigQueryReaper lists canned datasets and records every deletion.
type fakeBigQueryReaper struct {
	datasets  []bigQueryDatasetInfo
	listProj  string
	listErr   error
	deleted   []string
	deleteErr map[string]error
	closed    bool
}

func (f *fakeBigQueryReaper) ListDatasets(_ context.Context, project, prefix string) ([]bigQueryDatasetInfo, error) {
	f.listProj = project
	return f.datasets, f.listErr
}

func (f *fakeBigQueryReaper) DeleteDataset(_ context.Context, project, id string) error {
	f.deleted = append(f.deleted, project+"."+id)
	return f.deleteErr[id]
}

func (f *fakeBigQueryReaper) Close() error { f.closed = true; return nil }

func bqOpenerFor(f *fakeBigQueryReaper, project string) bigQueryOpener {
	return func(context.Context) (bigQueryReaperAPI, string, error) { return f, project, nil }
}

func TestSweepBigQuery_SelectsBenchPrefixOlderThanTTL(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	old, young := now.Add(-5*time.Hour), now.Add(-30*time.Minute)
	bq := &fakeBigQueryReaper{datasets: []bigQueryDatasetInfo{
		{ID: "bench_bench_20261120_060000", Created: old},
		{ID: "bench_bench_20261120_113000", Created: young},
		{ID: "benchmarks", Created: old},   // no bench_ prefix
		{ID: "prod_data", Created: old},    // never touched
		{ID: "bench_X;DROP", Created: old}, // not a plain identifier
		{ID: "bench_bench_20261120_060001", Created: now.Add(-4*time.Hour - time.Minute)},
	}}
	report, err := Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", nil, bqOpenerFor(bq, "bench-proj"))
	require.NoError(t, err)
	require.Equal(t, "bench-proj", bq.listProj)
	require.Equal(t, []string{"bench-proj.bench_bench_20261120_060000", "bench-proj.bench_bench_20261120_060001"}, bq.deleted)
	require.Equal(t, []string{
		"bigquery-dataset:bench-proj.bench_bench_20261120_060000",
		"bigquery-dataset:bench-proj.bench_bench_20261120_060001",
	}, report.Destroyed)
	require.True(t, bq.closed)
}

func TestSweepBigQuery_FailuresAreReportedNotFatal(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	bq := &fakeBigQueryReaper{
		datasets:  []bigQueryDatasetInfo{{ID: "bench_a", Created: now.Add(-5 * time.Hour)}},
		deleteErr: map[string]error{"bench_a": errors.New("permission denied")},
	}
	report, err := Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", nil, bqOpenerFor(bq, "p"))
	require.NoError(t, err)
	require.Equal(t, 1, report.Errors)
	require.Contains(t, report.Failed[0], "bigquery-dataset:p.bench_a")

	bq = &fakeBigQueryReaper{listErr: errors.New("quota")}
	report, err = Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", nil, bqOpenerFor(bq, "p"))
	require.NoError(t, err)
	require.Equal(t, 1, report.Errors)
	require.Contains(t, report.Failed[0], "bigquery-datasets")

	failOpen := func(context.Context) (bigQueryReaperAPI, string, error) { return nil, "", errors.New("auth failed") }
	report, err = Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", nil, failOpen)
	require.NoError(t, err)
	require.Equal(t, 1, report.Errors)

	bq = &fakeBigQueryReaper{}
	report, err = Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", nil, bqOpenerFor(bq, "p`x"))
	require.NoError(t, err)
	require.Equal(t, 1, report.Errors, "a bad project name is refused before any call")
	require.Empty(t, bq.deleted)
}

// Before the operator creates the secret (or when the lambda cannot read
// it), the BigQuery sweep is skipped without an error.
func TestSweepBigQuery_SkippedWhenSecretMissing(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	for name, gerr := range map[string]error{
		"not found":     &smtypes.ResourceNotFoundException{Message: strPtr("not found")},
		"access denied": &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized"},
	} {
		open := newBigQueryOpener(&fakeSecrets{err: gerr}, "arn:aws:secretsmanager:us-east-2:1:secret:redpanda-connect-bench/bigquery", nil)
		_, _, err := open(t.Context())
		require.ErrorIs(t, err, errBigQuerySecretMissing, name)

		report, err := Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", nil, open)
		require.NoError(t, err, name)
		require.Zero(t, report.Errors, name)
		require.Empty(t, report.Failed, name)
	}
	open := newBigQueryOpener(&fakeSecrets{err: errors.New("throttled")}, "sid", nil)
	_, _, err := open(t.Context())
	require.Error(t, err)
	require.NotErrorIs(t, err, errBigQuerySecretMissing)

	report, err := Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", nil, nil)
	require.NoError(t, err)
	require.Zero(t, report.Errors)
}

func TestBigQueryOpenerReadsSecretWithoutLeaking(t *testing.T) {
	const keyJSON = `{"type":"service_account","private_key":"-----BEGIN PRIVATE KEY-----\nZmFrZQ==\n-----END PRIVATE KEY-----\n","private_key_id":"pkid-1234abcd"}`
	sm := &fakeSecrets{value: `{"project":"bench-proj","credentials_json":` + quoteJSON(keyJSON) + `}`}
	var gotProject string
	var gotKey []byte
	fake := &fakeBigQueryReaper{}
	open := newBigQueryOpener(sm, "sid", func(_ context.Context, project string, creds []byte) (bigQueryReaperAPI, error) {
		gotProject, gotKey = project, creds
		return fake, nil
	})
	api, project, err := open(t.Context())
	require.NoError(t, err)
	require.Same(t, fake, api)
	require.Equal(t, "bench-proj", project)
	require.Equal(t, []string{"sid"}, sm.ids)
	require.JSONEq(t, keyJSON, string(gotKey))
	require.Equal(t, "bench-proj", gotProject)

	for name, v := range map[string]string{
		"not json":   "{" + keyJSON,
		"no project": `{"credentials_json":` + quoteJSON(keyJSON) + `}`,
		"no creds":   `{"project":"bench-proj"}`,
		"not a key":  `{"project":"bench-proj","credentials_json":"pkid-1234abcd"}`,
	} {
		sm.value = v
		_, _, err := open(t.Context())
		require.Error(t, err, name)
		require.NotContains(t, err.Error(), "pkid-1234abcd", name)
		require.NotContains(t, err.Error(), "PRIVATE KEY", name)
	}
}

func quoteJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// A dataset whose metadata gave no creation time is skipped, never treated
// as infinitely old: the zero time would otherwise delete a live run's
// dataset.
func TestSweepBigQuery_SkipsZeroCreationTime(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	bq := &fakeBigQueryReaper{datasets: []bigQueryDatasetInfo{
		{ID: "bench_bench_20261120_115900"},
		{ID: "bench_bench_20261120_060000", Created: now.Add(-5 * time.Hour)},
	}}
	report, err := Sweep(t.Context(), &FakeAWS{}, now, 4*time.Hour, "arn:sns:topic", nil, bqOpenerFor(bq, "p"))
	require.NoError(t, err)
	require.Equal(t, []string{"p.bench_bench_20261120_060000"}, bq.deleted)
	require.Zero(t, report.Errors)
}
