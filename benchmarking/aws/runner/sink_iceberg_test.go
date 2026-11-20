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
	"errors"
	"fmt"
	"maps"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/plan"
)

const (
	iceTestDB = "bench_bench_20261120_101010"
)

// fakeGlue is an in-memory Glue: databases with tables, a call log in call
// order, and per-call errors keyed "<Method>:<name>".
type fakeGlue struct {
	tables map[string][]string // database -> tables
	calls  []string
	errs   map[string]error
	// pageSize, when set, serves GetTables in pages of that size.
	pageSize int
}

func (f *fakeGlue) log(s string) { f.calls = append(f.calls, s) }

func (f *fakeGlue) CreateDatabase(_ context.Context, in *glue.CreateDatabaseInput, _ ...func(*glue.Options)) (*glue.CreateDatabaseOutput, error) {
	name := *in.DatabaseInput.Name
	f.log("CreateDatabase:" + name)
	if err := f.errs["CreateDatabase:"+name]; err != nil {
		return nil, err
	}
	if f.tables == nil {
		f.tables = map[string][]string{}
	}
	f.tables[name] = nil
	return &glue.CreateDatabaseOutput{}, nil
}

func (f *fakeGlue) DeleteDatabase(_ context.Context, in *glue.DeleteDatabaseInput, _ ...func(*glue.Options)) (*glue.DeleteDatabaseOutput, error) {
	f.log("DeleteDatabase:" + *in.Name)
	if err := f.errs["DeleteDatabase:"+*in.Name]; err != nil {
		return nil, err
	}
	if _, ok := f.tables[*in.Name]; !ok {
		return nil, &gluetypes.EntityNotFoundException{Message: aws.String("no db")}
	}
	delete(f.tables, *in.Name)
	return &glue.DeleteDatabaseOutput{}, nil
}

func (f *fakeGlue) GetTables(_ context.Context, in *glue.GetTablesInput, _ ...func(*glue.Options)) (*glue.GetTablesOutput, error) {
	f.log("GetTables:" + *in.DatabaseName)
	if err := f.errs["GetTables:"+*in.DatabaseName]; err != nil {
		return nil, err
	}
	names, ok := f.tables[*in.DatabaseName]
	if !ok {
		return nil, &gluetypes.EntityNotFoundException{Message: aws.String("no db")}
	}
	start := 0
	if in.NextToken != nil {
		fmt.Sscanf(*in.NextToken, "%d", &start)
	}
	end := len(names)
	var next *string
	if f.pageSize > 0 && start+f.pageSize < end {
		end = start + f.pageSize
		next = aws.String(fmt.Sprint(end))
	}
	out := &glue.GetTablesOutput{NextToken: next}
	for _, n := range names[start:end] {
		out.TableList = append(out.TableList, gluetypes.Table{Name: aws.String(n)})
	}
	return out, nil
}

func (f *fakeGlue) DeleteTable(_ context.Context, in *glue.DeleteTableInput, _ ...func(*glue.Options)) (*glue.DeleteTableOutput, error) {
	f.log("DeleteTable:" + *in.DatabaseName + "." + *in.Name)
	if err := f.errs["DeleteTable:"+*in.Name]; err != nil {
		return nil, err
	}
	tbls := f.tables[*in.DatabaseName]
	for i, n := range tbls {
		if n == *in.Name {
			f.tables[*in.DatabaseName] = append(tbls[:i:i], tbls[i+1:]...)
			return &glue.DeleteTableOutput{}, nil
		}
	}
	return nil, &gluetypes.EntityNotFoundException{Message: aws.String("no table")}
}

func iceTestDeps(g GlueAPI) SinkDeps {
	d := sinkTestDeps(&fakeRPK{})
	d.Outs["aws_account_id"] = "123456789012"
	d.Glue = g
	return d
}

func TestSinkIcebergProvisionCreatesDatabaseAndReturnsPlaceholders(t *testing.T) {
	g := &fakeGlue{}
	sink, err := sinkFor(SinkSpec{Kind: "iceberg"}, iceTestDeps(g))
	require.NoError(t, err)
	ph, err := sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
	require.Equal(t, []string{"CreateDatabase:" + iceTestDB}, g.calls)
	require.Equal(t, map[string]string{
		"BENCH_ICEBERG_CATALOG_URL":    "https://glue.us-east-2.amazonaws.com/iceberg",
		"BENCH_ICEBERG_WAREHOUSE":      "123456789012",
		"BENCH_ICEBERG_NAMESPACE":      iceTestDB,
		"BENCH_ICEBERG_TABLE":          "bench_dst",
		"BENCH_ICEBERG_BUCKET":         "bench-results-abc",
		"BENCH_ICEBERG_TABLE_LOCATION": "s3://bench-results-abc/iceberg/" + sinkTestSession + "/",
		"BENCH_AWS_REGION":             "us-east-2",
	}, ph)
	_, err = sink.Count(t.Context(), "p")
	require.ErrorIs(t, err, ErrCountUnsupported)
}

func TestSinkIcebergProvisionAlreadyExistsIsFine(t *testing.T) {
	g := &fakeGlue{errs: map[string]error{"CreateDatabase:" + iceTestDB: &gluetypes.AlreadyExistsException{}}}
	sink, err := sinkFor(SinkSpec{Kind: "iceberg"}, iceTestDeps(g))
	require.NoError(t, err)
	_, err = sink.Provision(t.Context(), sinkTestSession)
	require.NoError(t, err)
}

func TestSinkIcebergProvisionError(t *testing.T) {
	g := &fakeGlue{errs: map[string]error{"CreateDatabase:" + iceTestDB: errors.New("denied")}}
	sink, err := sinkFor(SinkSpec{Kind: "iceberg"}, iceTestDeps(g))
	require.NoError(t, err)
	_, err = sink.Provision(t.Context(), sinkTestSession)
	require.ErrorContains(t, err, "denied")
}

func TestSinkIcebergNeedsDeps(t *testing.T) {
	d := iceTestDeps(&fakeGlue{})
	delete(d.Outs, "aws_account_id")
	_, err := sinkFor(SinkSpec{Kind: "iceberg"}, d)
	require.ErrorContains(t, err, "aws_account_id")

	d = iceTestDeps(&fakeGlue{})
	delete(d.Outs, "results_bucket")
	_, err = sinkFor(SinkSpec{Kind: "iceberg"}, d)
	require.ErrorContains(t, err, "results_bucket")

	d = iceTestDeps(nil)
	_, err = sinkFor(SinkSpec{Kind: "iceberg"}, d)
	require.ErrorContains(t, err, "glue")
}

func TestSinkIcebergTeardownDeletesTablesThenDatabase(t *testing.T) {
	g := &fakeGlue{tables: map[string][]string{iceTestDB: {"t1", "t2", "t3"}}, pageSize: 2}
	sink, err := sinkFor(SinkSpec{Kind: "iceberg"}, iceTestDeps(g))
	require.NoError(t, err)
	require.NoError(t, sink.Teardown(t.Context(), sinkTestSession))
	require.Equal(t, []string{
		"GetTables:" + iceTestDB, "GetTables:" + iceTestDB,
		"DeleteTable:" + iceTestDB + ".t1", "DeleteTable:" + iceTestDB + ".t2", "DeleteTable:" + iceTestDB + ".t3",
		"DeleteDatabase:" + iceTestDB,
	}, g.calls)
	require.Empty(t, g.tables)
}

func TestSinkIcebergTeardownIdempotent(t *testing.T) {
	g := &fakeGlue{}
	sink, err := sinkFor(SinkSpec{Kind: "iceberg"}, iceTestDeps(g))
	require.NoError(t, err)
	require.NoError(t, sink.Teardown(t.Context(), sinkTestSession), "an absent database is already torn down")
	require.NoError(t, sink.Teardown(t.Context(), sinkTestSession))

	// A table that vanishes between list and delete is also fine.
	g = &fakeGlue{
		tables: map[string][]string{iceTestDB: {"t1"}},
		errs:   map[string]error{"DeleteTable:t1": &gluetypes.EntityNotFoundException{}},
	}
	sink, err = sinkFor(SinkSpec{Kind: "iceberg"}, iceTestDeps(g))
	require.NoError(t, err)
	require.NoError(t, sink.Teardown(t.Context(), sinkTestSession))
}

func TestSinkIcebergTeardownSurfacesErrors(t *testing.T) {
	g := &fakeGlue{tables: map[string][]string{iceTestDB: {"t1"}}, errs: map[string]error{"DeleteTable:t1": errors.New("access denied")}}
	sink, err := sinkFor(SinkSpec{Kind: "iceberg"}, iceTestDeps(g))
	require.NoError(t, err)
	err = sink.Teardown(t.Context(), sinkTestSession)
	require.ErrorContains(t, err, "access denied")
	require.NotContains(t, g.calls, "DeleteDatabase:"+iceTestDB, "the database stays when a table could not go")

	g = &fakeGlue{tables: map[string][]string{iceTestDB: nil}, errs: map[string]error{"DeleteDatabase:" + iceTestDB: errors.New("throttled")}}
	sink, err = sinkFor(SinkSpec{Kind: "iceberg"}, iceTestDeps(g))
	require.NoError(t, err)
	require.ErrorContains(t, sink.Teardown(t.Context(), sinkTestSession), "throttled")
}

// An e2e iceberg plan renders fully resolved once Provision's placeholders
// are merged in, and not before.
func TestSinkIcebergRendersResolved(t *testing.T) {
	cfg := map[string]any{
		"input":  map[string]any{"redpanda": map[string]any{}},
		"output": map[string]any{"iceberg": map[string]any{"catalog": map[string]any{"url": "https://x"}, "namespace": "n", "table": "t"}},
	}
	swapped, _, err := plan.SwapIdentity(cfg, plan.SourceRedpanda, plan.SinkIceberg)
	require.NoError(t, err)
	s := &Scenario{
		Direction: DirectionE2E,
		Connector: "redpanda",
		Pipeline:  swapped,
		Sink:      &SinkSpec{Kind: "iceberg"},
		Matrix:    MatrixSpec{CPUPoints: []int{1}},
	}
	outs := map[string]string{"results_bucket": "bench-results-abc", "bench_session_id": sinkTestSession}
	names := newBenchNames(sinkTestSession, s.Connector)
	pt := buildSweepPlan(s)[0]

	set, err := renderPointConfigs(s, outs, e2eTopology{}, names, pt)
	require.NoError(t, err)
	require.ErrorContains(t, checkPlaceholdersResolved([]renderedPointConfigs{set}), "${BENCH_ICEBERG_")

	sink, err := sinkFor(*s.Sink, iceTestDeps(&fakeGlue{}))
	require.NoError(t, err)
	merged := maps.Clone(outs)
	require.NoError(t, withSink(t.Context(), sink, sinkTestSession, func(ph map[string]string) error {
		maps.Copy(merged, ph)
		set, err = renderPointConfigs(s, merged, e2eTopology{}, names, pt)
		return err
	}))
	require.NoError(t, checkPlaceholdersResolved([]renderedPointConfigs{set}))
	raw, err := os.ReadFile(set.Single)
	require.NoError(t, err)
	require.Contains(t, string(raw), "https://glue.us-east-2.amazonaws.com/iceberg")
	require.Contains(t, string(raw), "s3://bench-results-abc/iceberg/"+sinkTestSession+"/")
}
