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

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
)

// icebergTable is the table the output creates in the run's database.
const icebergTable = "bench_dst"

// GlueAPI is the slice of Glue the iceberg sink uses. *glue.Client satisfies
// it; tests fake it.
type GlueAPI interface {
	CreateDatabase(ctx context.Context, in *glue.CreateDatabaseInput, opts ...func(*glue.Options)) (*glue.CreateDatabaseOutput, error)
	DeleteDatabase(ctx context.Context, in *glue.DeleteDatabaseInput, opts ...func(*glue.Options)) (*glue.DeleteDatabaseOutput, error)
	GetTables(ctx context.Context, in *glue.GetTablesInput, opts ...func(*glue.Options)) (*glue.GetTablesOutput, error)
	DeleteTable(ctx context.Context, in *glue.DeleteTableInput, opts ...func(*glue.Options)) (*glue.DeleteTableOutput, error)
}

// newGlueClient is the orchestrator's real Glue client, on its own
// credentials (not the runner host's).
func newGlueClient(ctx context.Context, region string) (GlueAPI, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config for glue: %w", err)
	}
	return glue.NewFromConfig(cfg), nil
}

// icebergSink writes to a per-run Glue database, bench_<session>, through
// the Glue Iceberg REST catalog. Table data goes under iceberg/<session>/ in
// the session results bucket, which is force_destroy and goes with the shared
// stack, so Teardown removes only the catalog entries.
type icebergSink struct {
	glue      GlueAPI
	region    string
	accountID string
	bucket    string
}

func newIcebergSink(deps SinkDeps) (*icebergSink, error) {
	if deps.Glue == nil {
		return nil, errors.New("iceberg sink: no glue client")
	}
	if deps.Outs["aws_account_id"] == "" {
		return nil, errors.New("iceberg sink: terraform output aws_account_id is missing or empty")
	}
	if deps.Outs["results_bucket"] == "" {
		return nil, errors.New("iceberg sink: terraform output results_bucket is missing or empty")
	}
	return &icebergSink{
		glue:      deps.Glue,
		region:    deps.Region,
		accountID: deps.Outs["aws_account_id"],
		bucket:    deps.Outs["results_bucket"],
	}, nil
}

// icebergDatabase is the run's Glue database, which is the Iceberg namespace.
func icebergDatabase(session string) string { return "bench_" + scratchName(session) }

func isGlueNotFound(err error) bool {
	var nf *gluetypes.EntityNotFoundException
	return errors.As(err, &nf)
}

func (s *icebergSink) Provision(ctx context.Context, session string) (map[string]string, error) {
	db := icebergDatabase(session)
	_, err := s.glue.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String(db)},
	})
	var exists *gluetypes.AlreadyExistsException
	if err != nil && !errors.As(err, &exists) {
		return nil, fmt.Errorf("create glue database %s: %w", db, err)
	}
	return map[string]string{
		"BENCH_ICEBERG_CATALOG_URL":    fmt.Sprintf("https://glue.%s.amazonaws.com/iceberg", s.region),
		"BENCH_ICEBERG_WAREHOUSE":      s.accountID,
		"BENCH_ICEBERG_NAMESPACE":      db,
		"BENCH_ICEBERG_TABLE":          icebergTable,
		"BENCH_ICEBERG_BUCKET":         s.bucket,
		"BENCH_ICEBERG_TABLE_LOCATION": fmt.Sprintf("s3://%s/iceberg/%s/", s.bucket, session),
		"BENCH_AWS_REGION":             s.region,
	}, nil
}

// Count is unsupported: counting rows would mean reading the table's
// snapshots or running a query engine.
func (*icebergSink) Count(context.Context, string) (int64, error) { return 0, ErrCountUnsupported }

func (s *icebergSink) Teardown(ctx context.Context, session string) error {
	return deleteGlueDatabase(ctx, s.glue, icebergDatabase(session))
}

// deleteGlueDatabase deletes every table in db, then db. A database or table
// that is already gone counts as deleted.
func deleteGlueDatabase(ctx context.Context, g GlueAPI, db string) error {
	var tables []string
	var token *string
	for {
		out, err := g.GetTables(ctx, &glue.GetTablesInput{DatabaseName: aws.String(db), NextToken: token})
		if isGlueNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("list tables in glue database %s: %w", db, err)
		}
		for _, t := range out.TableList {
			tables = append(tables, aws.ToString(t.Name))
		}
		if out.NextToken == nil || *out.NextToken == "" {
			break
		}
		token = out.NextToken
	}
	for _, t := range tables {
		_, err := g.DeleteTable(ctx, &glue.DeleteTableInput{DatabaseName: aws.String(db), Name: aws.String(t)})
		if err != nil && !isGlueNotFound(err) {
			return fmt.Errorf("delete glue table %s.%s: %w", db, t, err)
		}
	}
	_, err := g.DeleteDatabase(ctx, &glue.DeleteDatabaseInput{Name: aws.String(db)})
	if err != nil && !isGlueNotFound(err) {
		return fmt.Errorf("delete glue database %s: %w", db, err)
	}
	return nil
}
