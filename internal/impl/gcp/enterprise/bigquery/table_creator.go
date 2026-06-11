// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// yamlSchemaTypes maps the canonical YAML column types to BigQuery field
// types. The config parser normalises aliases (INT64→INTEGER etc.) before
// this mapping is consulted.
var yamlSchemaTypes = map[string]bigquery.FieldType{
	"STRING":     bigquery.StringFieldType,
	"BYTES":      bigquery.BytesFieldType,
	"INTEGER":    bigquery.IntegerFieldType,
	"FLOAT":      bigquery.FloatFieldType,
	"NUMERIC":    bigquery.NumericFieldType,
	"BIGNUMERIC": bigquery.BigNumericFieldType,
	"BOOLEAN":    bigquery.BooleanFieldType,
	"TIMESTAMP":  bigquery.TimestampFieldType,
	"DATE":       bigquery.DateFieldType,
	"TIME":       bigquery.TimeFieldType,
	"DATETIME":   bigquery.DateTimeFieldType,
	"GEOGRAPHY":  bigquery.GeographyFieldType,
	"JSON":       bigquery.JSONFieldType,
	"RECORD":     bigquery.RecordFieldType,
}

// yamlPartitioningTypes maps the YAML enum to BQ's TimePartitioningType. The
// parser validates the input enum, so missing entries here would be a bug.
var yamlPartitioningTypes = map[string]bigquery.TimePartitioningType{
	"DAY":   bigquery.DayPartitioningType,
	"HOUR":  bigquery.HourPartitioningType,
	"MONTH": bigquery.MonthPartitioningType,
	"YEAR":  bigquery.YearPartitioningType,
}

// yamlTimePartitioningToBQ returns the BigQuery time-partitioning struct, or
// nil if no partitioning was configured. The config parser leaves Type empty
// when the user didn't supply the `type` field (the explicit opt-in trigger).
// RequireFilter is intentionally not set here — the bigquery SDK marks the
// per-partitioning RequirePartitionFilter field as deprecated in favour of
// the top-level TableMetadata.RequirePartitionFilter, which tableCreator
// applies separately.
func yamlTimePartitioningToBQ(c bqTimePartitioningConfig) *bigquery.TimePartitioning {
	if c.Type == "" {
		return nil
	}
	return &bigquery.TimePartitioning{
		Type:       yamlPartitioningTypes[c.Type],
		Field:      c.Field,
		Expiration: c.Expiration,
	}
}

// yamlClusteringToBQ returns the BigQuery clustering struct, or nil if no
// clustering columns were configured.
func yamlClusteringToBQ(cols []string) *bigquery.Clustering {
	if len(cols) == 0 {
		return nil
	}
	return &bigquery.Clustering{Fields: cols}
}

// tableCreator builds bigquery.TableMetadata from the parsed config and
// creates BigQuery tables idempotently. It is invoked lazily by schemaResolver
// when Metadata returns 404 and auto_create_table is enabled, so it handles
// both static and interpolated table names.
type tableCreator struct {
	schema                 bigquery.Schema
	partitioning           *bigquery.TimePartitioning
	clustering             *bigquery.Clustering
	requirePartitionFilter bool
	primaryKeys            []string
	log                    *service.Logger
}

// newTableCreator builds a tableCreator from the parsed config. Returns nil
// when auto_create_table is disabled — callers nil-check to skip the create
// path entirely.
func newTableCreator(conf bqWriteAPIConfig, log *service.Logger) (*tableCreator, error) {
	if !conf.AutoCreateTable {
		return nil, nil
	}
	schema, err := yamlSchemaToBQSchema(conf.Schema)
	if err != nil {
		return nil, fmt.Errorf("building auto-create schema: %w", err)
	}
	return &tableCreator{
		schema:                 schema,
		partitioning:           yamlTimePartitioningToBQ(conf.TimePartitioning),
		clustering:             yamlClusteringToBQ(conf.Clustering),
		requirePartitionFilter: conf.TimePartitioning.RequireFilter,
		primaryKeys:            conf.PrimaryKeys,
		log:                    log,
	}, nil
}

// buildMetadata returns the bigquery.TableMetadata used by Ensure. Extracted
// from the inline literal so unit tests can exercise the PrimaryKey wiring
// without going through Create.
func (tc *tableCreator) buildMetadata() *bigquery.TableMetadata {
	meta := &bigquery.TableMetadata{
		Schema:                 tc.schema,
		TimePartitioning:       tc.partitioning,
		Clustering:             tc.clustering,
		RequirePartitionFilter: tc.requirePartitionFilter,
	}
	if len(tc.primaryKeys) > 0 {
		meta.TableConstraints = &bigquery.TableConstraints{
			PrimaryKey: &bigquery.PrimaryKey{Columns: append([]string{}, tc.primaryKeys...)},
		}
	}
	return meta
}

// Ensure creates the table if it does not exist. AlreadyExists (HTTP 409) is
// treated as success so concurrent creators race-tolerantly. Other errors
// propagate so the caller (schemaResolver) can classify and retry.
func (tc *tableCreator) Ensure(ctx context.Context, client *bigquery.Client, datasetID, tableID string) error {
	if err := client.Dataset(datasetID).Table(tableID).Create(ctx, tc.buildMetadata()); err != nil {
		// Extract googleapi diagnostics (HTTP status + GCP-side error details)
		// for a richer error message; fall back to the bare error otherwise.
		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) {
			if apiErr.Code == http.StatusConflict {
				// Another writer created the table first — that's the contract.
				return nil
			}
			return fmt.Errorf("creating table %s.%s: HTTP %d %s: %w",
				datasetID, tableID, apiErr.Code, apiErr.Message, err)
		}
		return fmt.Errorf("creating table %s.%s: %w", datasetID, tableID, err)
	}
	tc.log.Infof("Auto-created BigQuery table %s.%s", datasetID, tableID)
	return nil
}

// yamlSchemaToBQSchema converts the parsed YAML schema (a list of
// bqSchemaField) into a bigquery.Schema. The input is assumed to have already
// passed config-time validation, so canonical types and modes are guaranteed.
func yamlSchemaToBQSchema(fields []bqSchemaField) (bigquery.Schema, error) {
	out := make(bigquery.Schema, 0, len(fields))
	for _, f := range fields {
		bqType, ok := yamlSchemaTypes[f.Type]
		if !ok {
			return nil, fmt.Errorf("schema field %q: unsupported type %q", f.Name, f.Type)
		}
		fs := &bigquery.FieldSchema{
			Name:     f.Name,
			Type:     bqType,
			Required: f.Mode == "REQUIRED",
			Repeated: f.Mode == "REPEATED",
		}
		if bqType == bigquery.RecordFieldType {
			nested, err := yamlSchemaToBQSchema(f.Fields)
			if err != nil {
				return nil, err
			}
			fs.Schema = nested
		}
		out = append(out, fs)
	}
	return out, nil
}
