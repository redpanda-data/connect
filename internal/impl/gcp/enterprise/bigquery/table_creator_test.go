// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestYAMLSchemaToBQSchema(t *testing.T) {
	in := []bqSchemaField{
		{Name: "id", Type: "STRING", Mode: "REQUIRED"},
		{Name: "tags", Type: "STRING", Mode: "REPEATED"},
		{Name: "age", Type: "INTEGER", Mode: "NULLABLE"},
		{Name: "address", Type: "RECORD", Mode: "NULLABLE", Fields: []bqSchemaField{
			{Name: "line1", Type: "STRING", Mode: "NULLABLE"},
			{Name: "city", Type: "STRING", Mode: "REQUIRED"},
		}},
	}

	got, err := yamlSchemaToBQSchema(in)
	require.NoError(t, err)
	require.Len(t, got, 4)

	assert.Equal(t, "id", got[0].Name)
	assert.Equal(t, bigquery.StringFieldType, got[0].Type)
	assert.True(t, got[0].Required)
	assert.False(t, got[0].Repeated)

	assert.True(t, got[1].Repeated, "REPEATED maps to Repeated=true")
	assert.False(t, got[1].Required, "REPEATED implies Required=false")

	assert.Equal(t, bigquery.IntegerFieldType, got[2].Type)

	assert.Equal(t, bigquery.RecordFieldType, got[3].Type)
	require.Len(t, got[3].Schema, 2)
	assert.Equal(t, "city", got[3].Schema[1].Name)
	assert.True(t, got[3].Schema[1].Required)
}

func TestYAMLTimePartitioningToBQ(t *testing.T) {
	in := bqTimePartitioningConfig{Type: "HOUR", Field: "created_at", Expiration: 24 * time.Hour, RequireFilter: true}
	got := yamlTimePartitioningToBQ(in)
	require.NotNil(t, got)
	assert.Equal(t, bigquery.HourPartitioningType, got.Type)
	assert.Equal(t, "created_at", got.Field)
	assert.Equal(t, 24*time.Hour, got.Expiration)
}

func TestYAMLTimePartitioningEmpty(t *testing.T) {
	// Zero-value config means no partitioning at all (Type empty).
	assert.Nil(t, yamlTimePartitioningToBQ(bqTimePartitioningConfig{}))
}

func TestYAMLTimePartitioningIngestionTime(t *testing.T) {
	// Type set but field empty = ingestion-time partitioning.
	got := yamlTimePartitioningToBQ(bqTimePartitioningConfig{Type: "DAY"})
	require.NotNil(t, got)
	assert.Equal(t, bigquery.DayPartitioningType, got.Type)
	assert.Empty(t, got.Field)
}

func TestYAMLClusteringToBQ(t *testing.T) {
	got := yamlClusteringToBQ([]string{"user_id", "tenant_id"})
	require.NotNil(t, got)
	assert.Equal(t, []string{"user_id", "tenant_id"}, got.Fields)

	assert.Nil(t, yamlClusteringToBQ(nil), "empty list means no clustering")
}

func TestYAMLSchemaTypeMapping(t *testing.T) {
	cases := []struct {
		in   string
		want bigquery.FieldType
	}{
		{"STRING", bigquery.StringFieldType},
		{"BYTES", bigquery.BytesFieldType},
		{"INTEGER", bigquery.IntegerFieldType},
		{"FLOAT", bigquery.FloatFieldType},
		{"NUMERIC", bigquery.NumericFieldType},
		{"BIGNUMERIC", bigquery.BigNumericFieldType},
		{"BOOLEAN", bigquery.BooleanFieldType},
		{"TIMESTAMP", bigquery.TimestampFieldType},
		{"DATE", bigquery.DateFieldType},
		{"TIME", bigquery.TimeFieldType},
		{"DATETIME", bigquery.DateTimeFieldType},
		{"GEOGRAPHY", bigquery.GeographyFieldType},
		{"JSON", bigquery.JSONFieldType},
		{"RECORD", bigquery.RecordFieldType},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			in := []bqSchemaField{{Name: "x", Type: c.in, Mode: "NULLABLE"}}
			if c.in == "RECORD" {
				in[0].Fields = []bqSchemaField{{Name: "y", Type: "STRING", Mode: "NULLABLE"}}
			}
			got, err := yamlSchemaToBQSchema(in)
			require.NoError(t, err)
			assert.Equal(t, c.want, got[0].Type)
		})
	}
}

func TestTableCreatorBuildsPrimaryKey(t *testing.T) {
	creator := &tableCreator{
		schema: bigquery.Schema{
			{Name: "id", Type: bigquery.StringFieldType, Required: true},
		},
		primaryKeys: []string{"id"},
	}
	meta := creator.buildMetadata()
	require.NotNil(t, meta.TableConstraints)
	require.NotNil(t, meta.TableConstraints.PrimaryKey)
	assert.Equal(t, []string{"id"}, meta.TableConstraints.PrimaryKey.Columns)
}

func TestTableCreatorNoPrimaryKey(t *testing.T) {
	creator := &tableCreator{
		schema: bigquery.Schema{
			{Name: "id", Type: bigquery.StringFieldType, Required: true},
		},
	}
	meta := creator.buildMetadata()
	assert.Nil(t, meta.TableConstraints)
}
