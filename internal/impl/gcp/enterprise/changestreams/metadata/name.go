// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package metadata

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const (
	tableNameFormat              = "Metadata_%s_%s"
	watermarkIndexFormat         = "WatermarkIdx_%s_%s"
	metadataCreatedAtIndexFormat = "CreatedAtIdx_%s_%s"
)

func genName(template, databaseID, id string) string {
	// maxNameLength is the maximum length for table and index names in PostgreSQL (63 bytes)
	const maxNameLength = 63

	name := fmt.Sprintf(template, databaseID, id)
	name = strings.ReplaceAll(name, "-", "_")
	if len(name) > maxNameLength {
		return name[:maxNameLength]
	}
	return name
}

// deterministicSuffix derives a stable identifier from the database and
// table name so that repeated calls for the same table produce identical
// index names.
//
// This is required for the CREATE INDEX IF NOT EXISTS statements in
// CreatePartitionMetadataTableWithDatabaseAdminClient to actually be
// idempotent across connector restarts: that DDL runs on every startup, and
// a random per-call suffix (e.g. uuid.New()) would generate a distinct,
// never-matching index name each time, silently adding two new indexes to
// the metadata table on every restart until Spanner's per-table index limit
// (128) is hit.
func deterministicSuffix(databaseID, table string) string {
	sum := sha256.Sum256([]byte(databaseID + "/" + table))
	return hex.EncodeToString(sum[:8])
}

// TableNames specifies table and index names to be used for metadata storage.
type TableNames struct {
	TableName          string
	WatermarkIndexName string
	CreatedAtIndexName string
}

// RandomTableNames generates a unique name for the partition metadata table and its indexes.
// The table name will be in the form of "Metadata_<databaseId>_<uuid>".
// The watermark index will be in the form of "WatermarkIdx_<databaseId>_<uuid>".
// The createdAt / start timestamp index will be in the form of "CreatedAtIdx_<databaseId>_<uuid>".
func RandomTableNames(databaseID string) TableNames {
	id := uuid.New().String()
	return TableNames{
		TableName:          genName(tableNameFormat, databaseID, id),
		WatermarkIndexName: genName(watermarkIndexFormat, databaseID, id),
		CreatedAtIndexName: genName(metadataCreatedAtIndexFormat, databaseID, id),
	}
}

// TableNamesFromExistingTable encapsulates a selected table name.
// Index names are derived deterministically from the database and table
// name (see deterministicSuffix), so repeated calls -- e.g. on every
// connector restart -- produce the same names, and the CREATE INDEX IF NOT
// EXISTS statements issued at setup are true no-ops once the indexes exist.
// The watermark index will be in the form of "WatermarkIdx_<databaseId>_<suffix>".
// The createdAt / start timestamp index will be in the form of "CreatedAtIdx_<databaseId>_<suffix>".
func TableNamesFromExistingTable(databaseID, table string) TableNames {
	id := deterministicSuffix(databaseID, table)
	return TableNames{
		TableName:          table,
		WatermarkIndexName: genName(watermarkIndexFormat, databaseID, id),
		CreatedAtIndexName: genName(metadataCreatedAtIndexFormat, databaseID, id),
	}
}
