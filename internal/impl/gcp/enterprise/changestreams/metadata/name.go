// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package metadata

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const (
	tableNameFormat              = "Metadata_%s_%s"
	watermarkIndexFormat         = "WatermarkIdx_%s_%s"
	metadataCreatedAtIndexFormat = "CreatedAtIdx_%s_%s"
)

func genName(template, databaseID string, id uuid.UUID) string {
	// maxNameLength is the maximum length for table and index names in PostgreSQL (63 bytes)
	const maxNameLength = 63

	name := fmt.Sprintf(template, databaseID, id)
	name = strings.ReplaceAll(name, "-", "_")
	if len(name) > maxNameLength {
		return name[:maxNameLength]
	}
	return name
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
	id := uuid.New()
	return TableNames{
		TableName:          genName(tableNameFormat, databaseID, id),
		WatermarkIndexName: genName(watermarkIndexFormat, databaseID, id),
		CreatedAtIndexName: genName(metadataCreatedAtIndexFormat, databaseID, id),
	}
}

// TableNamesFromExistingTable encapsulates a selected table name.
// Index names are generated, but will only be used if the given table does not exist.
// The watermark index will be in the form of "WatermarkIdx_<databaseId>_<uuid>".
// The createdAt / start timestamp index will be in the form of "CreatedAtIdx_<databaseId>_<uuid>".
func TableNamesFromExistingTable(databaseID, table string) TableNames {
	id := uuid.New()
	return TableNames{
		TableName:          table,
		WatermarkIndexName: genName(watermarkIndexFormat, databaseID, id),
		CreatedAtIndexName: genName(metadataCreatedAtIndexFormat, databaseID, id),
	}
}
