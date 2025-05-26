// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomTableNamesRemovesHyphens(t *testing.T) {
	databaseID := "my-database-id-12345"

	names1 := RandomTableNames(databaseID)
	assert.NotContains(t, names1.TableName, "-")
	assert.NotContains(t, names1.WatermarkIndexName, "-")
	assert.NotContains(t, names1.CreatedAtIndexName, "-")

	names2 := RandomTableNames(databaseID)
	assert.NotEqual(t, names1.TableName, names2.TableName)
	assert.NotEqual(t, names1.WatermarkIndexName, names2.WatermarkIndexName)
	assert.NotEqual(t, names1.CreatedAtIndexName, names2.CreatedAtIndexName)
}

func TestRandomTableNamesIsShorterThanMaxLength(t *testing.T) {
	// maxNameLength is the maximum length for table and index names in PostgreSQL (63 bytes)
	const maxNameLength = 63

	longDatabaseID := "my-database-id-larger-than-maximum-length-1234567890-1234567890-1234567890"
	names := RandomTableNames(longDatabaseID)
	assert.LessOrEqual(t, len(names.TableName), maxNameLength)
	assert.LessOrEqual(t, len(names.WatermarkIndexName), maxNameLength)
	assert.LessOrEqual(t, len(names.CreatedAtIndexName), maxNameLength)

	shortDatabaseID := "d"
	names = RandomTableNames(shortDatabaseID)
	assert.LessOrEqual(t, len(names.TableName), maxNameLength)
	assert.LessOrEqual(t, len(names.WatermarkIndexName), maxNameLength)
	assert.LessOrEqual(t, len(names.CreatedAtIndexName), maxNameLength)
}

func TestTableNamesFromExistingTable(t *testing.T) {
	databaseID := "databaseid"
	tableName := "mytable"

	names1 := TableNamesFromExistingTable(databaseID, tableName)
	assert.Equal(t, tableName, names1.TableName)
	assert.NotContains(t, names1.WatermarkIndexName, "-")
	assert.NotContains(t, names1.CreatedAtIndexName, "-")

	names2 := TableNamesFromExistingTable(databaseID, tableName)
	assert.Equal(t, tableName, names2.TableName)
	assert.NotEqual(t, names1.WatermarkIndexName, names2.WatermarkIndexName)
	assert.NotEqual(t, names1.CreatedAtIndexName, names2.CreatedAtIndexName)
}
