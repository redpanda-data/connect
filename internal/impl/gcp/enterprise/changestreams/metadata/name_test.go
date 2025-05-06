// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package metadata

import (
	"strings"
	"testing"
)

func TestRandomTableNamesRemovesHyphens(t *testing.T) {
	databaseID := "my-database-id-12345"

	names1 := RandomTableNames(databaseID)
	if strings.Contains(names1.TableName, "-") {
		t.Errorf("TableName should not contain hyphens: %s", names1.TableName)
	}
	if strings.Contains(names1.WatermarkIndexName, "-") {
		t.Errorf("WatermarkIndexName should not contain hyphens: %s", names1.WatermarkIndexName)
	}
	if strings.Contains(names1.CreatedAtIndexName, "-") {
		t.Errorf("CreatedAtIndexName should not contain hyphens: %s", names1.CreatedAtIndexName)
	}

	names2 := RandomTableNames(databaseID)
	if names1.TableName == names2.TableName {
		t.Error("Generated table names should be different")
	}
	if names1.WatermarkIndexName == names2.WatermarkIndexName {
		t.Error("Generated watermark index names should be different")
	}
	if names1.CreatedAtIndexName == names2.CreatedAtIndexName {
		t.Error("Generated createdAt index names should be different")
	}
}

func TestRandomTableNamesIsShorterThanMaxLength(t *testing.T) {
	// maxNameLength is the maximum length for table and index names in PostgreSQL (63 bytes)
	const maxNameLength = 63

	longDatabaseID := "my-database-id-larger-than-maximum-length-1234567890-1234567890-1234567890"
	names := RandomTableNames(longDatabaseID)

	if len(names.TableName) > maxNameLength {
		t.Errorf("TableName length should be <= %d, got %d", maxNameLength, len(names.TableName))
	}
	if len(names.WatermarkIndexName) > maxNameLength {
		t.Errorf("WatermarkIndexName length should be <= %d, got %d", maxNameLength, len(names.WatermarkIndexName))
	}
	if len(names.CreatedAtIndexName) > maxNameLength {
		t.Errorf("CreatedAtIndexName length should be <= %d, got %d", maxNameLength, len(names.CreatedAtIndexName))
	}

	shortDatabaseID := "d"
	names = RandomTableNames(shortDatabaseID)

	if len(names.TableName) > maxNameLength {
		t.Errorf("TableName length should be <= %d, got %d", maxNameLength, len(names.TableName))
	}
	if len(names.WatermarkIndexName) > maxNameLength {
		t.Errorf("WatermarkIndexName length should be <= %d, got %d", maxNameLength, len(names.WatermarkIndexName))
	}
	if len(names.CreatedAtIndexName) > maxNameLength {
		t.Errorf("CreatedAtIndexName length should be <= %d, got %d", maxNameLength, len(names.CreatedAtIndexName))
	}
}

func TestTableNamesFromExistingTable(t *testing.T) {
	databaseID := "databaseid"
	tableName := "mytable"

	names1 := TableNamesFromExistingTable(databaseID, tableName)
	if names1.TableName != tableName {
		t.Errorf("Expected table name to be %s, got %s", tableName, names1.TableName)
	}
	if strings.Contains(names1.WatermarkIndexName, "-") {
		t.Errorf("WatermarkIndexName should not contain hyphens: %s", names1.WatermarkIndexName)
	}
	if strings.Contains(names1.CreatedAtIndexName, "-") {
		t.Errorf("CreatedAtIndexName should not contain hyphens: %s", names1.CreatedAtIndexName)
	}

	names2 := TableNamesFromExistingTable(databaseID, tableName)
	if names2.TableName != tableName {
		t.Errorf("Expected table name to be %s, got %s", tableName, names2.TableName)
	}
	if names1.WatermarkIndexName == names2.WatermarkIndexName {
		t.Error("Generated watermark index names should be different")
	}
	if names1.CreatedAtIndexName == names2.CreatedAtIndexName {
		t.Error("Generated createdAt index names should be different")
	}
}
