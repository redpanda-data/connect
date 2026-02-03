// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"testing"

	gomysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMysqlColumnToCommon(t *testing.T) {
	tests := []struct {
		name           string
		col            gomysqlschema.TableColumn
		expectedType   schema.CommonType
		expectedName   string
		hasChildren    bool
		expectedError  bool
	}{
		{
			name: "integer column",
			col: gomysqlschema.TableColumn{
				Name:    "id",
				Type:    gomysqlschema.TYPE_NUMBER,
				RawType: "bigint",
			},
			expectedType: schema.Int64,
			expectedName: "id",
			hasChildren:  false,
		},
		{
			name: "medium int column",
			col: gomysqlschema.TableColumn{
				Name:    "count",
				Type:    gomysqlschema.TYPE_MEDIUM_INT,
				RawType: "mediumint",
			},
			expectedType: schema.Int32,
			expectedName: "count",
			hasChildren:  false,
		},
		{
			name: "float column",
			col: gomysqlschema.TableColumn{
				Name:    "price",
				Type:    gomysqlschema.TYPE_FLOAT,
				RawType: "double",
			},
			expectedType: schema.Float64,
			expectedName: "price",
			hasChildren:  false,
		},
		{
			name: "decimal column",
			col: gomysqlschema.TableColumn{
				Name:    "balance",
				Type:    gomysqlschema.TYPE_DECIMAL,
				RawType: "decimal(10,2)",
			},
			expectedType: schema.String,
			expectedName: "balance",
			hasChildren:  false,
		},
		{
			name: "string column",
			col: gomysqlschema.TableColumn{
				Name:    "name",
				Type:    gomysqlschema.TYPE_STRING,
				RawType: "varchar(255)",
			},
			expectedType: schema.String,
			expectedName: "name",
			hasChildren:  false,
		},
		{
			name: "timestamp column",
			col: gomysqlschema.TableColumn{
				Name:    "created_at",
				Type:    gomysqlschema.TYPE_TIMESTAMP,
				RawType: "timestamp",
			},
			expectedType: schema.Timestamp,
			expectedName: "created_at",
			hasChildren:  false,
		},
		{
			name: "datetime column",
			col: gomysqlschema.TableColumn{
				Name:    "updated_at",
				Type:    gomysqlschema.TYPE_DATETIME,
				RawType: "datetime",
			},
			expectedType: schema.Timestamp,
			expectedName: "updated_at",
			hasChildren:  false,
		},
		{
			name: "binary column",
			col: gomysqlschema.TableColumn{
				Name:    "data",
				Type:    gomysqlschema.TYPE_BINARY,
				RawType: "blob",
			},
			expectedType: schema.ByteArray,
			expectedName: "data",
			hasChildren:  false,
		},
		{
			name: "enum column",
			col: gomysqlschema.TableColumn{
				Name:       "status",
				Type:       gomysqlschema.TYPE_ENUM,
				RawType:    "enum('active','inactive')",
				EnumValues: []string{"active", "inactive"},
			},
			expectedType: schema.String,
			expectedName: "status",
			hasChildren:  false,
		},
		{
			name: "set column",
			col: gomysqlschema.TableColumn{
				Name:      "flags",
				Type:      gomysqlschema.TYPE_SET,
				RawType:   "set('read','write','execute')",
				SetValues: []string{"read", "write", "execute"},
			},
			expectedType: schema.Array,
			expectedName: "flags",
			hasChildren:  true,
		},
		{
			name: "json column",
			col: gomysqlschema.TableColumn{
				Name:    "metadata",
				Type:    gomysqlschema.TYPE_JSON,
				RawType: "json",
			},
			expectedType: schema.String,
			expectedName: "metadata",
			hasChildren:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mysqlColumnToCommon(tt.col)

			if tt.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedName, result.Name)
			assert.Equal(t, tt.expectedType, result.Type)
			assert.True(t, result.Optional, "all columns should be optional by default")

			if tt.hasChildren {
				assert.NotEmpty(t, result.Children)
			} else {
				assert.Empty(t, result.Children)
			}
		})
	}
}

func TestMysqlTableToCommonSchema(t *testing.T) {
	table := &gomysqlschema.Table{
		Schema: "testdb",
		Name:   "users",
		Columns: []gomysqlschema.TableColumn{
			{
				Name:    "id",
				Type:    gomysqlschema.TYPE_NUMBER,
				RawType: "bigint",
			},
			{
				Name:    "name",
				Type:    gomysqlschema.TYPE_STRING,
				RawType: "varchar(255)",
			},
			{
				Name:    "email",
				Type:    gomysqlschema.TYPE_STRING,
				RawType: "varchar(255)",
			},
			{
				Name:    "created_at",
				Type:    gomysqlschema.TYPE_TIMESTAMP,
				RawType: "timestamp",
			},
		},
	}

	result, err := mysqlTableToCommonSchema(table)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, "users", result.Name)
	assert.Equal(t, schema.Object, result.Type)
	assert.False(t, result.Optional)
	assert.Len(t, result.Children, 4)

	// Verify column order is preserved
	assert.Equal(t, "id", result.Children[0].Name)
	assert.Equal(t, schema.Int64, result.Children[0].Type)

	assert.Equal(t, "name", result.Children[1].Name)
	assert.Equal(t, schema.String, result.Children[1].Type)

	assert.Equal(t, "email", result.Children[2].Name)
	assert.Equal(t, schema.String, result.Children[2].Type)

	assert.Equal(t, "created_at", result.Children[3].Name)
	assert.Equal(t, schema.Timestamp, result.Children[3].Type)
}

func TestMysqlTableToCommonSchemaRoundtrip(t *testing.T) {
	table := &gomysqlschema.Table{
		Schema: "testdb",
		Name:   "products",
		Columns: []gomysqlschema.TableColumn{
			{
				Name:    "id",
				Type:    gomysqlschema.TYPE_NUMBER,
				RawType: "int",
			},
			{
				Name:    "name",
				Type:    gomysqlschema.TYPE_STRING,
				RawType: "varchar(100)",
			},
			{
				Name:    "price",
				Type:    gomysqlschema.TYPE_DECIMAL,
				RawType: "decimal(10,2)",
			},
		},
	}

	// Convert to common schema
	commonSchema, err := mysqlTableToCommonSchema(table)
	require.NoError(t, err)

	// Serialize to generic format (as would be done for metadata)
	serialized := commonSchema.ToAny()
	require.NotNil(t, serialized)

	// Parse back from generic format
	parsed, err := schema.ParseFromAny(serialized)
	require.NoError(t, err)

	// Verify the parsed schema matches the original
	assert.Equal(t, commonSchema.Name, parsed.Name)
	assert.Equal(t, commonSchema.Type, parsed.Type)
	assert.Equal(t, len(commonSchema.Children), len(parsed.Children))

	for i, child := range commonSchema.Children {
		assert.Equal(t, child.Name, parsed.Children[i].Name)
		assert.Equal(t, child.Type, parsed.Children[i].Type)
		assert.Equal(t, child.Optional, parsed.Children[i].Optional)
	}
}

func TestMysqlTableToCommonSchemaNilTable(t *testing.T) {
	result, err := mysqlTableToCommonSchema(nil)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "table is nil")
}
