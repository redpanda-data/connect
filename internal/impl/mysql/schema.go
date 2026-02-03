// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"fmt"

	gomysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/redpanda-data/benthos/v4/public/schema"
)

// mysqlTableToCommonSchema converts a MySQL table schema to benthos common schema format.
func mysqlTableToCommonSchema(table *gomysqlschema.Table) (*schema.Common, error) {
	if table == nil {
		return nil, fmt.Errorf("table is nil")
	}

	children := make([]schema.Common, 0, len(table.Columns))
	for _, col := range table.Columns {
		commonCol, err := mysqlColumnToCommon(col)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s: %w", col.Name, err)
		}
		children = append(children, commonCol)
	}

	return &schema.Common{
		Name:     table.Name,
		Type:     schema.Object,
		Optional: false,
		Children: children,
	}, nil
}

// mysqlColumnToCommon converts a MySQL column to a benthos common schema field.
func mysqlColumnToCommon(col gomysqlschema.TableColumn) (schema.Common, error) {
	// Virtual and stored columns might not have physical values in CDC events
	// but we include them in the schema for completeness
	var commonType schema.CommonType
	var children []schema.Common

	switch col.Type {
	case gomysqlschema.TYPE_NUMBER:
		// TYPE_NUMBER includes tinyint, smallint, int, bigint, year
		// Map to INT64 for safety (can hold all integer types)
		// Could be more specific based on col.RawType if needed
		commonType = schema.Int64
	case gomysqlschema.TYPE_MEDIUM_INT:
		commonType = schema.Int32
	case gomysqlschema.TYPE_FLOAT:
		// Covers float and double
		commonType = schema.Float64
	case gomysqlschema.TYPE_DECIMAL:
		// Decimals are represented as strings in the message data
		commonType = schema.String
	case gomysqlschema.TYPE_STRING:
		commonType = schema.String
	case gomysqlschema.TYPE_DATETIME, gomysqlschema.TYPE_TIMESTAMP:
		commonType = schema.Timestamp
	case gomysqlschema.TYPE_DATE:
		// Dates could be represented as timestamps or strings
		// Using string for compatibility
		commonType = schema.String
	case gomysqlschema.TYPE_TIME:
		// Time is typically represented as string
		commonType = schema.String
	case gomysqlschema.TYPE_BINARY:
		commonType = schema.ByteArray
	case gomysqlschema.TYPE_BIT:
		// Bit types can be treated as integers
		commonType = schema.Int64
	case gomysqlschema.TYPE_ENUM:
		// Enums are sent as strings in the message
		commonType = schema.String
	case gomysqlschema.TYPE_SET:
		// Sets are sent as arrays of strings
		commonType = schema.Array
		children = []schema.Common{
			{
				Name:     "element",
				Type:     schema.String,
				Optional: false,
			},
		}
	case gomysqlschema.TYPE_JSON:
		// JSON columns can contain any type, but we'll represent as string
		// since the actual JSON is decoded into structured data
		// A more sophisticated approach would be to infer schema from actual data
		commonType = schema.String
	case gomysqlschema.TYPE_POINT:
		// Geometric types - treating as binary for now
		commonType = schema.ByteArray
	default:
		return schema.Common{}, fmt.Errorf("unsupported MySQL column type: %d (%s)", col.Type, col.RawType)
	}

	return schema.Common{
		Name:     col.Name,
		Type:     commonType,
		Optional: true, // All MySQL columns can be NULL unless specified otherwise
		Children: children,
	}, nil
}
