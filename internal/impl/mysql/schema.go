// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	gomysqlschema "github.com/go-mysql-org/go-mysql/schema"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// mysqlDecimalRegex parses the precision/scale out of MySQL's DECIMAL/NUMERIC
// raw column type. Matches "decimal(p,s)", "decimal(p)" and "decimal" (with
// any case). MySQL aliases NUMERIC to DECIMAL.
var mysqlDecimalRegex = regexp.MustCompile(`(?i)^\s*(?:decimal|numeric)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$`)

// parseMySQLDecimal returns (precision, scale) for a MySQL DECIMAL/NUMERIC
// column. Defaults match MySQL's: bare "decimal" -> (10, 0); "decimal(p)" ->
// (p, 0); "decimal(p, s)" -> (p, s).
func parseMySQLDecimal(rawType string) (precision, scale int32, ok bool) {
	m := mysqlDecimalRegex.FindStringSubmatch(rawType)
	if m == nil {
		return 0, 0, false
	}
	if m[1] == "" {
		return 10, 0, true
	}
	p, err := strconv.ParseInt(m[1], 10, 32)
	if err != nil {
		return 0, 0, false
	}
	if m[2] == "" {
		return int32(p), 0, true
	}
	s, err := strconv.ParseInt(m[2], 10, 32)
	if err != nil {
		return 0, 0, false
	}
	return int32(p), int32(s), true
}

// mysqlTableToCommonSchema converts a MySQL table schema to benthos common schema format.
func mysqlTableToCommonSchema(table *gomysqlschema.Table) (*schema.Common, error) {
	if table == nil {
		return nil, errors.New("table is nil")
	}

	children := make([]schema.Common, 0, len(table.Columns))
	for _, col := range table.Columns {
		commonCol, err := mysqlColumnToCommon(col)
		if err != nil {
			return nil, fmt.Errorf("converting column %s: %w", col.Name, err)
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
		rawLower := strings.ToLower(col.RawType)
		if strings.HasPrefix(rawLower, "bigint") ||
			(strings.HasPrefix(rawLower, "int") && col.IsUnsigned) {
			commonType = schema.Int64
		} else {
			commonType = schema.Int32
		}
	case gomysqlschema.TYPE_MEDIUM_INT:
		commonType = schema.Int32
	case gomysqlschema.TYPE_FLOAT:
		if strings.HasPrefix(strings.ToLower(col.RawType), "double") {
			commonType = schema.Float64
		} else {
			commonType = schema.Float32
		}
	case gomysqlschema.TYPE_DECIMAL:
		precision, scale, ok := parseMySQLDecimal(col.RawType)
		if !ok {
			return schema.Common{}, fmt.Errorf("unable to parse precision/scale from MySQL column %s raw type %q", col.Name, col.RawType)
		}
		decCol, err := schema.NewDecimal(col.Name, precision, scale, true)
		if err != nil {
			return schema.Common{}, fmt.Errorf("MySQL column %s: %w", col.Name, err)
		}
		return decCol, nil
	case gomysqlschema.TYPE_STRING:
		commonType = schema.String
	case gomysqlschema.TYPE_DATETIME, gomysqlschema.TYPE_TIMESTAMP:
		commonType = schema.Timestamp
	case gomysqlschema.TYPE_DATE:
		commonType = schema.Timestamp
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
		// JSON columns contain arbitrary structured data with no static schema.
		// schema.Any signals to downstream consumers (e.g. parquet_encode) that
		// the field type is unknown; they must handle Any explicitly or return an
		// actionable error prompting the user to add a type-conversion step.
		commonType = schema.Any
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
