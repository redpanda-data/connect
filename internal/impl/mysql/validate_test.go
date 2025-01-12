// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"strings"
	"testing"
)

func TestValidateTableName(t *testing.T) {
	tests := []struct {
		name        string
		tableName   string
		expectedErr error
	}{
		// Valid cases
		{
			name:        "Valid simple table name",
			tableName:   "users",
			expectedErr: nil,
		},
		{
			name:        "Valid table name with numbers",
			tableName:   "orders_2024",
			expectedErr: nil,
		},
		{
			name:        "Valid table name with underscore prefix",
			tableName:   "_temp_table",
			expectedErr: nil,
		},
		{
			name:        "Valid table name with dollar sign",
			tableName:   "user$data",
			expectedErr: nil,
		},
		{
			name:        "Valid table name with mixed case",
			tableName:   "UserProfiles",
			expectedErr: nil,
		},

		// Invalid cases
		{
			name:        "Empty table name",
			tableName:   "",
			expectedErr: errEmptyTableName,
		},
		{
			name:        "Table name starting with number",
			tableName:   "2users",
			expectedErr: errInvalidTableStartChar,
		},
		{
			name:        "Table name with special characters",
			tableName:   "users@table",
			expectedErr: errInvalidTableName,
		},
		{
			name:        "Table name with spaces",
			tableName:   "user table",
			expectedErr: errInvalidTableName,
		},
		{
			name:        "Table name with hyphens",
			tableName:   "user-table",
			expectedErr: errInvalidTableName,
		},
		{
			name:        "Too long table name",
			tableName:   strings.Repeat("a", 65),
			expectedErr: errInvalidTableLength,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTableName(tc.tableName)

			if tc.expectedErr == nil && err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			if tc.expectedErr != nil && err == nil {
				t.Errorf("expected error %v, got nil", tc.expectedErr)
			}

			if tc.expectedErr != nil && err != nil && tc.expectedErr.Error() != err.Error() {
				t.Errorf("expected error %v, got %v", tc.expectedErr, err)
			}
		})
	}
}
