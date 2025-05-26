// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreams

import (
	"fmt"
	"testing"

	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/changestreamstest"
)

func TestIntegrationDetectDialect(t *testing.T) {
	integration.CheckSkip(t)

	e := changestreamstest.MakeEmulatorHelper(t)

	testCases := []struct {
		dialect dialect
		fn      func(*adminpb.CreateDatabaseRequest)
	}{
		{
			dialect: dialectGoogleSQL,
		},
		{
			dialect: dialectPostgreSQL,
			fn: func(req *adminpb.CreateDatabaseRequest) {
				req.DatabaseDialect = dialectPostgreSQL
			},
		},
	}

	for i, tc := range testCases {
		t.Run(tc.dialect.String(), func(t *testing.T) {
			dbName := fmt.Sprintf("dialect%d", i)

			var opts []func(*adminpb.CreateDatabaseRequest)
			if tc.fn != nil {
				opts = append(opts, tc.fn)
			}
			dd, err := detectDialect(t.Context(), e.CreateTestDatabase(dbName, opts...))
			require.NoError(t, err)
			assert.Equal(t, tc.dialect, dd)
		})
	}
}
