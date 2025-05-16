// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
//
// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package changestreams

import (
	"context"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

type dialect = databasepb.DatabaseDialect

var (
	dialectGoogleSQL  = databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL
	dialectPostgreSQL = databasepb.DatabaseDialect_POSTGRESQL
)

func detectDialect(ctx context.Context, client *spanner.Client) (dialect, error) {
	const stmt = `SELECT option_value FROM information_schema.database_options WHERE option_name = 'database_dialect'`
	var v string
	if err := client.Single().Query(ctx, spanner.NewStatement(stmt)).Do(func(r *spanner.Row) error {
		return r.ColumnByName("option_value", &v)
	}); err != nil {
		return databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED, err
	}

	switch v {
	case dialectGoogleSQL.String(), "":
		return dialectGoogleSQL, nil
	case dialectPostgreSQL.String():
		return dialectPostgreSQL, nil
	default:
		return databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED, nil
	}
}
