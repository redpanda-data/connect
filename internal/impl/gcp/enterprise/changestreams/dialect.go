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
	"fmt"

	"cloud.google.com/go/spanner"
)

type dialect int

const (
	dialectUnknown dialect = iota
	dialectGoogleSQL
	dialectPostgreSQL
)

func (d dialect) String() string {
	switch d {
	case dialectGoogleSQL:
		return "GoogleSQL"
	case dialectPostgreSQL:
		return "PostgreSQL"
	default:
		return ""
	}
}

func detectDialect(ctx context.Context, client *spanner.Client) (dialect, error) {
	var value string
	stmt := spanner.NewStatement("SELECT option_value FROM information_schema.database_options WHERE option_name = 'database_dialect'")
	if err := client.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		return r.ColumnByName("option_value", &value)
	}); err != nil {
		return dialectUnknown, err
	}

	switch value {
	case "GOOGLE_STANDARD_SQL", "":
		return dialectGoogleSQL, nil
	case "POSTGRESQL":
		return dialectPostgreSQL, nil
	default:
		return dialectUnknown, fmt.Errorf("invalid dialect: %q", value)
	}
}
