// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	dbsql "database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The sql_* components accept `driver: hana` (aliased to hdb), so the go-hdb
// driver must be registered by this package rather than only via the
// enterprise-only saphana component import.
func TestHANADriverRegistered(t *testing.T) {
	assert.Contains(t, dbsql.Drivers(), "hdb",
		"driver: hana would fail at runtime in distributions that import sql components without the saphana package")
}
