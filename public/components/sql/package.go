// Package sql brings in the sql components and _all_ officially supported
// drivers. In order to hand-pick which drivers are included import
// github.com/benthosdev/benthos/v4/public/components/sql/base instead along
// with the specific drivers you want.
package sql

import (
	// Bring in the base plugin definitions.
	_ "github.com/benthosdev/benthos/v4/public/components/sql/base"

	// Import all (supported) sql drivers.
	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/sijms/go-ora/v2"
	_ "github.com/snowflakedb/gosnowflake"
)
