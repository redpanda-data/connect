package sql

import (
	// Bring in the internal plugin definitions.
	_ "github.com/benthosdev/benthos/v4/internal/impl/sql"

	// Import all (supported) sql drivers.
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)
