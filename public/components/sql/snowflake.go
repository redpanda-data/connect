//go:build !arm

package sql

import (
	// Import snowflake specifically.
	_ "github.com/snowflakedb/gosnowflake"
)
