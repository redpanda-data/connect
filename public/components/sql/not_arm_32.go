//go:build !arm
// +build !arm

package sql

import (
	// Packages that should only be included in non-arm builds (for now).
	_ "github.com/ClickHouse/clickhouse-go/v2"
)
