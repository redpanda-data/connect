//go:build !arm
// +build !arm

package all

import (
	// Packages that should only be included in non-arm builds (for now)
	_ "github.com/ClickHouse/clickhouse-go/v2"
)
