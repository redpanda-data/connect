//go:build x_benthos_extra

package all

import (
	// Import extra packages, these are packages only imported with the tag
	// x_benthos_extra, which is normally reserved for -cgo suffixed builds
	_ "github.com/redpanda-data/connect/v4/internal/impl/zeromq"
)
