//go:build cgo
// +build cgo

package all

import (
	// Import cgo packages.
	_ "github.com/benthosdev/benthos/v4/internal/impl/zeromq"
)
