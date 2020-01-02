// +build !wasm

package processor

// Import extra drivers that aren't supported by WASM builds.
import (
	// SQL Drivers
	_ "github.com/lib/pq"
)
