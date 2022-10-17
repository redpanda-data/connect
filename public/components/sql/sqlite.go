// Platforms and architectures list from https://pkg.go.dev/modernc.org/sqlite?utm_source=godoc#hdr-Supported_platforms_and_architectures
// Last updated from modernc.org/sqlite@v1.19.1
//go:build (darwin && (amd64 || arm64)) || (freebsd && (amd64 || arm64)) || (linux && (386 || amd64 || arm || arm64 || riscv64)) || (windows && (amd64 || arm64))

package sql

import (
	// Import sqlite specifically.
	_ "modernc.org/sqlite"
)
