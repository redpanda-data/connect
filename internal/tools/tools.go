//go:build tools
// +build tools

// Package tools makes it possible to record various development tools as
// dependencies that will be managed in go.mod/go.sum
// To learn more about this, visit: https://github.com/go-modules-by-example/index/blob/master/010_tools/README.md
package tools

import (
	_ "cuelang.org/go/cmd/cue"
)
