// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

//go:build tools

// Keeps the ruleguard DSL in this module's graph so the repo root's
// .golangci.yml (gocritic/ruleguard) can typecheck its rules file when
// golangci-lint runs from this standalone module. Mirrors the repo root's
// tools.go.
package tools

import (
	_ "github.com/quasilyte/go-ruleguard/dsl"
)
