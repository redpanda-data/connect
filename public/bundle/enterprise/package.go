// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package enterprise imports all enterprise licensed plugin implementations
// that ship with Redpanda Connect, along with all free plugin implementations.
// This is a convenient way of importing every single connector at the cost of a
// larger dependency tree for your application.
package enterprise

import (
	// Import all public sub-categories.
	_ "github.com/redpanda-data/connect/v4/public/components/all"
)
