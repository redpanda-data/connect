// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package all imports all enterprise and FOSS component implementations that
// ship with Redpanda Connect. This is a convenient way of importing every
// single connector at the cost of a larger dependency tree for your
// application.
package all

import (
	// Import all community components.
	_ "github.com/redpanda-data/connect/v4/public/components/community"

	// Import all enterprise components.
	_ "github.com/redpanda-data/connect/v4/public/components/gateway"
	_ "github.com/redpanda-data/connect/v4/public/components/gcp/enterprise"
	_ "github.com/redpanda-data/connect/v4/public/components/google"
	_ "github.com/redpanda-data/connect/v4/public/components/jira"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka/enterprise"
	_ "github.com/redpanda-data/connect/v4/public/components/mongodb/enterprise"
	_ "github.com/redpanda-data/connect/v4/public/components/mssqlserver"
	_ "github.com/redpanda-data/connect/v4/public/components/oracledb"
	_ "github.com/redpanda-data/connect/v4/public/components/postgresql"
	_ "github.com/redpanda-data/connect/v4/public/components/slack"
	_ "github.com/redpanda-data/connect/v4/public/components/snowflake"
	_ "github.com/redpanda-data/connect/v4/public/components/splunk"
	_ "github.com/redpanda-data/connect/v4/public/components/tigerbeetle"
)
