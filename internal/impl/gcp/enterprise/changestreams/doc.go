// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package changestreams has been copied from
// github.com/cloudspannerecosystem/spanner-change-streams-tail, and is a modified
// version of the original package with added:
//
//   - Partition Metadata Storage (TODO),
//   - Batching (TODO),
//   - Metrics (TODO).
//
// It also includes more test coverage, and is tailored RPCN streaming.
package changestreams
