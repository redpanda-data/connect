// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Enterprise
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rel.md

package cli

// Distribution represents the type of Redpanda Connect build.
type Distribution string

const (
	DistStandard  Distribution = "standard"
	DistCloud     Distribution = "cloud"
	DistCloudAI   Distribution = "cloud-ai"
	DistCommunity Distribution = "community"
)
