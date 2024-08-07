// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package schema

import (
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/cli"
)

// Standard returns the config schema of a standard build of Redpanda Connect.
func Standard() *service.ConfigSchema {
	return cli.Schema(false, "", "")
}

// Cloud returns the config schema of a cloud build of Redpanda Connect.
func Cloud() *service.ConfigSchema {
	return cli.Schema(true, "", "")
}
