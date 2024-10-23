// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import "github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"

type schemaDetails struct {
	sr.SchemaInfo
	// The `Subject` field is omitted from the schema info payload since the `/subjects/<Subject>/versions` endpoint
	// rejects it and we pass it along as metadata.
	Subject string `json:"-"`
	Version int    `json:"version"`
}
