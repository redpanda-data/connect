// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pgstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestNewPgStreamInputBadDSNRedacted(t *testing.T) {
	const secret = "S3cretPass"
	pConf, err := newPostgresCDCConfig().ParseYAML(`
dsn: postgres://user@localhost:5432/db?password=`+secret+`&sslmode=bogus
slot_name: my_slot
schema: dbo
tables:
  - events
`, service.NewEnvironment())
	require.NoError(t, err)

	mgr := service.MockResources()
	license.InjectTestService(mgr)

	_, err = newPgStreamInput(pConf, mgr)
	require.Error(t, err)
	assert.ErrorContains(t, err, "sslmode")
	assert.NotContains(t, err.Error(), secret)
}
