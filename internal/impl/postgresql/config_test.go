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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestParseIncrementalSnapshotCfgDisabled(t *testing.T) {
	const base = `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
`

	// A configuration with no incremental_snapshot block returns before the
	// parse. It must still give the default values, which are no
	// configuration and the default checkpoint key.
	tests := []struct {
		name string
		conf string
	}{
		{
			name: "block omitted",
			conf: base,
		},
		{
			name: "block present but disabled",
			conf: base + `
incremental_snapshot:
  enabled: false
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := newPostgresCDCConfig().ParseYAML(test.conf, service.NewEnvironment())
			require.NoError(t, err)

			got, err := parseIncrementalSnapshotCfg(pConf, service.MockResources(), time.Hour, nil, false)
			require.NoError(t, err)
			require.NotNil(t, got, "callers read the returned fields unconditionally")

			assert.Nil(t, got.cfg, "pglogicalstream branches on a nil cfg when disabled")
			assert.Equal(t, newDefaultIncSnapshotCfg(), got)
		})
	}
}
