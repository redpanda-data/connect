package saphana_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana"
)

func TestNewCheckpointCacheConfigValidatesTableName(t *testing.T) {
	cases := []struct {
		name    string
		table   string
		wantErr bool
	}{
		{"valid underscore schema", "_RPCN_CDC.CHECKPOINT", false},
		{"valid alphanumeric", "MY_SCHEMA.MY_TABLE", false},
		{"dollar sign", "MY$SCHEMA.MY$TABLE", false},
		{"empty", "", true},
		{"no schema separator", "CHECKPOINT", true},
		{"sql injection semicolon", "T; DROP TABLE FOO--", true},
		{"trailing dot", "SCHEMA.", true},
		{"leading dot", ".TABLE", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := saphana.NewCheckpointCacheConfig(tc.table)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCheckpointCacheConfigSQLContainsTableName(t *testing.T) {
	cfg, err := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")
	require.NoError(t, err)
	// Verify the pre-built SQL references the table name.
	assert.Contains(t, cfg.UpsertSQL(), "_RPCN_CDC.CHECKPOINT")
	assert.Contains(t, cfg.SelectSQL(), "_RPCN_CDC.CHECKPOINT")
}

func TestCheckpointCacheKeyValidation(t *testing.T) {
	cfg, err := saphana.NewCheckpointCacheConfig("_RPCN_CDC.CHECKPOINT")
	require.NoError(t, err)
	require.Error(t, cfg.ValidateKey(""))
	require.Error(t, cfg.ValidateKey(string(make([]byte, 256))))
	require.NoError(t, cfg.ValidateKey("saphana_cdc_main"))
	require.NoError(t, cfg.ValidateKey("key-with-dashes"))
}
