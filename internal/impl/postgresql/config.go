// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pgstream

import (
	"fmt"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/incrementalsnapshot"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
)

type incSnapshotCfg struct {
	cfg      *incrementalsnapshot.Cfg
	cache    string
	cacheKey string
}

func newDefaultIncSnapshotCfg() *incSnapshotCfg {
	return &incSnapshotCfg{
		cacheKey: incrementalsnapshot.DefaultIncSnapshotCheckpointKey,
	}
}

func parseIncrementalSnapshotCfg(conf *service.ParsedConfig, heartbeatInterval time.Duration, replicatedTables []string, streamSnapshot bool) (*incSnapshotCfg, error) {
	out := newDefaultIncSnapshotCfg()
	if conf.Contains(fieldIncSnapshot) {
		var (
			snapConf = conf.Namespace(fieldIncSnapshot)
			cfg      = &incrementalsnapshot.Cfg{}
			err      error
		)

		if cfg.Enabled, err = snapConf.FieldBool(fieldIncSnapshotEnabled); err != nil {
			return nil, err
		}
		if snapConf.Contains(fieldIncrementalSnapshotTables) {
			if cfg.Tables, err = snapConf.FieldStringList(fieldIncrementalSnapshotTables); err != nil {
				return nil, err
			}
		}

		if cfg.HeartbeatInterval, err = snapConf.FieldDuration(fieldIncSnapshotHeartbeatInterval); err != nil {
			return nil, err
		}
		if cfg.Enabled && cfg.HeartbeatInterval <= 0 {
			return nil, fmt.Errorf("%s.%s must be > 0, got %s", fieldIncSnapshot, fieldIncSnapshotHeartbeatInterval, cfg.HeartbeatInterval)
		}

		if cfg.ChunkSize, err = snapConf.FieldInt(fieldIncrementalSnapshotChunkSize); err != nil {
			return nil, err
		} else if cfg.ChunkSize <= 0 {
			return nil, fmt.Errorf("%s.%s must be > 0, got %d", fieldIncSnapshot, fieldIncrementalSnapshotChunkSize, cfg.ChunkSize)
		}

		if cfg.Enabled && streamSnapshot {
			return nil, fmt.Errorf(
				"%s and %s.%s are mutually exclusive snapshot modes, only one can be enabled",
				fieldStreamSnapshot, fieldIncSnapshot, fieldIncSnapshotEnabled,
			)
		}

		// Both lists empty: no table names to read, so the coordinator would
		// report itself complete without emitting a row.
		if cfg.Enabled && len(cfg.Tables) == 0 && len(replicatedTables) == 0 {
			return nil, fmt.Errorf(
				"%s.%s is true but no tables are listed: set %s.%s, or %s to inherit from",
				fieldIncSnapshot, fieldIncSnapshotEnabled,
				fieldIncSnapshot, fieldIncrementalSnapshotTables, fieldTables,
			)
		}

		// An unreplicated table is backfilled with no live changes to dedup
		// against, so writes after its chunk is read are lost. Only an
		// explicit list needs checking: an empty one inherits the replicated
		// set, and an empty replicatedTables means FOR ALL TABLES.
		if cfg.Enabled && len(cfg.Tables) > 0 && len(replicatedTables) > 0 {
			replicated := make(map[string]struct{}, len(replicatedTables))
			for _, table := range replicatedTables {
				normalized, err := sanitize.NormalizePostgresIdentifier(table)
				if err != nil {
					return nil, fmt.Errorf("invalid table name %q: %w", table, err)
				}
				replicated[normalized] = struct{}{}
			}
			for _, table := range cfg.Tables {
				normalized, err := sanitize.NormalizePostgresIdentifier(table)
				if err != nil {
					return nil, fmt.Errorf("invalid %s.%s entry %q: %w", fieldIncSnapshot, fieldIncrementalSnapshotTables, table, err)
				}
				if _, ok := replicated[normalized]; !ok {
					return nil, fmt.Errorf(
						"%s.%s entry %q is not listed in %s, so it would not be replicated: no live change could be deduplicated against its backfill",
						fieldIncSnapshot, fieldIncrementalSnapshotTables, table, fieldTables,
					)
				}
			}
		}

		// The snapshot moves forward only on a streamed commit. On a table
		// with no writes the heartbeat makes the only such commit. Without a
		// heartbeat the snapshot reads the first chunk and then stops for
		// ever, and it reports no error. Refuse this configuration.
		if cfg.Enabled && heartbeatInterval <= 0 {
			return nil, fmt.Errorf(
				"%s.%s is true but %s is disabled: incremental snapshot progress is paced by streamed commits, so a quiet table would never advance. Set %s to a non-zero interval",
				fieldIncSnapshot, fieldIncSnapshotEnabled, fieldHeartbeatInterval, fieldHeartbeatInterval,
			)
		}
		if snapConf.Contains(fieldIncSnapshotCheckpointCache) {
			if out.cache, err = snapConf.FieldString(fieldIncSnapshotCheckpointCache); err != nil {
				return nil, err
			}
		}
		if out.cacheKey, err = snapConf.FieldString(fieldIncSnapshotCheckpointCacheKey); err != nil {
			return nil, err
		}
		if cfg.Enabled && out.cache == "" {
			return nil, fmt.Errorf("%s.%s is required when %s.%s is true", fieldIncSnapshot, fieldIncSnapshotCheckpointCache, fieldIncSnapshot, fieldIncSnapshotEnabled)
		}
		if cfg.Enabled && !conf.Resources().HasCache(out.cache) {
			return nil, fmt.Errorf("unknown cache resource: %s", out.cache)
		}
		if cfg.Enabled {
			out.cfg = cfg
		}
	}

	return out, nil
}
