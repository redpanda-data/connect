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
	"github.com/redpanda-data/connect/v4/internal/replication"
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

func parseIncrementalSnapshotCfg(conf *service.ParsedConfig, heartbeatInterval time.Duration, signalTableName string, streamSnapshot bool) (*incSnapshotCfg, error) {
	out := newDefaultIncSnapshotCfg()
	// No config block: the snapshot is off and out holds the defaults.
	if !conf.Contains(fieldIncSnapshot) {
		return out, nil
	}

	var (
		snapConf = conf.Namespace(fieldIncSnapshot)
		cfg      = &incrementalsnapshot.Cfg{}
		err      error
	)

	if cfg.Enabled, err = snapConf.FieldBool(fieldIncSnapshotEnabled); err != nil {
		return nil, err
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

	// Tables come only from signals, which arrive as inserts into the
	// signal table. Without one nothing could ask for a backfill.
	if cfg.Enabled && signalTableName == "" {
		return nil, fmt.Errorf(
			"%s.%s is true but %s is not set: tables are requested by inserting a %q signal, so a signal table is required",
			fieldIncSnapshot, fieldIncSnapshotEnabled, fieldSignalTableName, replication.SnapshotSignalType,
		)
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

	return out, nil
}
