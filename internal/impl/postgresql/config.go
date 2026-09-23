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
	cfg      incrementalsnapshot.Cfg
	cache    string
	cacheKey string
}

func newDefaultIncSnapshotCfg() *incSnapshotCfg {
	return &incSnapshotCfg{
		cacheKey: incrementalsnapshot.DefaultIncSnapshotCheckpointKey,
		// The zero Cfg is a disabled snapshot.
	}
}

func parseIncrementalSnapshotCfg(conf *service.ParsedConfig, heartbeatInterval time.Duration, signalTableName string, streamSnapshot bool) (*incSnapshotCfg, error) {
	snapConf := conf.Namespace(fieldIncSnapshot)

	if enabled, err := snapConf.FieldBool(fieldIncSnapshotEnabled); err != nil {
		return nil, err
	} else if !enabled {
		return newDefaultIncSnapshotCfg(), nil
	}

	// stream_snapshot (blocking) must be disabled
	if streamSnapshot {
		return nil, fmt.Errorf(
			"%s and %s.%s are mutually exclusive snapshot modes, only one can be enabled",
			fieldStreamSnapshot, fieldIncSnapshot, fieldIncSnapshotEnabled,
		)
	}

	// signal table is needed
	if signalTableName == "" {
		return nil, fmt.Errorf(
			"%s.%s is true but %s is not set: tables are requested by inserting a %q signal, so a signal table is required",
			fieldIncSnapshot, fieldIncSnapshotEnabled, fieldSignalTableName, replication.SnapshotSignalType,
		)
	}

	var (
		out = newDefaultIncSnapshotCfg()
		cfg = incrementalsnapshot.Cfg{Enabled: true}
		err error
	)

	if cfg.HeartbeatInterval, err = snapConf.FieldDuration(fieldIncSnapshotHeartbeatInterval); err != nil {
		return nil, err
	} else if cfg.HeartbeatInterval <= 0 {
		return nil, fmt.Errorf("%s.%s must be > 0, got %s", fieldIncSnapshot, fieldIncSnapshotHeartbeatInterval, cfg.HeartbeatInterval)
	}

	if cfg.ChunkSize, err = snapConf.FieldInt(fieldIncrementalSnapshotChunkSize); err != nil {
		return nil, err
	} else if cfg.ChunkSize <= 0 {
		return nil, fmt.Errorf("%s.%s must be > 0, got %d", fieldIncSnapshot, fieldIncrementalSnapshotChunkSize, cfg.ChunkSize)
	}

	// The snapshot moves forward only on a streamed commit. On a table with
	// no writes the heartbeat makes the only such commit. Without a heartbeat
	// the snapshot reads the first chunk and then stops for ever, and it
	// reports no error. Refuse this configuration.
	if heartbeatInterval <= 0 {
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
	if out.cache == "" {
		return nil, fmt.Errorf("%s.%s is required when %s.%s is true", fieldIncSnapshot, fieldIncSnapshotCheckpointCache, fieldIncSnapshot, fieldIncSnapshotEnabled)
	}
	if !conf.Resources().HasCache(out.cache) {
		return nil, fmt.Errorf("unknown cache resource: %s", out.cache)
	}
	if out.cacheKey, err = snapConf.FieldString(fieldIncSnapshotCheckpointCacheKey); err != nil {
		return nil, err
	}

	out.cfg = cfg
	return out, nil
}
