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

func parseIncrementalSnapshotCfg(conf *service.ParsedConfig, mgr *service.Resources, heartbeatInterval time.Duration) (*incSnapshotCfg, error) {
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

		if cfg.ChunkSize, err = snapConf.FieldInt(fieldIncrementalSnapshotChunkSize); err != nil {
			return nil, err
		}
		if cfg.ChunkSize <= 0 {
			return nil, fmt.Errorf("%s.%s must be > 0, got %d", fieldIncSnapshot, fieldIncrementalSnapshotChunkSize, cfg.ChunkSize)
		}

		// The snapshot only advances on a streamed commit, so on tables with no
		// live write traffic the heartbeat is the sole source of progress.
		// Without one the backfill fetches its first chunk and then stalls
		// indefinitely, with nothing to report -- reject that outright rather
		// than looking healthy while doing nothing.
		if cfg.Enabled && heartbeatInterval <= 0 {
			return nil, fmt.Errorf(
				"%s.%s is true but %s is disabled: incremental snapshot progress is paced by streamed commits, so a quiet table would never advance. Set %s to a non-zero interval",
				fieldIncSnapshot, fieldIncSnapshotEnabled, fieldHeartbeatInterval, fieldHeartbeatInterval,
			)
		}
		if cfg.Enabled && heartbeatInterval > incSnapshotSlowHeartbeatThreshold {
			// Same dependency, less severe: it progresses, just slowly. Quantify
			// it rather than leaving operators to discover the rate themselves.
			mgr.Logger().Warnf(
				"Incremental snapshot advances at most one chunk (%s.%s=%d rows) per streamed commit, and %s is %s. On tables with little write traffic that caps the backfill at roughly %d rows/hour; lower %s to speed it up.",
				fieldIncSnapshot, fieldIncrementalSnapshotChunkSize, cfg.ChunkSize,
				fieldHeartbeatInterval, heartbeatInterval,
				int64(float64(cfg.ChunkSize)*time.Hour.Seconds()/heartbeatInterval.Seconds()),
				fieldHeartbeatInterval,
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
