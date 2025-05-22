// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type checkpointCache struct {
	resources *service.Resources
	cacheName string
	cacheKey  string
}

type checkpointState struct {
	ResumeToken bson.Raw `bson:"rpcn_resume_token,omitempty" json:"rpcn_resume_token,omitempty"`
	// stored after the first oplog write
	OplogPosition *bson.Timestamp `bson:"rpcn_oplog_start,omitempty" json:"rpcn_oplog_start,omitempty"`
}

func (c *checkpointCache) Store(ctx context.Context, state checkpointState) error {
	if state.ResumeToken == nil && state.OplogPosition.IsZero() {
		return nil
	}
	b, err := bson.MarshalExtJSON(&state, true, false)
	if err != nil {
		return err
	}
	var cErr error
	err = c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		cErr = cache.Set(ctx, c.cacheKey, b, nil)
	})
	if err == nil {
		err = cErr
	}
	return err
}

func (c *checkpointCache) Load(ctx context.Context) (*checkpointState, error) {
	var cVal []byte
	var cErr error
	err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		cVal, cErr = cache.Get(ctx, c.cacheKey)
	})
	if err == nil {
		err = cErr
	}
	if err == service.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var test bson.M
	if err = bson.UnmarshalExtJSON(cVal, true, &test); err != nil {
		return nil, err
	}
	_, ok1 := test["rpcn_oplog_start"]
	_, ok2 := test["rpcn_resume_token"]
	if !ok1 && !ok2 {
		// this means it is the legacy (raw) resume token format
		var resumeToken bson.Raw
		if err = bson.UnmarshalExtJSON(cVal, true, &resumeToken); err != nil {
			return nil, err
		}
		return &checkpointState{ResumeToken: resumeToken}, nil
	}
	var state checkpointState
	if err = bson.UnmarshalExtJSON(cVal, true, &state); err != nil {
		return nil, err
	}
	return &state, nil
}
