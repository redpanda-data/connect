// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"math"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func bsonGetPath(doc bson.D, path ...string) any {
	var current any
	current = doc
	for _, segment := range path {
		d, ok := current.(bson.D)
		if !ok {
			return nil
		}
		for _, kv := range d {
			if kv.Key == segment {
				current = kv.Value
				break
			}
		}
	}
	return current
}

func nextTimestamp(ts bson.Timestamp) bson.Timestamp {
	if ts.I == math.MaxUint32 {
		return bson.Timestamp{T: ts.T + 1}
	}
	return bson.Timestamp{T: ts.T, I: ts.I + 1}
}
