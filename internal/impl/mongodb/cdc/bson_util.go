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

func bsonGetPath(doc bson.M, path ...string) any {
	var current any
	current = doc
	for _, segment := range path {
		d, ok := current.(bson.M)
		if !ok {
			return nil
		}
		current, ok = d[segment]
		if !ok {
			return nil
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

// prevTimestamp returns the cluster time just before ts. A change event at ts
// only proves the stream is through the tick before it: the events of one
// transaction share a cluster time, and the rest may not have streamed yet.
func prevTimestamp(ts bson.Timestamp) bson.Timestamp {
	switch {
	case ts.I > 0:
		return bson.Timestamp{T: ts.T, I: ts.I - 1}
	case ts.T > 0:
		return bson.Timestamp{T: ts.T - 1, I: math.MaxUint32}
	default:
		return bson.Timestamp{}
	}
}
