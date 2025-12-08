// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package couchbase

import (
	"errors"
	"time"

	"github.com/couchbase/gocb/v2"
)

func valueFromOp(op gocb.BulkOp) (out any, err error) {
	switch o := op.(type) {
	case *gocb.GetOp:
		if o.Err != nil {
			return nil, o.Err
		}
		err := o.Result.Content(&out)
		return out, err
	case *gocb.InsertOp:
		return nil, o.Err
	case *gocb.RemoveOp:
		return nil, o.Err
	case *gocb.ReplaceOp:
		return nil, o.Err
	case *gocb.UpsertOp:
		return nil, o.Err
	}

	return nil, errors.New("type not supported")
}

func get(key string, _ []byte, _ *time.Duration) gocb.BulkOp {
	return &gocb.GetOp{
		ID: key,
	}
}

func insert(key string, data []byte, ttl *time.Duration) gocb.BulkOp {
	op := &gocb.InsertOp{
		ID:    key,
		Value: data,
	}

	if ttl != nil {
		op.Expiry = *ttl
	}

	return op
}

func remove(key string, _ []byte, _ *time.Duration) gocb.BulkOp {
	return &gocb.RemoveOp{
		ID: key,
	}
}

func replace(key string, data []byte, ttl *time.Duration) gocb.BulkOp {
	op := &gocb.ReplaceOp{
		ID:    key,
		Value: data,
	}

	if ttl != nil {
		op.Expiry = *ttl
	}

	return op
}

func upsert(key string, data []byte, ttl *time.Duration) gocb.BulkOp {
	op := &gocb.UpsertOp{
		ID:    key,
		Value: data,
	}

	if ttl != nil {
		op.Expiry = *ttl
	}

	return op
}
