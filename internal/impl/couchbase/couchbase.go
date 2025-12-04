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
	"strconv"

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
	case *gocb.IncrementOp:
		return o.Result.Content(), o.Err
	case *gocb.DecrementOp:
		return o.Result.Content(), o.Err
	}

	return nil, errors.New("type not supported")
}

func get(key string, _ []byte) gocb.BulkOp {
	return &gocb.GetOp{
		ID: key,
	}
}

func insert(key string, data []byte) gocb.BulkOp {
	return &gocb.InsertOp{
		ID:    key,
		Value: data,
	}
}

func remove(key string, _ []byte) gocb.BulkOp {
	return &gocb.RemoveOp{
		ID: key,
	}
}

func replace(key string, data []byte) gocb.BulkOp {
	return &gocb.ReplaceOp{
		ID:    key,
		Value: data,
	}
}

func upsert(key string, data []byte) gocb.BulkOp {
	return &gocb.UpsertOp{
		ID:    key,
		Value: data,
	}
}

func increment(key string, data []byte) gocb.BulkOp {
	delta := int64(0)
	if len(data) > 0 {
		if d, err := strconv.ParseInt(string(data), 10, 64); err == nil {
			delta = d
		}
	}

	return &gocb.IncrementOp{
		ID:      key,
		Delta:   delta,
		Initial: delta, // Default initial to delta
	}
}

func decrement(key string, data []byte) gocb.BulkOp {
	delta := int64(0)
	if len(data) > 0 {
		if d, err := strconv.ParseInt(string(data), 10, 64); err == nil {
			delta = d
		}
	}

	return &gocb.DecrementOp{
		ID:      key,
		Delta:   delta,
		Initial: -delta, // Default initial to -delta
	}
}
