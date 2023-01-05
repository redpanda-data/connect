package couchbase

import (
	"fmt"

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

	return nil, fmt.Errorf("type not supported")
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
