package qdrant

import (
	"fmt"

	pb "github.com/qdrant/go-client/qdrant"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

// newPointID converts an ID of any type to a pb.PointId, returning an error if the type is invalid.
func newPointID(id any) (*pb.PointId, error) {
	switch v := id.(type) {
	case string:
		return &pb.PointId{
			PointIdOptions: &pb.PointId_Uuid{
				Uuid: v,
			},
		}, nil
	default:
		n, err := bloblang.ValueAsInt64(id)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, fmt.Errorf("ID cannot be a negative integer ID: %d", v)
		}
		return &pb.PointId{
			PointIdOptions: &pb.PointId_Num{
				Num: uint64(n),
			},
		}, nil
	}
}
