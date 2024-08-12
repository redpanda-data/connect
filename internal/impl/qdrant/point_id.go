package qdrant

import (
	"fmt"

	pb "github.com/qdrant/go-client/qdrant"
)

func newPointID(id any) (*pb.PointId, error) {
	switch id := id.(type) {
	case string:
		return &pb.PointId{
			PointIdOptions: &pb.PointId_Uuid{
				Uuid: id,
			},
		}, nil
	case int64:
		return &pb.PointId{
			PointIdOptions: &pb.PointId_Num{
				Num: uint64(id),
			},
		}, nil
	default:
		return nil, fmt.Errorf("invalid point ID type %T", id)
	}
}
