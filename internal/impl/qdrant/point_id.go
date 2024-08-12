package qdrant

import (
	"errors"
	"fmt"

	pb "github.com/qdrant/go-client/qdrant"
)

// parsePointID extracts and converts the 'id' field from a map to a pb.PointId.
func newPointID(id any) (*pb.PointId, error) {
	idMap, ok := id.(map[string]any)
	if !ok {
		return nil, errors.New("failed to parse root to map, got invalid type")
	}

	idVal, exists := idMap["id"]
	if !exists {
		return nil, errors.New("'id' field not found in map")
	}

	return parseID(idVal)
}

// converts an ID of any type to a pb.PointId, returning an error if the type is invalid.
func parseID(id any) (*pb.PointId, error) {
	switch v := id.(type) {
	case string:
		return &pb.PointId{
			PointIdOptions: &pb.PointId_Uuid{
				Uuid: v,
			},
		}, nil
	case int64:
		if v < 0 {
			return nil, fmt.Errorf("ID cannot be a negative integer ID: %d", v)
		}
		return &pb.PointId{
			PointIdOptions: &pb.PointId_Num{
				Num: uint64(v),
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported ID type: %T Expected UUID string or a postive integer", v)
	}
}
