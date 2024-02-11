package gcp

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
)

const (
	// Test messages.
	msg1 = `{"what1":"meow1","what2":1,"what3":true}`
	msg2 = `{"what1":"meow2","what2":2,"what3":false}`
)

var (
	// Protobuf-encoded variants of msg1 (different field orderings).
	msg1EncodedV1 = []byte{10, 5, 109, 101, 111, 119, 49, 16, 1, 24, 1}
	msg1EncodedV2 = []byte{24, 1, 10, 5, 109, 101, 111, 119, 49, 16, 1}
	msg1EncodedV3 = []byte{16, 1, 24, 1, 10, 5, 109, 101, 111, 119, 49}

	// Protobuf-encoded variants of msg2 (different field orderings).
	msg2EncodedV1 = []byte{10, 5, 109, 101, 111, 119, 50, 16, 2}
	msg2EncodedV2 = []byte{24, 1, 10, 5, 109, 101, 111, 119, 49, 16, 1}
	msg2EncodedV3 = []byte{16, 2, 10, 5, 109, 101, 111, 119, 50}
)

type fakeBigQueryWriteServer struct {
	storagepb.UnimplementedBigQueryWriteServer

	// Internal state captured from AppendRows.
	Data []byte
}

func (f *fakeBigQueryWriteServer) GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest) (*storagepb.WriteStream, error) {
	return &storagepb.WriteStream{}, nil
}

func (f *fakeBigQueryWriteServer) AppendRows(stream storagepb.BigQueryWrite_AppendRowsServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		for _, row := range req.GetProtoRows().GetRows().GetSerializedRows() {
			msg, err := decodeRow(row)
			if err != nil {
				return err
			}
			f.Data = append(f.Data, []byte(msg)...)
			f.Data = append(f.Data, byte('\n'))
		}
		resp := &storagepb.AppendRowsResponse{
			WriteStream: "_default",
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func decodeRow(row []byte) (string, error) {
	if bytes.Equal(row, msg1EncodedV1) {
		return msg1, nil
	}
	if bytes.Equal(row, msg1EncodedV2) {
		return msg1, nil
	}
	if bytes.Equal(row, msg1EncodedV3) {
		return msg1, nil
	}
	if bytes.Equal(row, msg2EncodedV1) {
		return msg2, nil
	}
	if bytes.Equal(row, msg2EncodedV2) {
		return msg2, nil
	}
	if bytes.Equal(row, msg2EncodedV3) {
		return msg2, nil
	}
	return "", fmt.Errorf("cannot decode row: %v", row)
}
