// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/vmihailenco/msgpack/v5"
)

var errFailedParse = errors.New("the data appears to be corrupt")

func appendBatchV0(buffer []byte, batch service.MessageBatch) ([]byte, error) {
	// First value indicates the marshal version, which starts at 0.
	buffer = binary.LittleEndian.AppendUint32(buffer, 0)

	// Second value indicates the number of messages in the batch.
	buffer = binary.LittleEndian.AppendUint32(buffer, uint32(len(batch)))

	for _, msg := range batch {
		var err error
		if buffer, err = appendMessageV0(buffer, msg); err != nil {
			return nil, err
		}
	}
	return buffer, nil
}

func appendMessageV0(buffer []byte, msg *service.Message) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.GetEncoder()
	enc.Reset(&buf)
	err := msg.MetaWalkMut(func(key string, value any) error {
		if err := enc.EncodeString(key); err != nil {
			return err
		}
		if err := enc.Encode(value); err != nil {
			return err
		}
		return nil
	})
	msgpack.PutEncoder(enc)
	if err != nil {
		return nil, err
	}
	metaBytes := buf.Bytes()
	// First value indicates length of serialized metadata.
	buffer = binary.LittleEndian.AppendUint32(buffer, uint32(len(metaBytes)))
	// Followed by metadata.
	buffer = append(buffer, metaBytes...)

	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	// Second value indicates length of content.
	buffer = binary.LittleEndian.AppendUint32(buffer, uint32(len(msgBytes)))
	// Followed by content.
	buffer = append(buffer, msgBytes...)
	return buffer, nil
}

func readBatch(b []byte) (service.MessageBatch, error) {
	if len(b) < 4 {
		return nil, errFailedParse
	}
	ver := binary.LittleEndian.Uint32(b)
	// Only supported version thus far.
	if ver != 0 {
		return nil, errFailedParse
	}
	b = b[4:]
	mb, b, err := readBatchV0(b)
	if err != nil {
		return nil, err
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("extra left over bytes when reading batch: %d", len(b))
	}
	return mb, nil
}

func readBatchV0(b []byte) (service.MessageBatch, []byte, error) {
	if len(b) < 4 {
		return nil, nil, errFailedParse
	}
	parts := binary.LittleEndian.Uint32(b)
	b = b[4:]
	batch := make(service.MessageBatch, parts)
	var err error
	for i := uint32(0); i < parts; i++ {
		if batch[i], b, err = readMessageV0(b); err != nil {
			return nil, nil, err
		}
	}
	return batch, b, nil
}

func readMessageV0(b []byte) (*service.Message, []byte, error) {
	// Metadata bytes.
	if len(b) < 4 {
		return nil, nil, errFailedParse
	}
	contentLen := binary.LittleEndian.Uint32(b)
	if len(b) < int(contentLen+4) {
		return nil, nil, errFailedParse
	}
	metaBytes := b[4 : 4+contentLen]
	b = b[contentLen+4:]
	// Content bytes.
	if len(b) < 4 {
		return nil, nil, errFailedParse
	}
	contentLen = binary.LittleEndian.Uint32(b)
	if len(b) < int(contentLen+4) {
		return nil, nil, errFailedParse
	}
	contentBytes := b[4 : 4+contentLen]
	b = b[contentLen+4:]
	msg := service.NewMessage(contentBytes)
	// Decode metadata
	dec := msgpack.GetDecoder()
	dec.UsePreallocateValues(true)
	dec.Reset(bytes.NewReader(metaBytes))
	defer msgpack.PutDecoder(dec)
	for {
		if _, err := dec.PeekCode(); errors.Is(err, io.EOF) {
			break
		}
		key, err := dec.DecodeString()
		if err != nil {
			return nil, nil, err
		}
		val, err := dec.DecodeInterface()
		if err != nil {
			return nil, nil, err
		}
		msg.MetaSetMut(key, val)
	}
	return msg, b, nil
}
