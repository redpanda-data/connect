// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protowire"
)

// Supported values for the --format flag.
const (
	formatJSON     = "json"
	formatProtobuf = "protobuf"
)

// orderProtoSchema is the Protobuf definition registered in the Schema
// Registry under <topic>-value. It mirrors the JSON order record built by
// buildRecord field for field: id/ts/region/amount/status/payload keep the
// JSON record's names and the natural proto type for each JSON type (JSON
// number integer -> int64, float -> double, strings -> string). ts stays a
// string (RFC 3339) rather than google.protobuf.Timestamp so the schema has
// no imports and a Parquet writer sees the same column types the JSON
// pipeline would infer.
//
// The field numbers here are baked into encodeOrderProto below; the unit
// tests parse this exact text and round-trip the hand-encoded bytes through
// it so the two cannot drift.
const orderProtoSchema = `syntax = "proto3";
package bench;

message Order {
  int64 id = 1;
  string ts = 2;
  string region = 3;
  double amount = 4;
  string status = 5;
  string payload = 6;
}
`

// Field numbers of the Order message, in schema order.
const (
	orderFieldID      protowire.Number = 1
	orderFieldTS      protowire.Number = 2
	orderFieldRegion  protowire.Number = 3
	orderFieldAmount  protowire.Number = 4
	orderFieldStatus  protowire.Number = 5
	orderFieldPayload protowire.Number = 6
)

// protoFixedOverhead is the average non-payload byte cost of one framed
// Protobuf order: the 6-byte Confluent header (magic + 4-byte schema id +
// single-byte message index), id (tag + up to a 5-byte varint), ts (tag + len
// + ~30 chars), region, amount (tag + 8 bytes), status, and the payload's tag
// plus 2-byte length. Measured over a million synthetic records at about 74.4
// bytes (see TestProtobufEncoder_AverageSizeMatchesRowSize), so --row-size in
// protobuf mode targets the TOTAL value size on the wire, which is what
// dataset.row_size_bytes in the scenario then reports truthfully.
const protoFixedOverhead = 74

// confluentMagicByte leads every Schema-Registry-framed value.
const confluentMagicByte = 0x00

// recordEncoder turns a buildRecord map into the bytes produced to Kafka.
type recordEncoder interface {
	// Encode returns a fresh slice (the producer retains it).
	Encode(rec map[string]any) ([]byte, error)
	// PadLen is the payload padding length that makes an encoded record about
	// rowSize bytes.
	PadLen(rowSize int) int
}

type jsonEncoder struct{}

func (jsonEncoder) Encode(rec map[string]any) ([]byte, error) { return json.Marshal(rec) }
func (jsonEncoder) PadLen(rowSize int) int                    { return rowPadLen(rowSize) }

// protobufEncoder frames each order in the Confluent wire format.
type protobufEncoder struct {
	// header is the constant 6-byte prefix: magic byte, big-endian schema id,
	// then the message-index array. The index array for the first message in
	// the schema is the single byte 0x00 (the Confluent shorthand for [0]).
	header []byte
}

func newProtobufEncoder(schemaID int) *protobufEncoder {
	h := make([]byte, 0, 6)
	h = append(h, confluentMagicByte,
		byte(schemaID>>24), byte(schemaID>>16), byte(schemaID>>8), byte(schemaID),
		0x00)
	return &protobufEncoder{header: h}
}

func (e *protobufEncoder) PadLen(rowSize int) int {
	n := rowSize - protoFixedOverhead
	if n < 0 {
		return 0
	}
	return n
}

func (e *protobufEncoder) Encode(rec map[string]any) ([]byte, error) {
	payload, _ := rec["payload"].(string)
	ts, _ := rec["ts"].(string)
	region, _ := rec["region"].(string)
	status, _ := rec["status"].(string)
	id, ok := rec["id"].(int64)
	if !ok {
		return nil, fmt.Errorf("order id is %T, want int64", rec["id"])
	}
	amount, ok := rec["amount"].(float64)
	if !ok {
		return nil, fmt.Errorf("order amount is %T, want float64", rec["amount"])
	}
	// Capacity: header + generous slack for the small fields, so the append
	// chain never reallocates.
	const smallFieldsSlack = 96
	b := make([]byte, 0, len(e.header)+len(payload)+smallFieldsSlack)
	b = append(b, e.header...)
	b = appendOrderProto(b, id, ts, region, amount, status, payload)
	return b, nil
}

// appendOrderProto appends the proto3 encoding of an Order. Fields holding
// their proto3 default (0, 0.0, "") are omitted, exactly as protoc-generated
// encoders do, so sizes match what a real producer emits.
func appendOrderProto(b []byte, id int64, ts, region string, amount float64, status, payload string) []byte {
	if id != 0 {
		b = protowire.AppendTag(b, orderFieldID, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(id))
	}
	if ts != "" {
		b = protowire.AppendTag(b, orderFieldTS, protowire.BytesType)
		b = protowire.AppendString(b, ts)
	}
	if region != "" {
		b = protowire.AppendTag(b, orderFieldRegion, protowire.BytesType)
		b = protowire.AppendString(b, region)
	}
	if amount != 0 {
		b = protowire.AppendTag(b, orderFieldAmount, protowire.Fixed64Type)
		b = protowire.AppendFixed64(b, math.Float64bits(amount))
	}
	if status != "" {
		b = protowire.AppendTag(b, orderFieldStatus, protowire.BytesType)
		b = protowire.AppendString(b, status)
	}
	if payload != "" {
		b = protowire.AppendTag(b, orderFieldPayload, protowire.BytesType)
		b = protowire.AppendString(b, payload)
	}
	return b
}

// Retry shape for reaching the Schema Registry: it starts with the broker,
// whose cloud-init takes several minutes, matching the topic-create loop.
var (
	srRegisterAttempts = 90
	srRegisterBackoff  = 5 * time.Second
)

// registerOrderSchema registers orderProtoSchema under subject and returns
// its id. Registering an identical schema again returns the existing id, so
// seed and every workload invocation can call it unconditionally.
func registerOrderSchema(ctx context.Context, registryURL, subject string) (int, error) {
	cl, err := sr.NewClient(sr.URLs(registryURL))
	if err != nil {
		return 0, fmt.Errorf("schema registry client: %w", err)
	}
	var lastErr error
	for attempt := 1; attempt <= srRegisterAttempts; attempt++ {
		ss, err := cl.CreateSchema(ctx, subject, sr.Schema{Schema: orderProtoSchema, Type: sr.TypeProtobuf})
		if err == nil {
			log.Printf("json-orders: schema registered under %q (id %d)", subject, ss.ID)
			return ss.ID, nil
		}
		lastErr = err
		log.Printf("json-orders: waiting for schema registry (attempt %d): %v", attempt, err)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(srRegisterBackoff):
		}
	}
	return 0, fmt.Errorf("registering %q at %s: %w", subject, registryURL, lastErr)
}

// newRecordEncoder builds the encoder for --format. For protobuf it registers
// the schema (subject <topic>-value, the Confluent TopicNameStrategy) first,
// because the schema id is part of every record's header.
func newRecordEncoder(ctx context.Context, format, registryURL, topic string) (recordEncoder, error) {
	switch format {
	case "", formatJSON:
		return jsonEncoder{}, nil
	case formatProtobuf:
		if registryURL == "" {
			return nil, fmt.Errorf("--schema-registry-url is required with --format=protobuf")
		}
		id, err := registerOrderSchema(ctx, registryURL, topic+"-value")
		if err != nil {
			return nil, err
		}
		return newProtobufEncoder(id), nil
	default:
		return nil, fmt.Errorf("--format must be %q or %q (got %q)", formatJSON, formatProtobuf, format)
	}
}
