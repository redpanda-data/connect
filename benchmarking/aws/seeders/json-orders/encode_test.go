// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// orderDescriptor compiles orderProtoSchema, the exact text registered in the
// Schema Registry, so decoding proves the hand-rolled encoding matches what
// consumers will see.
func orderDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	p := protoparse.Parser{Accessor: protoparse.FileContentsFromMap(map[string]string{"order.proto": orderProtoSchema})}
	fds, err := p.ParseFiles("order.proto")
	if err != nil {
		t.Fatalf("parsing orderProtoSchema: %v", err)
	}
	md := fds[0].FindMessage("bench.Order")
	if md == nil {
		t.Fatal("bench.Order not found in orderProtoSchema")
	}
	return md.UnwrapMessage()
}

func TestProtobufEncoder_WireFormatFraming(t *testing.T) {
	const schemaID = 0x01020304
	enc := newProtobufEncoder(schemaID)
	rng := rand.New(rand.NewSource(1))
	rec := buildRecord(newPayloadPool(rng), rng, 5, 5, 16)

	val, err := enc.Encode(rec)
	if err != nil {
		t.Fatal(err)
	}

	if val[0] != 0x00 {
		t.Errorf("magic byte = %#x, want 0x00", val[0])
	}
	wantID := []byte{0x01, 0x02, 0x03, 0x04}
	if string(val[1:5]) != string(wantID) {
		t.Errorf("schema id bytes = %v, want big-endian %v", val[1:5], wantID)
	}
	if val[5] != 0x00 {
		t.Errorf("message index byte = %#x, want single 0x00 (= [0])", val[5])
	}

	// The same bytes must parse with the library the real Confluent clients
	// are compatible with: franz-go's sr.ConfluentHeader.
	var h sr.ConfluentHeader
	id, rest, err := h.DecodeID(val)
	if err != nil {
		t.Fatal(err)
	}
	if id != schemaID {
		t.Errorf("DecodeID = %d, want %d", id, schemaID)
	}
	idx, body, err := h.DecodeIndex(rest, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx) != 1 || idx[0] != 0 {
		t.Errorf("DecodeIndex = %v, want [0]", idx)
	}
	if len(body) != len(val)-6 {
		t.Errorf("body length = %d, want %d", len(body), len(val)-6)
	}
}

func TestProtobufEncoder_RoundTripsEveryField(t *testing.T) {
	md := orderDescriptor(t)
	enc := newProtobufEncoder(9)
	rng := rand.New(rand.NewSource(2))
	pool := newPayloadPool(rng)

	for _, tc := range []struct{ id, vary int64 }{{0, 0}, {1, 1}, {1 << 40, 3}, {123456789, 99999}} {
		rec := buildRecord(pool, rng, tc.id, tc.vary, 200)
		val, err := enc.Encode(rec)
		if err != nil {
			t.Fatal(err)
		}
		msg := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(val[6:], msg); err != nil {
			t.Fatalf("unmarshal id=%d: %v", tc.id, err)
		}
		get := func(name string) protoreflect.Value {
			return msg.Get(md.Fields().ByName(protoreflect.Name(name)))
		}
		if got := get("id").Int(); got != rec["id"].(int64) {
			t.Errorf("id = %d, want %d", got, rec["id"])
		}
		for _, f := range []string{"ts", "region", "status", "payload"} {
			if got := get(f).String(); got != rec[f].(string) {
				t.Errorf("%s = %q, want %q", f, got, rec[f])
			}
		}
		if got := get("amount").Float(); got != rec["amount"].(float64) {
			t.Errorf("amount = %v, want %v", got, rec["amount"])
		}
		if n := len(msg.GetUnknown()); n != 0 {
			t.Errorf("%d bytes of unknown fields: encoding does not match schema", n)
		}
	}
}

func TestProtobufEncoder_MatchesCanonicalMarshal(t *testing.T) {
	// proto.Marshal over a dynamic message is protoc's canonical output; the
	// hand-rolled bytes must be identical, which also pins field order and the
	// omit-defaults behaviour.
	md := orderDescriptor(t)
	enc := newProtobufEncoder(1)
	rng := rand.New(rand.NewSource(3))
	pool := newPayloadPool(rng)
	for _, id := range []int64{0, 7, 300, 1 << 33} {
		rec := buildRecord(pool, rng, id, id, 64)
		val, _ := enc.Encode(rec)

		msg := dynamicpb.NewMessage(md)
		set := func(name string, v protoreflect.Value) { msg.Set(md.Fields().ByName(protoreflect.Name(name)), v) }
		set("id", protoreflect.ValueOfInt64(rec["id"].(int64)))
		set("ts", protoreflect.ValueOfString(rec["ts"].(string)))
		set("region", protoreflect.ValueOfString(rec["region"].(string)))
		set("amount", protoreflect.ValueOfFloat64(rec["amount"].(float64)))
		set("status", protoreflect.ValueOfString(rec["status"].(string)))
		set("payload", protoreflect.ValueOfString(rec["payload"].(string)))
		want, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		if string(val[6:]) != string(want) {
			t.Errorf("id=%d: hand-encoded bytes differ from canonical marshal", id)
		}
	}
}

func TestProtobufEncoder_RejectsWrongTypes(t *testing.T) {
	enc := newProtobufEncoder(1)
	if _, err := enc.Encode(map[string]any{"id": "x", "amount": 1.0}); err == nil {
		t.Error("string id must error")
	}
	if _, err := enc.Encode(map[string]any{"id": int64(1), "amount": "x"}); err == nil {
		t.Error("string amount must error")
	}
}

// TestProtobufEncoder_AverageSizeMatchesRowSize pins protoFixedOverhead: with
// --row-size=1200 the mean framed value must land within a few bytes of 1200,
// so dataset.row_size_bytes: 1200 in the scenario is the real wire size. Run
// with -v to see the measured mean.
func TestProtobufEncoder_AverageSizeMatchesRowSize(t *testing.T) {
	const (
		rowSize   = 1200
		samples   = 200000
		tolerance = 3.0
	)
	enc := newProtobufEncoder(123)
	rng := rand.New(rand.NewSource(4))
	pool := newPayloadPool(rng)
	padLen := enc.PadLen(rowSize)

	var total, minLen, maxLen int
	minLen = math.MaxInt
	for i := int64(0); i < samples; i++ {
		// Ids spread across the range a 300k rows/s, 25 minute point reaches.
		id := i * 2250
		val, err := enc.Encode(buildRecord(pool, rng, id, i, padLen))
		if err != nil {
			t.Fatal(err)
		}
		total += len(val)
		minLen, maxLen = min(minLen, len(val)), max(maxLen, len(val))
	}
	mean := float64(total) / samples
	t.Logf("protobuf framed value size over %d records: mean %.1f B (min %d, max %d) for --row-size=%d, padLen=%d",
		samples, mean, minLen, maxLen, rowSize, padLen)
	if math.Abs(mean-rowSize) > tolerance {
		t.Errorf("mean framed size %.1f B differs from --row-size %d by more than %.0f B; retune protoFixedOverhead",
			mean, rowSize, tolerance)
	}
}

func TestProtobufEncoder_PadLenClampsAtZero(t *testing.T) {
	enc := newProtobufEncoder(1)
	if got := enc.PadLen(10); got != 0 {
		t.Errorf("PadLen(10) = %d, want 0", got)
	}
	if got := enc.PadLen(protoFixedOverhead + 5); got != 5 {
		t.Errorf("PadLen = %d, want 5", got)
	}
}

func TestJSONEncoder_UnchangedBehaviour(t *testing.T) {
	enc := jsonEncoder{}
	if got, want := enc.PadLen(1200), rowPadLen(1200); got != want {
		t.Errorf("json PadLen = %d, want %d (existing scenarios must not change)", got, want)
	}
	rng := rand.New(rand.NewSource(5))
	rec := buildRecord(newPayloadPool(rng), rng, 1, 1, 8)
	got, err := enc.Encode(rec)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(rec)
	if string(got) != string(want) {
		t.Errorf("json encoder output differs from json.Marshal")
	}
}

func TestOrderProtoSchemaFieldNumbers(t *testing.T) {
	md := orderDescriptor(t)
	for name, num := range map[string]protowire.Number{
		"id": orderFieldID, "ts": orderFieldTS, "region": orderFieldRegion,
		"amount": orderFieldAmount, "status": orderFieldStatus, "payload": orderFieldPayload,
	} {
		f := md.Fields().ByName(protoreflect.Name(name))
		if f == nil || protowire.Number(f.Number()) != num {
			t.Errorf("field %s: schema number %v, encoder constant %d", name, f, num)
		}
	}
	if md.Fields().Len() != 6 {
		t.Errorf("schema has %d fields, encoder knows 6", md.Fields().Len())
	}
}

func TestRegisterOrderSchema_PostsProtobufUnderValueSubject(t *testing.T) {
	srRegisterAttempts, srRegisterBackoff = 1, 0
	var gotPath string
	var gotBody struct {
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// After the POST the client resolves the id's subject/version.
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
			writeFakeSRGet(w, r, "bench-topic-value", 42)
			return
		}
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		_, _ = w.Write([]byte(`{"id": 42}`))
	}))
	defer srv.Close()

	id, err := registerOrderSchema(context.Background(), srv.URL, "bench-topic-value")
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Errorf("id = %d, want 42", id)
	}
	if gotPath != "/subjects/bench-topic-value/versions" {
		t.Errorf("path = %q", gotPath)
	}
	if gotBody.SchemaType != "PROTOBUF" {
		t.Errorf("schemaType = %q, want PROTOBUF", gotBody.SchemaType)
	}
	if gotBody.Schema != orderProtoSchema {
		t.Errorf("registered schema text differs from orderProtoSchema")
	}
}

func TestNewRecordEncoder_FormatSelection(t *testing.T) {
	srRegisterAttempts, srRegisterBackoff = 1, 0
	ctx := context.Background()
	if e, err := newRecordEncoder(ctx, "", "", "t"); err != nil || e == nil {
		t.Errorf("empty format must default to json: %v", err)
	} else if _, ok := e.(jsonEncoder); !ok {
		t.Errorf("default encoder is %T, want jsonEncoder", e)
	}
	if _, err := newRecordEncoder(ctx, formatProtobuf, "", "t"); err == nil {
		t.Error("protobuf without a registry URL must error")
	}
	if _, err := newRecordEncoder(ctx, "avro", "", "t"); err == nil {
		t.Error("unknown format must error")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			writeFakeSRGet(w, r, "t-value", 3)
			return
		}
		_, _ = w.Write([]byte(`{"id": 3}`))
	}))
	defer srv.Close()
	e, err := newRecordEncoder(ctx, formatProtobuf, srv.URL, "t")
	if err != nil {
		t.Fatal(err)
	}
	pe, ok := e.(*protobufEncoder)
	if !ok {
		t.Fatalf("encoder is %T, want *protobufEncoder", e)
	}
	if pe.header[4] != 3 {
		t.Errorf("schema id not baked into header: %v", pe.header)
	}
}

// writeFakeSRGet answers the follow-up lookups franz-go's client makes after
// a schema POST: the id's subject/version list, then the subject version.
func writeFakeSRGet(w http.ResponseWriter, r *http.Request, subj string, id int) {
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	if strings.HasSuffix(r.URL.Path, "/versions") {
		_, _ = w.Write([]byte(`[{"subject":"` + subj + `","version":1}]`))
		return
	}
	_, _ = fmt.Fprintf(w, `{"subject":%q,"version":1,"id":%d,"schema":"x"}`, subj, id)
}
