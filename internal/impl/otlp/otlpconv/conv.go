// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlpconv

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"sort"

	"go.opentelemetry.io/collector/pdata/pcommon"

	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
)

// int64ToUint64 safely converts an int64 timestamp to uint64.
// For timestamps, UnixNano() returns int64 but OTLP protobuf expects uint64.
// Negative timestamps are converted to 0.
func int64ToUint64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// uint64ToInt64 converts uint64 timestamp back to int64.
func uint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 { // Max int64
		return math.MaxInt64
	}
	return int64(v)
}

func resourceToRedpanda(src pcommon.Resource) *pb.Resource {
	attrs := src.Attributes()
	if attrs.Len() == 0 {
		return &pb.Resource{}
	}

	return &pb.Resource{
		Attributes:             attributesToRedpanda(attrs),
		DroppedAttributesCount: src.DroppedAttributesCount(),
	}
}

func resourceFromRedpanda(src *pb.Resource, dest pcommon.Resource) {
	if src == nil {
		return
	}
	attributesFromRedpanda(src.Attributes, dest.Attributes())
	dest.SetDroppedAttributesCount(src.DroppedAttributesCount)
}

func scopeToRedpanda(src pcommon.InstrumentationScope) *pb.InstrumentationScope {
	return &pb.InstrumentationScope{
		Name:                   src.Name(),
		Version:                src.Version(),
		Attributes:             attributesToRedpanda(src.Attributes()),
		DroppedAttributesCount: src.DroppedAttributesCount(),
	}
}

func scopeFromRedpanda(src *pb.InstrumentationScope, dest pcommon.InstrumentationScope) {
	if src == nil {
		return
	}
	dest.SetName(src.Name)
	dest.SetVersion(src.Version)
	attributesFromRedpanda(src.Attributes, dest.Attributes())
	dest.SetDroppedAttributesCount(src.DroppedAttributesCount)
}

func attributesToRedpanda(src pcommon.Map) []*pb.KeyValue {
	if src.Len() == 0 {
		return nil
	}

	result := make([]*pb.KeyValue, 0, src.Len())
	src.Range(func(k string, v pcommon.Value) bool {
		result = append(result, &pb.KeyValue{
			Key:   k,
			Value: anyValueToRedpanda(v),
		})
		return true
	})
	return result
}

func attributesFromRedpanda(src []*pb.KeyValue, dest pcommon.Map) {
	if len(src) == 0 {
		return
	}
	for _, kv := range src {
		anyValueFromRedpanda(kv.Value, dest.PutEmpty(kv.Key))
	}
}

func anyValueToRedpanda(src pcommon.Value) *pb.AnyValue {
	switch src.Type() {
	case pcommon.ValueTypeStr:
		return &pb.AnyValue{Value: &pb.AnyValue_StringValue{StringValue: src.Str()}}
	case pcommon.ValueTypeBool:
		return &pb.AnyValue{Value: &pb.AnyValue_BoolValue{BoolValue: src.Bool()}}
	case pcommon.ValueTypeInt:
		return &pb.AnyValue{Value: &pb.AnyValue_IntValue{IntValue: src.Int()}}
	case pcommon.ValueTypeDouble:
		return &pb.AnyValue{Value: &pb.AnyValue_DoubleValue{DoubleValue: src.Double()}}
	case pcommon.ValueTypeBytes:
		return &pb.AnyValue{Value: &pb.AnyValue_BytesValue{BytesValue: src.Bytes().AsRaw()}}
	case pcommon.ValueTypeSlice:
		slice := src.Slice()
		values := make([]*pb.AnyValue, 0, slice.Len())
		for i := range slice.Len() {
			values = append(values, anyValueToRedpanda(slice.At(i)))
		}
		return &pb.AnyValue{Value: &pb.AnyValue_ArrayValue{ArrayValue: &pb.ArrayValue{Values: values}}}
	case pcommon.ValueTypeMap:
		m := src.Map()
		kvList := make([]*pb.KeyValue, 0, m.Len())
		m.Range(func(k string, v pcommon.Value) bool {
			kvList = append(kvList, &pb.KeyValue{
				Key:   k,
				Value: anyValueToRedpanda(v),
			})
			return true
		})
		return &pb.AnyValue{Value: &pb.AnyValue_KvlistValue{KvlistValue: &pb.KeyValueList{Values: kvList}}}
	default:
		// Empty value
		return &pb.AnyValue{}
	}
}

func anyValueFromRedpanda(src *pb.AnyValue, dest pcommon.Value) {
	if src == nil {
		return
	}

	switch v := src.Value.(type) {
	case *pb.AnyValue_StringValue:
		dest.SetStr(v.StringValue)
	case *pb.AnyValue_BoolValue:
		dest.SetBool(v.BoolValue)
	case *pb.AnyValue_IntValue:
		dest.SetInt(v.IntValue)
	case *pb.AnyValue_DoubleValue:
		dest.SetDouble(v.DoubleValue)
	case *pb.AnyValue_BytesValue:
		dest.SetEmptyBytes().FromRaw(v.BytesValue)
	case *pb.AnyValue_ArrayValue:
		if v.ArrayValue == nil {
			return
		}
		slice := dest.SetEmptySlice()
		for _, item := range v.ArrayValue.Values {
			anyValueFromRedpanda(item, slice.AppendEmpty())
		}
	case *pb.AnyValue_KvlistValue:
		if v.KvlistValue == nil {
			return
		}
		m := dest.SetEmptyMap()
		for _, kv := range v.KvlistValue.Values {
			anyValueFromRedpanda(kv.Value, m.PutEmpty(kv.Key))
		}
	}
}

// ResourceHash computes a deterministic hash of a Resource.
func ResourceHash(res *pb.Resource) string {
	if res == nil || len(res.Attributes) == 0 {
		return ""
	}

	h := sha256.New()
	writeSortedAttributes(h, res.Attributes)
	return hex.EncodeToString(h.Sum(nil))
}

// ScopeHash computes a deterministic hash of an InstrumentationScope.
func ScopeHash(scope *pb.InstrumentationScope) string {
	if scope == nil {
		return ""
	}

	h := sha256.New()
	h.Write([]byte("name="))
	h.Write([]byte(scope.Name))
	h.Write([]byte("|version="))
	h.Write([]byte(scope.Version))

	if len(scope.Attributes) > 0 {
		h.Write([]byte("|"))
		writeSortedAttributes(h, scope.Attributes)
	}

	return hex.EncodeToString(h.Sum(nil))
}

func writeSortedAttributes(h io.Writer, attrs []*pb.KeyValue) {
	if len(attrs) == 0 {
		return
	}

	// Copy and sort attributes by key for deterministic hashing
	sorted := make([]*pb.KeyValue, len(attrs))
	copy(sorted, attrs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Key < sorted[j].Key
	})

	for i, kv := range sorted {
		if i > 0 {
			h.Write([]byte("|"))
		}
		h.Write([]byte(kv.Key))
		h.Write([]byte("="))
		writeAnyValue(h, kv.Value)
	}
}

func writeAnyValue(w io.Writer, v *pb.AnyValue) {
	if v == nil {
		w.Write([]byte("nil"))
		return
	}

	switch val := v.Value.(type) {
	case *pb.AnyValue_StringValue:
		w.Write([]byte("s:"))
		w.Write([]byte(val.StringValue))
	case *pb.AnyValue_BoolValue:
		if val.BoolValue {
			w.Write([]byte("b:true"))
		} else {
			w.Write([]byte("b:false"))
		}
	case *pb.AnyValue_IntValue:
		fmt.Fprintf(w, "i:%d", val.IntValue)
	case *pb.AnyValue_DoubleValue:
		fmt.Fprintf(w, "d:%x", math.Float64bits(val.DoubleValue))
	case *pb.AnyValue_BytesValue:
		fmt.Fprintf(w, "bytes:%x", val.BytesValue)
	case *pb.AnyValue_ArrayValue:
		if val.ArrayValue == nil {
			w.Write([]byte("array:nil"))
			return
		}
		w.Write([]byte("array:["))
		for i, item := range val.ArrayValue.Values {
			if i > 0 {
				w.Write([]byte(","))
			}
			writeAnyValue(w, item)
		}
		w.Write([]byte("]"))
	case *pb.AnyValue_KvlistValue:
		if val.KvlistValue == nil {
			w.Write([]byte("kvlist:nil"))
			return
		}
		w.Write([]byte("kvlist:{"))
		for i, kv := range val.KvlistValue.Values {
			if i > 0 {
				w.Write([]byte(","))
			}
			w.Write([]byte(kv.Key))
			w.Write([]byte("="))
			writeAnyValue(w, kv.Value)
		}
		w.Write([]byte("}"))
	default:
		w.Write([]byte("empty"))
	}
}
