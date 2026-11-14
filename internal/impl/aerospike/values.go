// Copyright 2026 Redpanda Data, Inc.
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

package aerospike

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

// maxBinNameLen is a hard server limit. Exceeding it is rejected with
// BIN_NAME_TOO_LONG, so components check it locally where the error can name
// the offending field.
const maxBinNameLen = 15

// defaultTombstoneBin marks a fenced delete. The record is kept so the fence
// survives; readers should treat a record carrying this bin as missing.
const defaultTombstoneBin = "_deleted"

// defaultFenceBin holds the monotonic fence value written alongside a record.
// It is bookkeeping rather than data.
const defaultFenceBin = "_fence"

// validateBinName checks a bin name against the server's length limit, so the
// error can name the field that produced it rather than surfacing as a
// BIN_NAME_TOO_LONG result code.
func validateBinName(name string) error {
	if name == "" {
		return errors.New("bin name must not be empty")
	}
	if len(name) > maxBinNameLen {
		return fmt.Errorf("bin name %q is %d bytes, which exceeds the Aerospike limit of %d", name, len(name), maxBinNameLen)
	}
	return nil
}

// toAerospike normalises a decoded JSON value into something the client stores
// with the intended type.
//
// The case that matters is numbers: JSON has one number type, so an identifier
// or a counter arrives as a float64 and would be stored as an Aerospike double.
// That breaks integer comparisons, `add` operations and integer secondary
// indexes, so integral values become int64 when coerceInts is set.
func toAerospike(v any, coerceInts bool) (any, error) {
	switch t := v.(type) {
	case nil:
		// A nil bin value deletes the bin from the record.
		return nil, nil
	case bool, string, []byte:
		return t, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32:
		return t, nil
	case uint64:
		if t > math.MaxInt64 {
			return nil, fmt.Errorf("value %d exceeds the maximum Aerospike integer", t)
		}
		return int64(t), nil
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, nil
		}
		f, err := t.Float64()
		if err != nil {
			return nil, fmt.Errorf("value %q is not a valid number: %w", t.String(), err)
		}
		return f, nil
	case float32:
		return convertFloat(float64(t), coerceInts), nil
	case float64:
		return convertFloat(t, coerceInts), nil
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			conv, err := toAerospike(val, coerceInts)
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", k, err)
			}
			out[k] = conv
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			conv, err := toAerospike(val, coerceInts)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", i, err)
			}
			out[i] = conv
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported value of type %T", v)
	}
}

func convertFloat(f float64, coerceInts bool) any {
	if !coerceInts {
		return f
	}
	if math.IsInf(f, 0) || math.IsNaN(f) || f != math.Trunc(f) {
		return f
	}
	if f < math.MinInt64 || f > math.MaxInt64 {
		return f
	}
	return int64(f)
}

// estimateSize returns a conservative byte count for a value about to be stored
// as a bin. It is not the on-wire size — it exists so a mapping can be rejected
// before the server returns RECORD_TOO_BIG. Nested lists and maps are walked
// because an unbounded collection is how records grow past the I/O budget.
func estimateSize(v any) int {
	switch t := v.(type) {
	case nil:
		return 0
	case bool:
		return 1
	case string:
		return len(t)
	case []byte:
		return len(t)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return 8
	case map[string]any:
		n := 0
		for k, val := range t {
			n += len(k) + estimateSize(val)
		}
		return n
	case []any:
		n := 0
		for _, val := range t {
			n += estimateSize(val)
		}
		return n
	default:
		return len(fmt.Sprint(t))
	}
}
