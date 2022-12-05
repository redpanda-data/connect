package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/gabs/v2"
)

// SliceToDotPath returns a valid dot path from a slice of path segments.
func SliceToDotPath(path ...string) string {
	escapes := make([]string, len(path))
	for i, s := range path {
		s = strings.ReplaceAll(s, "~", "~0")
		s = strings.ReplaceAll(s, ".", "~1")
		escapes[i] = s
	}
	return strings.Join(escapes, ".")
}

//------------------------------------------------------------------------------

// ValueType represents a discrete value type supported by Bloblang queries.
type ValueType string

// ValueType variants.
var (
	ValueString    ValueType = "string"
	ValueBytes     ValueType = "bytes"
	ValueNumber    ValueType = "number"
	ValueBool      ValueType = "bool"
	ValueTimestamp ValueType = "timestamp"
	ValueArray     ValueType = "array"
	ValueObject    ValueType = "object"
	ValueNull      ValueType = "null"
	ValueDelete    ValueType = "delete"
	ValueNothing   ValueType = "nothing"
	ValueQuery     ValueType = "query expression"
	ValueUnknown   ValueType = "unknown"

	// Specialised and not generally known over ValueNumber.
	ValueInt   ValueType = "integer"
	ValueFloat ValueType = "float"
)

// ITypeOf returns the type of a boxed value as a discrete ValueType. If the
// type of the value is unknown then ValueUnknown is returned.
func ITypeOf(i any) ValueType {
	switch i.(type) {
	case string:
		return ValueString
	case []byte:
		return ValueBytes
	case int, int64, uint64, float64, json.Number:
		return ValueNumber
	case bool:
		return ValueBool
	case time.Time:
		return ValueTimestamp
	case []any:
		return ValueArray
	case map[string]any:
		return ValueObject
	case Delete:
		return ValueDelete
	case Nothing:
		return ValueNothing
	case nil:
		return ValueNull
	}
	if _, isDyn := i.(Function); isDyn {
		return ValueQuery
	}
	return ValueUnknown
}

//------------------------------------------------------------------------------

// Delete is a special type that serializes to `null` when forced but indicates
// a target should be deleted.
type Delete *struct{}

// Nothing is a special type that serializes to `null` when forced but indicates
// a query should be disregarded (and not mapped).
type Nothing *struct{}

// IGetNumber takes a boxed value and attempts to extract a number (float64)
// from it.
func IGetNumber(v any) (float64, error) {
	switch t := v.(type) {
	case int:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case uint64:
		return float64(t), nil
	case float64:
		return t, nil
	case json.Number:
		return t.Float64()
	}
	return 0, NewTypeError(v, ValueNumber)
}

// IGetFloat32 takes a boxed value and attempts to extract a number (float32)
// from it.
func IGetFloat32(v any) (float32, error) {
	switch t := v.(type) {
	case int:
		return float32(t), nil
	case int64:
		return float32(t), nil
	case uint64:
		return float32(t), nil
	case float32:
		return t, nil
	case float64:
		return float32(t), nil
	case json.Number:
		v, e := t.Float64()
		return float32(v), e
	}
	return 0, NewTypeError(v, ValueNumber)
}

// IGetInt takes a boxed value and attempts to extract an integer (int64) from
// it.
func IGetInt(v any) (int64, error) {
	switch t := v.(type) {
	case int:
		return int64(t), nil
	case int64:
		return t, nil
	case uint64:
		return int64(t), nil
	case float64:
		return int64(t), nil
	case json.Number:
		i, err := t.Int64()
		if err == nil {
			return i, nil
		}
		if f, ferr := t.Float64(); ferr == nil {
			return int64(f), nil
		}
		return 0, err
	}
	return 0, NewTypeError(v, ValueNumber)
}

// IGetBool takes a boxed value and attempts to extract a boolean from it.
func IGetBool(v any) (bool, error) {
	switch t := v.(type) {
	case bool:
		return t, nil
	case int:
		return t != 0, nil
	case int64:
		return t != 0, nil
	case uint64:
		return t != 0, nil
	case float64:
		return t != 0, nil
	case json.Number:
		return t.String() != "0", nil
	}
	return false, NewTypeError(v, ValueBool)
}

// IGetString takes a boxed value and attempts to return a string value. Returns
// an error if the value is not a string or byte slice.
func IGetString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	case time.Time:
		return t.Format(time.RFC3339Nano), nil
	}
	return "", NewTypeError(v, ValueString)
}

// IGetBytes takes a boxed value and attempts to return a byte slice value.
// Returns an error if the value is not a string or byte slice.
func IGetBytes(v any) ([]byte, error) {
	switch t := v.(type) {
	case string:
		return []byte(t), nil
	case []byte:
		return t, nil
	case time.Time:
		return t.AppendFormat(nil, time.RFC3339Nano), nil
	}
	return nil, NewTypeError(v, ValueBytes)
}

// IGetTimestamp takes a boxed value and attempts to coerce it into a timestamp,
// either by interpretting a numerical value as a unix timestamp, or by parsing
// a string value as RFC3339Nano.
func IGetTimestamp(v any) (time.Time, error) {
	if tVal, ok := v.(time.Time); ok {
		return tVal, nil
	}
	switch t := ISanitize(v).(type) {
	case int64:
		return time.Unix(t, 0), nil
	case uint64:
		return time.Unix(int64(t), 0), nil
	case float64:
		fint := math.Trunc(t)
		fdec := t - fint
		return time.Unix(int64(fint), int64(fdec*1e9)), nil
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return time.Unix(i, 0), nil
		} else if f, err := t.Float64(); err == nil {
			fint := math.Trunc(f)
			fdec := f - fint
			return time.Unix(int64(fint), int64(fdec*1e9)), nil
		} else {
			return time.Time{}, fmt.Errorf("failed to parse value '%v' as number", v)
		}
	case []byte:
		return time.Parse(time.RFC3339Nano, string(t))
	case string:
		return time.Parse(time.RFC3339Nano, t)
	}
	return time.Time{}, NewTypeError(v, ValueNumber, ValueString)
}

// IIsNull returns whether a bloblang type is null, this includes Delete and
// Nothing types.
func IIsNull(i any) bool {
	if i == nil {
		return true
	}
	switch i.(type) {
	case Delete, Nothing:
		return true
	}
	return false
}

func restrictForComparison(v any) any {
	v = ISanitize(v)
	switch t := v.(type) {
	case int64:
		return float64(t)
	case uint64:
		return float64(t)
	case json.Number:
		if f, err := IGetNumber(t); err == nil {
			return f
		}
	case []byte:
		return string(t)
	}
	return v
}

// ISanitize takes a boxed value of any type and attempts to convert it into one
// of the following types: string, []byte, int64, uint64, float64, bool,
// []interface{}, map[string]interface{}, Delete, Nothing.
func ISanitize(i any) any {
	switch t := i.(type) {
	case string, []byte, int64, uint64, float64, bool, []any, map[string]any, Delete, Nothing:
		return i
	case json.RawMessage:
		return []byte(t)
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	case time.Time:
		return t.Format(time.RFC3339Nano)
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case uint32:
		return uint64(t)
	case uint:
		return uint64(t)
	case float32:
		return float64(t)
	}
	// Do NOT support unknown types (for now).
	return nil
}

// IToBytes takes a boxed value of any type and attempts to convert it into a
// byte slice.
func IToBytes(i any) []byte {
	switch t := i.(type) {
	case string:
		return []byte(t)
	case []byte:
		return t
	case json.Number:
		return []byte(t.String())
	case int64:
		return strconv.AppendInt(nil, t, 10)
	case uint64:
		return strconv.AppendUint(nil, t, 10)
	case float64:
		return strconv.AppendFloat(nil, t, 'g', -1, 64)
	case bool:
		if t {
			return []byte("true")
		}
		return []byte("false")
	case time.Time:
		return t.AppendFormat(nil, time.RFC3339Nano)
	case nil:
		return []byte(`null`)
	}
	// Last resort
	return gabs.Wrap(i).Bytes()
}

// IToString takes a boxed value of any type and attempts to convert it into a
// string.
func IToString(i any) string {
	switch t := i.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case json.Number:
		return t.String()
	case bool:
		if t {
			return "true"
		}
		return "false"
	case time.Time:
		return t.Format(time.RFC3339Nano)
	case nil:
		return `null`
	}
	// Last resort
	return gabs.Wrap(i).String()
}

// IToNumber takes a boxed value and attempts to extract a number (float64)
// from it or parse one.
func IToNumber(v any) (float64, error) {
	switch t := v.(type) {
	case int:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case uint64:
		return float64(t), nil
	case float64:
		return t, nil
	case json.Number:
		return t.Float64()
	case []byte:
		return strconv.ParseFloat(string(t), 64)
	case string:
		return strconv.ParseFloat(t, 64)
	}
	return 0, NewTypeError(v, ValueNumber)
}

const (
	maxUint   = ^uint64(0)
	maxUint32 = ^uint32(0)
	maxInt    = maxUint >> 1
	maxInt32  = maxUint32 >> 1
	minInt    = ^int64(maxInt)
	minInt32  = ^int32(maxInt32)
)

// IToInt takes a boxed value and attempts to extract a number (int64) from it
// or parse one.
func IToInt(v any) (int64, error) {
	switch t := v.(type) {
	case int:
		return int64(t), nil
	case int64:
		return t, nil
	case uint64:
		if t > maxInt {
			return 0, errors.New("unsigned integer value is too large to be cast as a signed integer")
		}
		return int64(t), nil
	case float64:
		if math.IsInf(t, 0) {
			return 0, errors.New("cannot convert +/-INF to an integer")
		}
		if math.IsNaN(t) {
			return 0, errors.New("cannot convert NAN to an integer")
		}
		if t > float64(maxInt) {
			return 0, errors.New("float value is too large to be cast as a signed integer")
		}
		if t < float64(minInt) {
			return 0, errors.New("float value is too small to be cast as a signed integer")
		}
		if t-float64(int64(t)) != 0 {
			return 0, errors.New("float value contains decimals and therefore cannot be cast as a signed integer, if you intend to round the value then call `.round()` explicitly before this cast")
		}
		return int64(t), nil
	case json.Number:
		return t.Int64()
	case []byte:
		return strconv.ParseInt(string(t), 10, 64)
	case string:
		return strconv.ParseInt(t, 10, 64)
	}
	return 0, NewTypeError(v, ValueNumber)
}

// IToInt32 takes a boxed value and attempts to extract a number (int32) from
// it or parse one.
func IToInt32(v any) (int32, error) {
	i64, err := IToInt(v)
	if err != nil {
		return 0, err
	}
	if i64 > int64(maxInt32) {
		return 0, errors.New("value is too large to be cast as a 32-bit signed integer")
	}
	if i64 < int64(minInt32) {
		return 0, errors.New("value is too small to be cast as a 32-bit signed integer")
	}
	return int32(i64), nil
}

// IToUint takes a boxed value and attempts to extract a number (uint64) from it
// or parse one.
func IToUint(v any) (uint64, error) {
	switch t := v.(type) {
	case uint64:
		return t, nil
	case int:
		if t < 0 {
			return 0, errors.New("signed integer value is negative and cannot be cast as an unsigned integer")
		}
		return uint64(t), nil
	case int64:
		if t < 0 {
			return 0, errors.New("signed integer value is negative and cannot be cast as an unsigned integer")
		}
		return uint64(t), nil
	case float64:
		if t < 0 {
			return 0, errors.New("float value is negative and cannot be cast as an unsigned integer")
		}
		if math.IsInf(t, 0) {
			return 0, errors.New("cannot convert +/-INF to an unsigned integer")
		}
		if math.IsNaN(t) {
			return 0, errors.New("cannot convert NAN to an unsigned integer")
		}
		if t > float64(maxUint) {
			return 0, errors.New("float value is too large to be cast as an unsigned integer")
		}
		if t-float64(uint64(t)) != 0 {
			return 0, errors.New("float value contains decimals and therefore cannot be cast as an unsigned integer, if you intend to round the value then call `.round()` explicitly before this cast")
		}
		return uint64(t), nil
	case json.Number:
		i, err := t.Int64()
		if err != nil {
			return 0, err
		}
		if i < 0 {
			return 0, errors.New("signed integer value is negative and cannot be cast as an unsigned integer")
		}
		return uint64(i), nil
	case []byte:
		return strconv.ParseUint(string(t), 10, 64)
	case string:
		return strconv.ParseUint(t, 10, 64)
	}
	return 0, NewTypeError(v, ValueNumber)
}

// IToUint32 takes a boxed value and attempts to extract a number (uint32) from
// it or parse one.
func IToUint32(v any) (uint32, error) {
	u64, err := IToUint(v)
	if err != nil {
		return 0, err
	}
	if u64 > uint64(maxUint32) {
		return 0, errors.New("value is too large to be cast as a 32-bit unsigned integer")
	}
	return uint32(u64), nil
}

// IToBool takes a boxed value and attempts to extract a boolean from it or
// parse it into a bool.
func IToBool(v any) (bool, error) {
	switch t := v.(type) {
	case bool:
		return t, nil
	case int:
		return t != 0, nil
	case int64:
		return t != 0, nil
	case uint64:
		return t != 0, nil
	case float64:
		return t != 0, nil
	case json.Number:
		return t.String() != "0", nil
	case []byte:
		if v, err := strconv.ParseBool(string(t)); err == nil {
			return v, nil
		}
	case string:
		if v, err := strconv.ParseBool(t); err == nil {
			return v, nil
		}
	}
	return false, NewTypeError(v, ValueBool)
}

// IClone performs a deep copy of a generic value.
func IClone(root any) any {
	switch t := root.(type) {
	case map[string]any:
		newMap := make(map[string]any, len(t))
		for k, v := range t {
			newMap[k] = IClone(v)
		}
		return newMap
	case []any:
		newSlice := make([]any, len(t))
		for i, v := range t {
			newSlice[i] = IClone(v)
		}
		return newSlice
	}
	return root
}

// ICompare returns true if both the left and right are equal according to one
// of the following conditions:
//
// - The types exactly match and have the same value
// - The types are both either a string or byte slice and the underlying data is the same
// - The types are both numerical and have the same value
// - Both types are a matching slice or map containing values matching these same conditions.
func ICompare(left, right any) bool {
	if left == nil && right == nil {
		return true
	}
	switch lhs := restrictForComparison(left).(type) {
	case string:
		rhs, err := IGetString(right)
		if err != nil {
			return false
		}
		return lhs == rhs
	case float64:
		rhs, err := IGetNumber(right)
		if err != nil {
			return false
		}
		return lhs == rhs
	case bool:
		rhs, err := IGetBool(right)
		if err != nil {
			return false
		}
		return lhs == rhs
	case []any:
		rhs, matches := right.([]any)
		if !matches {
			return false
		}
		if len(lhs) != len(rhs) {
			return false
		}
		for i, vl := range lhs {
			if !ICompare(vl, rhs[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		rhs, matches := right.(map[string]any)
		if !matches {
			return false
		}
		if len(lhs) != len(rhs) {
			return false
		}
		for k, vl := range lhs {
			if !ICompare(vl, rhs[k]) {
				return false
			}
		}
		return true
	}
	return false
}
