package query

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

// ValueType represents a discrete value type supported by Bloblang queries.
type ValueType string

// ValueType variants.
var (
	ValueString  ValueType = "string"
	ValueBytes   ValueType = "bytes"
	ValueNumber  ValueType = "number"
	ValueBool    ValueType = "bool"
	ValueArray   ValueType = "array"
	ValueObject  ValueType = "object"
	ValueNull    ValueType = "null"
	ValueDelete  ValueType = "delete"
	ValueNothing ValueType = "nothing"
	ValueUnknown ValueType = "unknown"
)

// ITypeOf returns the type of a boxed value as a discrete ValueType. If the
// type of the value is unknown then ValueUnknown is returned.
func ITypeOf(i interface{}) ValueType {
	switch i.(type) {
	case string:
		return ValueString
	case []byte:
		return ValueBytes
	case int64, uint64, float64:
		return ValueNumber
	case bool:
		return ValueBool
	case []interface{}:
		return ValueArray
	case map[string]interface{}:
		return ValueObject
	case Delete:
		return ValueDelete
	case Nothing:
		return ValueNothing
	case nil:
		return ValueNull
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
func IGetNumber(v interface{}) (float64, error) {
	switch t := v.(type) {
	case int64:
		return float64(t), nil
	case uint64:
		return float64(t), nil
	case float64:
		return t, nil
	}
	return 0, NewTypeError(v, ValueNumber)
}

// IGetInt takes a boxed value and attempts to extract an integer (int64) from
// it.
func IGetInt(v interface{}) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case uint64:
		return int64(t), nil
	case float64:
		return int64(t), nil
	}
	return 0, NewTypeError(v, ValueNumber)
}

// IGetBool takes a boxed value and attempts to extract a boolean from it.
func IGetBool(v interface{}) (bool, error) {
	switch t := v.(type) {
	case bool:
		return t, nil
	case int64:
		return t > 0, nil
	case uint64:
		return t > 0, nil
	case float64:
		return t > 0, nil
	}
	return false, NewTypeError(v, ValueBool)
}

// IGetString takes a boxed value and attempts to return a string value. Returns
// an error if the value is not a string or byte slice.
func IGetString(v interface{}) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	}
	return "", NewTypeError(v, ValueString)
}

// IIsNull returns whether a bloblang type is null, this includes Delete and
// Nothing types.
func IIsNull(i interface{}) bool {
	if i == nil {
		return true
	}
	switch i.(type) {
	case Delete, Nothing:
		return true
	}
	return false
}

// ISanitize takes a boxed value of any type and attempts to convert it into one
// of the following types: string, []byte, int64, uint64, float64, bool,
// []interface{}, map[string]interface{}, Delete, Nothing.
func ISanitize(i interface{}) interface{} {
	switch t := i.(type) {
	case string, []byte, int64, uint64, float64, bool, []interface{}, map[string]interface{}, Delete, Nothing:
		return i
	case json.RawMessage:
		return []byte(t)
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return int(i)
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
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
func IToBytes(i interface{}) []byte {
	switch t := i.(type) {
	case string:
		return []byte(t)
	case []byte:
		return t
	case int64, uint64, float64:
		return []byte(fmt.Sprintf("%v", t)) // TODO
	case bool:
		if t {
			return []byte("true")
		}
		return []byte("false")
	case nil:
		return []byte(`null`)
	}
	// Last resort
	return gabs.Wrap(i).Bytes()
}

// IToString takes a boxed value of any type and attempts to convert it into a
// string.
func IToString(i interface{}) string {
	switch t := i.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int64, uint64, float64:
		return fmt.Sprintf("%v", t) // TODO
	case bool:
		if t {
			return "true"
		}
		return "false"
	case nil:
		return `null`
	}
	// Last resort
	return gabs.Wrap(i).String()
}

// IToNumber takes a boxed value and attempts to extract a number (float64)
// from it or parse one.
func IToNumber(v interface{}) (float64, error) {
	switch t := v.(type) {
	case int64:
		return float64(t), nil
	case uint64:
		return float64(t), nil
	case float64:
		return t, nil
	case []byte:
		return strconv.ParseFloat(string(t), 64)
	case string:
		return strconv.ParseFloat(t, 64)
	}
	return 0, NewTypeError(v, ValueNumber)
}

const maxUint = ^uint64(0)
const maxInt = uint64(maxUint >> 1)

// IToInt takes a boxed value and attempts to extract a number (int64) from it
// or parse one.
func IToInt(v interface{}) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case uint64:
		if t > maxInt {
			return 0, errors.New("unsigned integer value is too large to be cast as a signed integer")
		}
		return int64(t), nil
	case float64:
		return int64(t), nil
	case []byte:
		return strconv.ParseInt(string(t), 10, 64)
	case string:
		return strconv.ParseInt(t, 10, 64)
	}
	return 0, NewTypeError(v, ValueNumber)
}

// IToBool takes a boxed value and attempts to extract a boolean from it or
// parse it into a bool.
func IToBool(v interface{}) (bool, error) {
	switch t := v.(type) {
	case bool:
		return t, nil
	case int64:
		return t > 0, nil
	case uint64:
		return t > 0, nil
	case float64:
		return t > 0, nil
	case []byte:
		if bytes.Equal(t, []byte("true")) {
			return true, nil
		} else if bytes.Equal(t, []byte("false")) {
			return false, nil
		}
	case string:
		switch t {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
	}
	return false, NewTypeError(v, ValueBool)
}

// IClone performs a deep copy of a generic value.
func IClone(root interface{}) interface{} {
	switch t := root.(type) {
	case map[string]interface{}:
		newMap := make(map[string]interface{}, len(t))
		for k, v := range t {
			newMap[k] = IClone(v)
		}
		return newMap
	case []interface{}:
		newSlice := make([]interface{}, len(t))
		for i, v := range t {
			newSlice[i] = IClone(v)
		}
		return newSlice
	}
	return root
}

//------------------------------------------------------------------------------
