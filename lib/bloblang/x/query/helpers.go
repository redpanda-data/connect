package query

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

// Delete is a special type that serializes to `null` but indicates a target
// should be deleted.
type Delete *struct{}

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
	case string:
		return strconv.ParseFloat(t, 64)
	}
	return 0, fmt.Errorf("function returned non-numerical type: %T", v)
}

// ISanitize takes a boxed value of any type and attempts to convert it into one
// of the following types: string, []byte, int64, uint64, float64, bool,
// []interface{}, map[string]interface{}, Delete.
func ISanitize(i interface{}) interface{} {
	switch t := i.(type) {
	case string, []byte, int64, uint64, float64, bool, []interface{}, map[string]interface{}, Delete:
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

//------------------------------------------------------------------------------
