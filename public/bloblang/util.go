package bloblang

import (
	"time"

	"github.com/benthosdev/benthos/v4/internal/value"
)

// ValueToString converts any value into a string according to the same rules
// that other native benthos components including bloblang would follow, where
// simple value types are stringified, but complex types are converted into JSON
// marshalled as a string.
func ValueToString(v any) string {
	return value.IToString(v)
}

// ValueAsBytes takes a boxed value and attempts to return a byte slice value.
// Returns an error if the value is not a string or byte slice.
func ValueAsBytes(v any) ([]byte, error) {
	return value.IGetBytes(v)
}

// ValueAsTimestamp takes a boxed value and attempts to coerce it into a
// timestamp, either by interpretting a numerical value as a unix timestamp, or
// by parsing a string value as RFC3339Nano.
func ValueAsTimestamp(v any) (time.Time, error) {
	return value.IGetTimestamp(v)
}

// ValueAsInt64 takes a boxed value and attempts to extract a number from it.
func ValueAsInt64(v any) (int64, error) {
	return value.IGetInt(v)
}

// ValueAsFloat64 takes a boxed value and attempts to extract a number from it.
func ValueAsFloat64(v any) (float64, error) {
	return value.IGetNumber(v)
}

// ValueAsFloat32 takes a boxed value and attempts to extract a number from it.
func ValueAsFloat32(v any) (float32, error) {
	return value.IGetFloat32(v)
}

// ValueSanitized takes a boxed value of any type and attempts to convert it
// into one of the following types: string, []byte, int64, uint64, float64,
// bool, []interface{}, map[string]interface{}, Delete, Nothing.
func ValueSanitized(i any) any {
	return value.ISanitize(i)
}
