package bloblang

import (
	"time"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

// Method defines a Bloblang function that executes on a value. Arguments are
// provided to the constructor, allowing the implementation of this method to
// resolve them statically when possible.
//
// In order to avoid type checking the value use one of the typed variants such
// as StringMethod.
type Method func(v interface{}) (interface{}, error)

// MethodConstructor defines a constructor for a Bloblang method, where a
// variadic list of arguments are provided.
//
// When a method is parsed from a mapping with static arguments the constructor
// will be called only once at parse time. When a method is parsed with dynamic
// arguments, such as a value derived from the mapping input, the constructor
// will be called on each invocation of the mapping with the derived arguments.
//
// For a convenient way to perform type checking and coercion on the arguments
// use an ArgSpec.
type MethodConstructor func(args ...interface{}) (Method, error)

// MethodConstructorV2 defines a constructor for a Bloblang method where
// parameters are parsed using a ParamsSpec provided when registering the
// method.
//
// When a method is parsed from a mapping with static arguments the constructor
// will be called only once at parse time. When a method is parsed with dynamic
// arguments, such as a value derived from the mapping input, the constructor
// will be called on each invocation of the mapping with the derived arguments.
type MethodConstructorV2 func(args *ParsedParams) (Method, error)

//------------------------------------------------------------------------------

// StringMethod creates a general method signature from a string method by
// performing type checking on the method target.
func StringMethod(methodFn func(string) (interface{}, error)) Method {
	return func(v interface{}) (interface{}, error) {
		str, err := query.IGetString(v)
		if err != nil {
			return nil, err
		}
		return methodFn(str)
	}
}

// BytesMethod creates a general method signature from a byte slice method by
// performing type checking on the method target.
func BytesMethod(methodFn func([]byte) (interface{}, error)) Method {
	return func(v interface{}) (interface{}, error) {
		b, err := query.IGetBytes(v)
		if err != nil {
			return nil, err
		}
		return methodFn(b)
	}
}

// TimestampMethod creates a general method signature from a timestamp method by
// performing type checking on the method target.
func TimestampMethod(methodFn func(time.Time) (interface{}, error)) Method {
	return func(v interface{}) (interface{}, error) {
		t, err := query.IGetTimestamp(v)
		if err != nil {
			return nil, err
		}
		return methodFn(t)
	}
}

// ArrayMethod creates a general method signature from an array method by
// performing type checking on the method target.
func ArrayMethod(methodFn func([]interface{}) (interface{}, error)) Method {
	return func(v interface{}) (interface{}, error) {
		arr, ok := v.([]interface{})
		if !ok {
			return nil, query.NewTypeError(v, query.ValueArray)
		}
		return methodFn(arr)
	}
}

// BoolMethod creates a general method signature from a bool method by
// performing type checking on the method target.
func BoolMethod(methodFn func(bool) (interface{}, error)) Method {
	return func(v interface{}) (interface{}, error) {
		b, err := query.IGetBool(v)
		if err != nil {
			return nil, err
		}
		return methodFn(b)
	}
}

// Int64Method creates a general method signature from an int method by
// performing type checking on the method target.
func Int64Method(methodFn func(int64) (interface{}, error)) Method {
	return func(v interface{}) (interface{}, error) {
		i, err := query.IGetInt(v)
		if err != nil {
			return nil, err
		}
		return methodFn(i)
	}
}

// Float64Method creates a general method signature from a float method by
// performing type checking on the method target.
func Float64Method(methodFn func(float64) (interface{}, error)) Method {
	return func(v interface{}) (interface{}, error) {
		f, err := query.IGetNumber(v)
		if err != nil {
			return nil, err
		}
		return methodFn(f)
	}
}

// ObjectMethod creates a general method signature from an object method by
// performing type checking on the method target.
func ObjectMethod(methodFn func(obj map[string]interface{}) (interface{}, error)) Method {
	return func(v interface{}) (interface{}, error) {
		obj, ok := v.(map[string]interface{})
		if !ok {
			return nil, query.NewTypeError(v, query.ValueObject)
		}
		return methodFn(obj)
	}
}
