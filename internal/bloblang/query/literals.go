package query

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/value"
)

var _ Function = &mapLiteral{}

type mapLiteral struct {
	keyValues [][2]any
}

// NewMapLiteral creates a map literal from a slice of key/value pairs. If all
// keys and values are static then a static map[string]interface{} value is
// returned. However, if any keys or values are dynamic a Function is returned.
func NewMapLiteral(values [][2]any) (any, error) {
	isDynamic := false
	staticValues := make(map[string]any, len(values))
	for i, kv := range values {
		var key string
		switch t := kv[0].(type) {
		case string:
			key = t
		case *Literal:
			var isStr bool
			if key, isStr = t.Value.(string); !isStr {
				return nil, fmt.Errorf("object keys must be strings, received: %T", t.Value)
			}
			values[i][0] = key
		case Function:
			isDynamic = true
		default:
			return nil, fmt.Errorf("object keys must be strings, received: %T", t)
		}
		switch t := kv[1].(type) {
		case *Literal:
			values[i][1] = t.Value
			if !isDynamic {
				switch t.Value.(type) {
				case value.Delete, value.Nothing:
				default:
					staticValues[key] = t.Value
				}
			}
		case Function:
			isDynamic = true
		default:
			if !isDynamic {
				switch kv[1].(type) {
				case value.Delete, value.Nothing:
				default:
					staticValues[key] = kv[1]
				}
			}
		}
	}
	if isDynamic {
		return &mapLiteral{keyValues: values}, nil
	}
	return staticValues, nil
}

func (m *mapLiteral) Annotation() string {
	return "object literal"
}

func (m *mapLiteral) Exec(ctx FunctionContext) (any, error) {
	dynMap := make(map[string]any, len(m.keyValues))
	for _, kv := range m.keyValues {
		var key string
		var val any

		var err error
		switch t := kv[0].(type) {
		case string:
			key = t
		case Function:
			var keyI any
			if keyI, err = t.Exec(ctx); err != nil {
				return nil, fmt.Errorf("failed to resolve key: %w", err)
			}
			switch t2 := keyI.(type) {
			case string:
				key = t2
			case []byte:
				key = string(t2)
			default:
				return nil, fmt.Errorf("mapping returned invalid key type: %T", keyI)
			}
		default:
			return nil, fmt.Errorf("invalid key type: %T", kv[0])
		}

		if fn, isFunction := kv[1].(Function); isFunction {
			if val, err = fn.Exec(ctx); err != nil {
				return nil, fmt.Errorf("failed to resolve '%v' value: %w", key, err)
			}
		} else {
			val = kv[1]
		}

		switch val.(type) {
		case value.Delete, value.Nothing:
		default:
			dynMap[key] = val
		}
	}
	return dynMap, nil
}

func (m *mapLiteral) QueryTargets(ctx TargetsContext) (TargetsContext, []TargetPath) {
	var targetPaths []TargetPath
	for _, kv := range m.keyValues {
		if fn, ok := kv[0].(Function); ok {
			_, paths := fn.QueryTargets(ctx)
			targetPaths = append(targetPaths, paths...)
		}
		if fn, ok := kv[1].(Function); ok {
			_, paths := fn.QueryTargets(ctx)
			targetPaths = append(targetPaths, paths...)
		}
	}
	// TODO: Mark next context with aliases?
	return ctx, targetPaths
}

//------------------------------------------------------------------------------

var _ Function = &arrayLiteral{}

type arrayLiteral struct {
	values []any
}

// NewArrayLiteral creates an array literal from a slice of values. If all
// values are static then a static []interface{} value is returned. However, if
// any values are dynamic a Function is returned.
func NewArrayLiteral(values ...Function) any {
	var expandedValues []any
	isDynamic := false
	for _, v := range values {
		switch t := v.(type) {
		case *Literal:
			switch t.Value.(type) {
			case value.Delete, value.Nothing:
			default:
				expandedValues = append(expandedValues, t.Value)
			}
		case Function:
			isDynamic = true
			expandedValues = append(expandedValues, v)
		default:
			expandedValues = append(expandedValues, v)
		}
	}
	if !isDynamic {
		return expandedValues
	}
	return &arrayLiteral{expandedValues}
}

func (a *arrayLiteral) Annotation() string {
	return "array literal"
}

func (a *arrayLiteral) Exec(ctx FunctionContext) (any, error) {
	dynArray := make([]any, 0, len(a.values))
	for _, v := range a.values {
		if fn, isFunction := v.(Function); isFunction {
			var err error
			if v, err = fn.Exec(ctx); err != nil {
				return nil, err
			}
		}
		switch v.(type) {
		case value.Delete, value.Nothing:
		default:
			dynArray = append(dynArray, v)
		}
	}
	return dynArray, nil
}

func (a *arrayLiteral) QueryTargets(ctx TargetsContext) (TargetsContext, []TargetPath) {
	var targetPaths []TargetPath
	for _, v := range a.values {
		if fn, ok := v.(Function); ok {
			_, paths := fn.QueryTargets(ctx)
			targetPaths = append(targetPaths, paths...)
		}
	}
	// TODO: Mark next context with aliases?
	return ctx, targetPaths
}
