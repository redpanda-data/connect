package docs

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/value"
)

// GetInferenceCandidateFromMap checks a map config structure for a component
// and returns either the inferred type name or an error if one cannot be
// inferred.
func GetInferenceCandidateFromMap(docProv Provider, t Type, m map[string]any) (string, ComponentSpec, error) {
	if tStr, ok := m["type"].(string); ok {
		spec, exists := docProv.GetDocs(tStr, t)
		if !exists {
			return "", ComponentSpec{}, fmt.Errorf("%v type '%v' was not recognised", string(t), tStr)
		}
		return tStr, spec, nil
	}

	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	return getInferenceCandidateFromList(docProv, t, keys)
}

// AnyToValue converts any value into a generic value containing fleshed out
// defaults by referencing the expected type.
func (f FieldSpec) AnyToValue(v any, conf ToValueConfig) (any, error) {
	switch f.Kind {
	case Kind2DArray:
		a, ok := v.([]any)
		if !conf.Passive && !ok {
			return nil, fmt.Errorf("expected array value, got %T", v)
		}
		subSpec := f.Array()

		var s []any
		for i := 0; i < len(a); i++ {
			v, err := subSpec.AnyToValue(a[i], conf)
			if err != nil {
				return nil, err
			}
			s = append(s, v)
		}
		return s, nil
	case KindArray:
		a, ok := v.([]any)
		if !conf.Passive && !ok {
			return nil, fmt.Errorf("expected array value, got %T", v)
		}
		subSpec := f.Scalar()

		var s []any
		for i := 0; i < len(a); i++ {
			v, err := subSpec.AnyToValue(a[i], conf)
			if err != nil {
				return nil, err
			}
			s = append(s, v)
		}
		return s, nil
	case KindMap:
		m, ok := v.(map[string]any)
		if !conf.Passive && !ok {
			return nil, fmt.Errorf("expected map value, got %T", v)
		}
		subSpec := f.Scalar()

		for k, v := range m {
			var err error
			if m[k], err = subSpec.AnyToValue(v, conf); err != nil {
				return nil, err
			}
		}
		return m, nil
	}
	switch f.Type {
	case FieldTypeString:
		return value.IGetString(v)
	case FieldTypeInt:
		i64, err := value.IGetInt(v)
		return int(i64), err
	case FieldTypeFloat:
		return value.IGetNumber(v)
	case FieldTypeBool:
		return value.IGetBool(v)
	case FieldTypeUnknown:
		return v, nil
	case FieldTypeObject:
		return f.Children.AnyToMap(v, conf)
	}
	return v, nil
}

// AnyToMap converts a raw map value node into a generic map structure
// referencing expected fields, adding default values to the map when the node
// does not contain them.
func (f FieldSpecs) AnyToMap(v any, conf ToValueConfig) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map value, got %T", v)
	}

	pendingFieldsMap := map[string]FieldSpec{}
	for _, field := range f {
		pendingFieldsMap[field.Name] = field
	}

	for fieldName, fieldValue := range m {
		f, exists := pendingFieldsMap[fieldName]
		if !exists {
			continue
		}

		delete(pendingFieldsMap, f.Name)
		var err error
		if m[fieldName], err = f.AnyToValue(fieldValue, conf); err != nil {
			return nil, fmt.Errorf("field '%v': %w", fieldName, err)
		}
	}

	for k, v := range pendingFieldsMap {
		defValue, err := getDefault(k, v)
		if err != nil {
			if v.needsDefault() && !conf.Passive {
				return nil, err
			}
			continue
		}
		m[k] = defValue
	}

	return m, nil
}
