package docs

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// FieldsFromNode walks the children of a YAML node and returns a list of fields
// extracted from it. This can be used in order to infer a field spec for a
// parsed component.
func FieldsFromNode(node *yaml.Node) FieldSpecs {
	var fields FieldSpecs
	for i := 0; i < len(node.Content)-1; i += 2 {
		field := FieldCommon(node.Content[i].Value, "")
		if len(node.Content[i+1].Content) > 0 {
			field = field.WithChildren(FieldsFromNode(node.Content[i+1])...)
		}
		fields = append(fields, field)
	}
	return fields
}

// ToNode creates a YAML node from a field spec. If a default value has been
// specified then it is used. Otherwise, a zero value is generated. If recurse
// is enabled and the field has children then all children will also have values
// generated.
func (f FieldSpec) ToNode(recurse bool) (*yaml.Node, error) {
	var node yaml.Node
	if f.Default != nil {
		if err := node.Encode(*f.Default); err != nil {
			return nil, err
		}
		return &node, nil
	}
	if f.IsArray {
		s := []interface{}{}
		if err := node.Encode(s); err != nil {
			return nil, err
		}
	} else if f.IsMap || len(f.Children) > 0 {
		if len(f.Children) > 0 && recurse {
			return f.Children.ToNode()
		}
		s := map[string]interface{}{}
		if err := node.Encode(s); err != nil {
			return nil, err
		}
	} else {
		switch f.Type {
		case FieldString:
			if err := node.Encode(""); err != nil {
				return nil, err
			}
		case FieldInt:
			if err := node.Encode(0); err != nil {
				return nil, err
			}
		case FieldFloat:
			if err := node.Encode(0.0); err != nil {
				return nil, err
			}
		case FieldBool:
			if err := node.Encode(false); err != nil {
				return nil, err
			}
		default:
			if err := node.Encode(nil); err != nil {
				return nil, err
			}
		}
	}
	return &node, nil
}

// NodeToValue converts a yaml node into a generic value by referencing the
// expected type.
func (f FieldSpec) NodeToValue(node *yaml.Node) (interface{}, error) {
	if f.IsArray {
		switch f.Type {
		case FieldString:
			var s []string
			if err := node.Decode(&s); err != nil {
				return nil, err
			}
			si := make([]interface{}, len(s))
			for i, v := range s {
				si[i] = v
			}
			return si, nil
		case FieldInt:
			var ints []int
			if err := node.Decode(&ints); err != nil {
				return nil, err
			}
			ii := make([]interface{}, len(ints))
			for i, v := range ints {
				ii[i] = v
			}
			return ii, nil
		case FieldFloat:
			var f []float64
			if err := node.Decode(&f); err != nil {
				return nil, err
			}
			fi := make([]interface{}, len(f))
			for i, v := range f {
				fi[i] = v
			}
			return fi, nil
		case FieldBool:
			var b []bool
			if err := node.Decode(&b); err != nil {
				return nil, err
			}
			bi := make([]interface{}, len(b))
			for i, v := range b {
				bi[i] = v
			}
			return bi, nil
		case FieldObject:
			var c []yaml.Node
			if err := node.Decode(&c); err != nil {
				return nil, err
			}
			ci := make([]interface{}, len(c))
			for i, v := range c {
				var err error
				if ci[i], err = f.Children.NodeToMap(&v); err != nil {
					return nil, err
				}
			}
			return ci, nil
		}
	} else if f.IsMap {
		switch f.Type {
		case FieldString:
			var s map[string]string
			if err := node.Decode(&s); err != nil {
				return nil, err
			}
			si := make(map[string]interface{}, len(s))
			for k, v := range s {
				si[k] = v
			}
			return si, nil
		case FieldInt:
			var ints map[string]int
			if err := node.Decode(&ints); err != nil {
				return nil, err
			}
			ii := make(map[string]interface{}, len(ints))
			for k, v := range ints {
				ii[k] = v
			}
			return ii, nil
		case FieldFloat:
			var f map[string]float64
			if err := node.Decode(&f); err != nil {
				return nil, err
			}
			fi := make(map[string]interface{}, len(f))
			for k, v := range f {
				fi[k] = v
			}
			return fi, nil
		case FieldBool:
			var b map[string]bool
			if err := node.Decode(&b); err != nil {
				return nil, err
			}
			bi := make(map[string]interface{}, len(b))
			for k, v := range b {
				bi[k] = v
			}
			return bi, nil
		case FieldObject:
			var c map[string]yaml.Node
			if err := node.Decode(&c); err != nil {
				return nil, err
			}
			ci := make(map[string]interface{}, len(c))
			for k, v := range c {
				var err error
				if ci[k], err = f.Children.NodeToMap(&v); err != nil {
					return nil, err
				}
			}
			return ci, nil
		}
	}
	switch f.Type {
	case FieldString:
		var s string
		if err := node.Decode(&s); err != nil {
			return nil, err
		}
		return s, nil
	case FieldInt:
		var i int
		if err := node.Decode(&i); err != nil {
			return nil, err
		}
		return i, nil
	case FieldFloat:
		var f float64
		if err := node.Decode(&f); err != nil {
			return nil, err
		}
		return f, nil
	case FieldBool:
		var b bool
		if err := node.Decode(&b); err != nil {
			return nil, err
		}
		return b, nil
	case FieldObject:
		return f.Children.NodeToMap(node)
	}
	var v interface{}
	if err := node.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// ToNode creates a YAML node from a list of field specs. If a default value has
// been specified for a given field then it is used. Otherwise, a zero value is
// generated.
func (f FieldSpecs) ToNode() (*yaml.Node, error) {
	var node yaml.Node
	node.Kind = yaml.MappingNode

	for _, spec := range f {
		var keyNode yaml.Node
		if err := keyNode.Encode(spec.Name); err != nil {
			return nil, err
		}
		valueNode, err := spec.ToNode(true)
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, &keyNode, valueNode)
	}

	return &node, nil
}

// NodeToMap converts a yaml node into a generic map structure by referencing
// expected fields, adding default values to the map when the node does not
// contain them.
func (f FieldSpecs) NodeToMap(node *yaml.Node) (map[string]interface{}, error) {
	pendingFieldsMap := map[string]FieldSpec{}
	for _, field := range f {
		pendingFieldsMap[field.Name] = field
	}

	resultMap := map[string]interface{}{}

	for i := 0; i < len(node.Content)-1; i += 2 {
		fieldName := node.Content[i].Value

		if f, exists := pendingFieldsMap[fieldName]; exists {
			delete(pendingFieldsMap, f.Name)
			var err error
			if resultMap[fieldName], err = f.NodeToValue(node.Content[i+1]); err != nil {
				return nil, fmt.Errorf("field '%v': %w", fieldName, err)
			}
		} else {
			var v interface{}
			if err := node.Content[i+1].Decode(&v); err != nil {
				return nil, err
			}
			resultMap[fieldName] = v
		}
	}

	for k, v := range pendingFieldsMap {
		var err error
		if resultMap[k], err = getDefault(k, v); err != nil {
			return nil, err
		}
	}

	return resultMap, nil
}
