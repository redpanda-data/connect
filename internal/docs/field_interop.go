package docs

import (
	"gopkg.in/yaml.v3"
)

// ComponentFieldsFromConf walks the children of a YAML node and returns a list
// of fields extracted from it. This can be used in order to infer a field spec
// for a parsed component.
//
// TODO: V5 Remove this eventually.
func ComponentFieldsFromConf(conf any) (inferred map[string]FieldSpecs) {
	inferred = map[string]FieldSpecs{}

	componentNodes := map[string]yaml.Node{}

	var node yaml.Node
	if err := node.Encode(conf); err != nil {
		return
	}

	if err := node.Decode(componentNodes); err != nil {
		return
	}

	for k, v := range componentNodes {
		inferred[k] = FieldsFromYAML(&v)
	}
	return
}

// FieldsFromConf attempts to infer field documents from a config struct.
func FieldsFromConf(conf any) FieldSpecs {
	var node yaml.Node
	if err := node.Encode(conf); err != nil {
		return FieldSpecs{}
	}
	return FieldsFromYAML(&node)
}

// ChildDefaultAndTypesFromStruct enriches a field specs children with a type
// string and default value from another field spec inferred from a config
// struct.
//
// TODO: V5 Remove this eventually.
func (f FieldSpec) ChildDefaultAndTypesFromStruct(conf any) FieldSpec {
	var node yaml.Node
	if err := node.Encode(conf); err != nil {
		return f
	}
	f.Children = f.Children.DefaultAndTypeFrom(FieldsFromYAML(&node))
	return f
}

// DefaultAndTypeFrom enriches a field spec with a type string and default value
// from another field spec.
func (f FieldSpec) DefaultAndTypeFrom(from FieldSpec) FieldSpec {
	if f.Default == nil && from.Default != nil {
		f.Default = from.Default
	}
	if f.Type == "" && from.Type != "" {
		f.Type = from.Type
	}
	f.Children = f.Children.DefaultAndTypeFrom(from.Children)
	return f
}

// DefaultAndTypeFrom enriches a field spec with a type string and default value
// from another field spec.
func (f FieldSpecs) DefaultAndTypeFrom(from FieldSpecs) FieldSpecs {
	newSpecs := make(FieldSpecs, len(f))
	fromMap := map[string]FieldSpec{}
	for _, v := range from {
		fromMap[v.Name] = v
	}
	for i, v := range f {
		ref, exists := fromMap[v.Name]
		if !exists {
			newSpecs[i] = v
			continue
		}
		newSpecs[i] = v.DefaultAndTypeFrom(ref)
	}
	return newSpecs
}
