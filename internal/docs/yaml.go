package docs

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// FieldsFromYAML walks the children of a YAML node and returns a list of fields
// extracted from it. This can be used in order to infer a field spec for a
// parsed component.
func FieldsFromYAML(node *yaml.Node) FieldSpecs {
	node = unwrapDocumentNode(node)

	var fields FieldSpecs
	for i := 0; i < len(node.Content)-1; i += 2 {
		fields = append(fields, FieldFromYAML(node.Content[i].Value, node.Content[i+1]))
	}
	return fields
}

// FieldFromYAML infers a field spec from a YAML node. This mechanism has many
// limitations and should only be used for pre-hydrating field specs for old
// components with struct based config.
func FieldFromYAML(name string, node *yaml.Node) FieldSpec {
	node = unwrapDocumentNode(node)

	field := FieldCommon(name, "")

	switch node.Kind {
	case yaml.MappingNode:
		field = field.WithChildren(FieldsFromYAML(node)...)
		field.Type = FieldTypeObject
		if len(field.Children) == 0 {
			var defaultI interface{} = map[string]interface{}{}
			field.Default = &defaultI
		}
	case yaml.SequenceNode:
		field.Kind = KindArray
		field.Type = FieldTypeUnknown
		if len(node.Content) > 0 {
			tmpField := FieldFromYAML("", node.Content[0])
			field.Type = tmpField.Type
			field.Children = tmpField.Children
			switch field.Type {
			case FieldTypeString:
				var defaultArray []string
				_ = node.Decode(&defaultArray)

				var defaultI interface{} = defaultArray
				field.Default = &defaultI
			case FieldTypeInt:
				var defaultArray []int64
				_ = node.Decode(&defaultArray)

				var defaultI interface{} = defaultArray
				field.Default = &defaultI
			}
		} else {
			var defaultI interface{} = []interface{}{}
			field.Default = &defaultI
		}
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!bool":
			field.Type = FieldTypeBool

			var defaultBool bool
			_ = node.Decode(&defaultBool)

			var defaultI interface{} = defaultBool
			field.Default = &defaultI
		case "!!int":
			field.Type = FieldTypeInt

			var defaultInt int64
			_ = node.Decode(&defaultInt)

			var defaultI interface{} = defaultInt
			field.Default = &defaultI
		case "!!float":
			field.Type = FieldTypeFloat

			var defaultFloat float64
			_ = node.Decode(&defaultFloat)

			var defaultI interface{} = defaultFloat
			field.Default = &defaultI
		default:
			field.Type = FieldTypeString

			var defaultStr string
			_ = node.Decode(&defaultStr)

			var defaultI interface{} = defaultStr
			field.Default = &defaultI
		}
	}

	return field
}

// GetInferenceCandidateFromYAML checks a yaml node config structure for a
// component and returns either the inferred type name or an error if one cannot
// be inferred.
func GetInferenceCandidateFromYAML(t Type, defaultType string, node *yaml.Node) (string, ComponentSpec, error) {
	refreshOldPlugins()

	node = unwrapDocumentNode(node)

	if node.Kind != yaml.MappingNode {
		return "", ComponentSpec{}, fmt.Errorf("invalid type %v, expected object", node.Kind)
	}

	var keys []string
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "type" {
			tStr := node.Content[i+1].Value
			spec, exists := GetDocs(tStr, t)
			if !exists {
				return "", ComponentSpec{}, fmt.Errorf("%v type '%v' was not recognised", string(t), tStr)
			}
			return tStr, spec, nil
		}
		keys = append(keys, node.Content[i].Value)
	}

	return getInferenceCandidateFromList(t, defaultType, keys)
}

// GetPluginConfigYAML extracts a plugin configuration node from a component
// config. This exists because there are two styles of plugin config, the old
// style (within `plugin`):
//
// type: foo
// plugin:
//   bar: baz
//
// And the new style:
//
// foo:
//   bar: baz
//
func GetPluginConfigYAML(name string, node *yaml.Node) (yaml.Node, error) {
	node = unwrapDocumentNode(node)
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name {
			return *node.Content[i+1], nil
		}
	}
	pluginStruct := struct {
		Plugin yaml.Node `yaml:"plugin"`
	}{}
	if err := node.Decode(&pluginStruct); err != nil {
		return yaml.Node{}, err
	}
	return pluginStruct.Plugin, nil
}

//------------------------------------------------------------------------------

func (f FieldSpec) shouldOmitYAML(parentFields FieldSpecs, fieldNode, parentNode *yaml.Node) (string, bool) {
	if f.omitWhenFn == nil {
		return "", false
	}
	field, err := f.YAMLToValue(true, fieldNode)
	if err != nil {
		return "", false
	}
	parent, err := parentFields.YAMLToMap(true, parentNode)
	if err != nil {
		return "", false
	}
	return f.omitWhenFn(field, parent)
}

// TODO: V4 Remove this.
func sanitiseConditionConfigYAML(node *yaml.Node) error {
	// This is a nasty hack until Benthos v4.
	newNodes := []*yaml.Node{}

	var name string
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "type" {
			name = node.Content[i+1].Value
			newNodes = append(newNodes, node.Content[i], node.Content[i+1])
			break
		}
	}

	if name == "" {
		return nil
	}

	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name {
			newNodes = append(newNodes, node.Content[i], node.Content[i+1])
			break
		}
	}

	node.Content = newNodes
	return nil
}

// SanitiseYAML takes a yaml.Node and a config spec and sorts the fields of the
// node according to the spec. Also optionally removes the `type` field from
// this and all nested components.
func SanitiseYAML(cType Type, node *yaml.Node, conf SanitiseConfig) error {
	node = unwrapDocumentNode(node)

	if cType == "condition" {
		return sanitiseConditionConfigYAML(node)
	}

	newNodes := []*yaml.Node{}

	var name string
	var keys []string
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "label" {
			if _, omit := labelField.shouldOmitYAML(nil, node.Content[i+1], node); !omit {
				newNodes = append(newNodes, node.Content[i], node.Content[i+1])
			}
			break
		}
	}
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "type" {
			name = node.Content[i+1].Value
			if !conf.RemoveTypeField {
				newNodes = append(newNodes, node.Content[i], node.Content[i+1])
			}
			break
		} else {
			keys = append(keys, node.Content[i].Value)
		}
	}
	if name == "" {
		if len(node.Content) == 0 {
			return nil
		}
		var err error
		if name, _, err = getInferenceCandidateFromList(cType, "", keys); err != nil {
			return err
		}
	}

	cSpec, exists := GetDocs(name, cType)
	if !exists {
		return fmt.Errorf("failed to obtain docs for %v type %v", cType, name)
	}

	nameFound := false
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "plugin" && cSpec.Plugin {
			node.Content[i].Value = name
		}

		if node.Content[i].Value != name {
			continue
		}

		nameFound = true
		if err := cSpec.Config.SanitiseYAML(node.Content[i+1], conf); err != nil {
			return err
		}
		newNodes = append(newNodes, node.Content[i], node.Content[i+1])
		break
	}

	// If the type field was omitted but we didn't see a config under the name
	// then we need to add an empty object.
	if !nameFound && conf.RemoveTypeField {
		var keyNode yaml.Node
		if err := keyNode.Encode(name); err != nil {
			return err
		}
		bodyNode, err := cSpec.Config.ToYAML(conf.ForExample)
		if err != nil {
			return err
		}
		if err := cSpec.Config.SanitiseYAML(bodyNode, conf); err != nil {
			return err
		}
		newNodes = append(newNodes, &keyNode, bodyNode)
	}

	reservedFields := reservedFieldsByType(cType)
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name || node.Content[i].Value == "type" || node.Content[i].Value == "label" {
			continue
		}
		if spec, exists := reservedFields[node.Content[i].Value]; exists {
			if _, omit := spec.shouldOmitYAML(nil, node.Content[i+1], node); omit {
				continue
			}
			if err := spec.SanitiseYAML(node.Content[i+1], conf); err != nil {
				return err
			}
			newNodes = append(newNodes, node.Content[i], node.Content[i+1])
		}
	}

	node.Content = newNodes
	return nil
}

// SanitiseYAML attempts to reduce a parsed config (as a *yaml.Node) down into a
// minimal representation without changing the behaviour of the config. The
// fields of the result will also be sorted according to the field spec.
func (f FieldSpec) SanitiseYAML(node *yaml.Node, conf SanitiseConfig) error {
	node = unwrapDocumentNode(node)

	if coreType, isCore := f.Type.IsCoreComponent(); isCore {
		switch f.Kind {
		case Kind2DArray:
			for i := 0; i < len(node.Content); i++ {
				for j := 0; j < len(node.Content[i].Content); j++ {
					if err := SanitiseYAML(coreType, node.Content[i].Content[j], conf); err != nil {
						return err
					}
				}
			}
		case KindArray:
			for i := 0; i < len(node.Content); i++ {
				if err := SanitiseYAML(coreType, node.Content[i], conf); err != nil {
					return err
				}
			}
		case KindMap:
			for i := 0; i < len(node.Content)-1; i += 2 {
				if err := SanitiseYAML(coreType, node.Content[i+1], conf); err != nil {
					return err
				}
			}
		default:
			if err := SanitiseYAML(coreType, node, conf); err != nil {
				return err
			}
		}
	} else if len(f.Children) > 0 {
		switch f.Kind {
		case Kind2DArray:
			for i := 0; i < len(node.Content); i++ {
				for j := 0; j < len(node.Content[i].Content); j++ {
					if err := f.Children.SanitiseYAML(node.Content[i].Content[j], conf); err != nil {
						return err
					}
				}
			}
		case KindArray:
			for i := 0; i < len(node.Content); i++ {
				if err := f.Children.SanitiseYAML(node.Content[i], conf); err != nil {
					return err
				}
			}
		case KindMap:
			for i := 0; i < len(node.Content)-1; i += 2 {
				if err := f.Children.SanitiseYAML(node.Content[i+1], conf); err != nil {
					return err
				}
			}
		default:
			if err := f.Children.SanitiseYAML(node, conf); err != nil {
				return err
			}
		}
	}
	return nil
}

// SanitiseYAML attempts to reduce a parsed config (as a *yaml.Node) down into a
// minimal representation without changing the behaviour of the config. The
// fields of the result will also be sorted according to the field spec.
func (f FieldSpecs) SanitiseYAML(node *yaml.Node, conf SanitiseConfig) error {
	node = unwrapDocumentNode(node)

	// Following the order of our field specs, extract each field.
	newNodes := []*yaml.Node{}
	for _, field := range f {
		if field.IsDeprecated && conf.RemoveDeprecated {
			continue
		}
		if conf.Filter.shouldDrop(field) {
			continue
		}
		for i := 0; i < len(node.Content)-1; i += 2 {
			if node.Content[i].Value != field.Name {
				continue
			}

			nextNode := node.Content[i+1]
			if _, omit := field.shouldOmitYAML(f, nextNode, node); omit {
				break
			}
			if err := field.SanitiseYAML(nextNode, conf); err != nil {
				return err
			}
			newNodes = append(newNodes, node.Content[i], nextNode)
			break
		}
	}
	node.Content = newNodes
	return nil
}

//------------------------------------------------------------------------------

func lintYAMLFromOmit(parentSpec FieldSpecs, lintTargetSpec FieldSpec, parent, node *yaml.Node) []Lint {
	var lints []Lint
	if lintTargetSpec.omitWhenFn != nil {
		fieldValue, err := lintTargetSpec.YAMLToValue(true, node)
		if err != nil {
			// If we weren't able to infer a value type then it's assumed
			// that we'll capture this type error elsewhere.
			return []Lint{}
		}
		parentMap, err := parentSpec.YAMLToMap(true, parent)
		if err != nil {
			// If we weren't able to infer a value type then it's assumed
			// that we'll capture this type error elsewhere.
			return []Lint{}
		}
		if why, omit := lintTargetSpec.shouldOmit(fieldValue, parentMap); omit {
			lints = append(lints, NewLintError(node.Line, why))
		}
	}
	return lints
}

func customLintFromYAML(ctx LintContext, spec FieldSpec, node *yaml.Node) []Lint {
	if spec.customLintFn == nil {
		return nil
	}
	fieldValue, err := spec.YAMLToValue(true, node)
	if err != nil {
		// If we weren't able to infer a value type then it's assumed
		// that we'll capture this type error elsewhere.
		return []Lint{}
	}
	lints := spec.customLintFn(ctx, node.Line, node.Column, fieldValue)
	return lints
}

// LintYAML takes a yaml.Node and a config spec and returns a list of linting
// errors found in the config.
func LintYAML(ctx LintContext, cType Type, node *yaml.Node) []Lint {
	if cType == "condition" {
		return nil
	}

	node = unwrapDocumentNode(node)

	var lints []Lint

	var name string
	var keys []string
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "type" {
			name = node.Content[i+1].Value
			break
		} else {
			keys = append(keys, node.Content[i].Value)
		}
	}
	if name == "" {
		if len(node.Content) == 0 {
			return nil
		}
		var err error
		if name, _, err = getInferenceCandidateFromList(cType, "", keys); err != nil {
			lints = append(lints, NewLintWarning(node.Line, "unable to infer component type"))
			return lints
		}
	}

	cSpec, exists := GetDocs(name, cType)
	if !exists {
		lints = append(lints, NewLintWarning(node.Line, fmt.Sprintf("failed to obtain docs for %v type %v", cType, name)))
		return lints
	}

	nameFound := false
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name {
			nameFound = true
			lints = append(lints, cSpec.Config.LintYAML(ctx, node.Content[i+1])...)
			break
		}
	}

	reservedFields := reservedFieldsByType(cType)
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name || node.Content[i].Value == "type" {
			continue
		}
		if node.Content[i].Value == "plugin" {
			if nameFound || !cSpec.Plugin {
				lints = append(lints, NewLintError(node.Content[i].Line, "plugin object is ineffective"))
			} else {
				lints = append(lints, cSpec.Config.LintYAML(ctx, node.Content[i+1])...)
			}
		}
		spec, exists := reservedFields[node.Content[i].Value]
		if exists {
			lints = append(lints, lintYAMLFromOmit(cSpec.Config.Children, spec, node, node.Content[i+1])...)
			lints = append(lints, spec.LintYAML(ctx, node.Content[i+1])...)
		} else {
			lints = append(lints, NewLintError(
				node.Content[i].Line,
				fmt.Sprintf("field %v is invalid when the component type is %v (%v)", node.Content[i].Value, name, cType),
			))
		}
	}

	return lints
}

// LintYAML returns a list of linting errors found by checking a field
// definition against a yaml node.
func (f FieldSpec) LintYAML(ctx LintContext, node *yaml.Node) []Lint {
	if f.skipLint {
		return nil
	}

	node = unwrapDocumentNode(node)

	var lints []Lint

	// Check basic kind matches, and execute custom linters
	switch f.Kind {
	case Kind2DArray:
		if node.Kind != yaml.SequenceNode {
			lints = append(lints, NewLintError(node.Line, "expected array value"))
			return lints
		}
		for i := 0; i < len(node.Content); i++ {
			lints = append(lints, f.Array().LintYAML(ctx, node.Content[i])...)
		}
		return lints
	case KindArray:
		if node.Kind != yaml.SequenceNode {
			lints = append(lints, NewLintError(node.Line, "expected array value"))
			return lints
		}
		for i := 0; i < len(node.Content); i++ {
			lints = append(lints, f.Scalar().LintYAML(ctx, node.Content[i])...)
		}
		return lints
	case KindMap:
		if node.Kind != yaml.MappingNode {
			lints = append(lints, NewLintError(node.Line, "expected object value"))
			return lints
		}
		for i := 0; i < len(node.Content)-1; i += 2 {
			lints = append(lints, f.Scalar().LintYAML(ctx, node.Content[i+1])...)
		}
		return lints
	}

	// Execute custom linters
	lints = append(lints, customLintFromYAML(ctx, f, node)...)

	// If we're a core type then execute component specific linting
	if coreType, isCore := f.Type.IsCoreComponent(); isCore {
		return append(lints, LintYAML(ctx, coreType, node)...)
	}

	// If the field has children then lint the child fields
	if len(f.Children) > 0 {
		return append(lints, f.Children.LintYAML(ctx, node)...)
	}

	// Otherwise we're a leaf node, so do basic type checking
	switch f.Type {
	// TODO: Do proper checking for bool and number types.
	case FieldTypeBool, FieldTypeString, FieldTypeInt, FieldTypeFloat:
		if node.Kind == yaml.MappingNode || node.Kind == yaml.SequenceNode {
			lints = append(lints, NewLintError(node.Line, fmt.Sprintf("expected %v value", f.Type)))
		}
	case FieldTypeObject:
		if node.Kind != yaml.MappingNode && node.Kind != yaml.AliasNode {
			lints = append(lints, NewLintError(node.Line, "expected object value"))
		}
	}
	return lints
}

// LintYAML walks a yaml node and returns a list of linting errors found.
func (f FieldSpecs) LintYAML(ctx LintContext, node *yaml.Node) []Lint {
	node = unwrapDocumentNode(node)

	var lints []Lint
	if node.Kind != yaml.MappingNode {
		if node.Kind == yaml.AliasNode {
			// TODO: Actually lint through aliases
			return nil
		}
		lints = append(lints, NewLintError(node.Line, "expected object value"))
		return lints
	}

	specNames := map[string]FieldSpec{}
	for _, field := range f {
		specNames[field.Name] = field
	}

	for i := 0; i < len(node.Content)-1; i += 2 {
		spec, exists := specNames[node.Content[i].Value]
		if !exists {
			if node.Content[i+1].Kind != yaml.AliasNode {
				lints = append(lints, NewLintError(node.Content[i].Line, fmt.Sprintf("field %v not recognised", node.Content[i].Value)))
			}
			continue
		}
		lints = append(lints, lintYAMLFromOmit(f, spec, node, node.Content[i+1])...)
		lints = append(lints, spec.LintYAML(ctx, node.Content[i+1])...)
		delete(specNames, node.Content[i].Value)
	}

	for name, remaining := range specNames {
		_, isCore := remaining.Type.IsCoreComponent()
		if !remaining.IsOptional &&
			remaining.Default == nil &&
			!isCore &&
			remaining.Kind == KindScalar &&
			!remaining.IsDeprecated &&
			len(remaining.Children) == 0 {
			lints = append(lints, NewLintError(node.Line, fmt.Sprintf("field %v is required", name)))
		}
	}
	return lints
}

//------------------------------------------------------------------------------

// ToYAML creates a YAML node from a field spec. If a default value has been
// specified then it is used. Otherwise, a zero value is generated. If recurse
// is enabled and the field has children then all children will also have values
// generated.
func (f FieldSpec) ToYAML(recurse bool) (*yaml.Node, error) {
	var node yaml.Node
	if f.Default != nil {
		if err := node.Encode(*f.Default); err != nil {
			return nil, err
		}
		return &node, nil
	}
	if f.Kind == KindArray || f.Kind == Kind2DArray {
		s := []interface{}{}
		if err := node.Encode(s); err != nil {
			return nil, err
		}
	} else if f.Kind == KindMap || len(f.Children) > 0 {
		if len(f.Children) > 0 && recurse {
			return f.Children.ToYAML()
		}
		s := map[string]interface{}{}
		if err := node.Encode(s); err != nil {
			return nil, err
		}
	} else {
		switch f.Type {
		case FieldTypeString:
			if err := node.Encode(""); err != nil {
				return nil, err
			}
		case FieldTypeInt:
			if err := node.Encode(0); err != nil {
				return nil, err
			}
		case FieldTypeFloat:
			if err := node.Encode(0.0); err != nil {
				return nil, err
			}
		case FieldTypeBool:
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

// ToYAML creates a YAML node from a list of field specs. If a default value has
// been specified for a given field then it is used. Otherwise, a zero value is
// generated.
func (f FieldSpecs) ToYAML() (*yaml.Node, error) {
	var node yaml.Node
	node.Kind = yaml.MappingNode

	for _, spec := range f {
		var keyNode yaml.Node
		if err := keyNode.Encode(spec.Name); err != nil {
			return nil, err
		}
		valueNode, err := spec.ToYAML(true)
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, &keyNode, valueNode)
	}

	return &node, nil
}

// YAMLToValue converts a yaml node into a generic value by referencing the
// expected type.
func (f FieldSpec) YAMLToValue(passive bool, node *yaml.Node) (interface{}, error) {
	node = unwrapDocumentNode(node)

	switch f.Kind {
	case Kind2DArray:
		switch f.Type {
		case FieldTypeString:
			var s [][]string
			if err := node.Decode(&s); err != nil {
				return nil, err
			}
			si := make([]interface{}, len(s))
			for i, v := range s {
				si[i] = v
			}
			return si, nil
		case FieldTypeInt:
			var ints [][]int
			if err := node.Decode(&ints); err != nil {
				return nil, err
			}
			ii := make([]interface{}, len(ints))
			for i, v := range ints {
				ii[i] = v
			}
			return ii, nil
		case FieldTypeFloat:
			var f [][]float64
			if err := node.Decode(&f); err != nil {
				return nil, err
			}
			fi := make([]interface{}, len(f))
			for i, v := range f {
				fi[i] = v
			}
			return fi, nil
		case FieldTypeBool:
			var b [][]bool
			if err := node.Decode(&b); err != nil {
				return nil, err
			}
			bi := make([]interface{}, len(b))
			for i, v := range b {
				bi[i] = v
			}
			return bi, nil
		}
	case KindArray:
		switch f.Type {
		case FieldTypeString:
			var s []string
			if err := node.Decode(&s); err != nil {
				return nil, err
			}
			si := make([]interface{}, len(s))
			for i, v := range s {
				si[i] = v
			}
			return si, nil
		case FieldTypeInt:
			var ints []int
			if err := node.Decode(&ints); err != nil {
				return nil, err
			}
			ii := make([]interface{}, len(ints))
			for i, v := range ints {
				ii[i] = v
			}
			return ii, nil
		case FieldTypeFloat:
			var f []float64
			if err := node.Decode(&f); err != nil {
				return nil, err
			}
			fi := make([]interface{}, len(f))
			for i, v := range f {
				fi[i] = v
			}
			return fi, nil
		case FieldTypeBool:
			var b []bool
			if err := node.Decode(&b); err != nil {
				return nil, err
			}
			bi := make([]interface{}, len(b))
			for i, v := range b {
				bi[i] = v
			}
			return bi, nil
		case FieldTypeObject:
			var c []yaml.Node
			if err := node.Decode(&c); err != nil {
				return nil, err
			}
			ci := make([]interface{}, len(c))
			for i, v := range c {
				var err error
				if ci[i], err = f.Children.YAMLToMap(passive, &v); err != nil {
					return nil, err
				}
			}
			return ci, nil
		}
	case KindMap:
		switch f.Type {
		case FieldTypeString:
			var s map[string]string
			if err := node.Decode(&s); err != nil {
				return nil, err
			}
			si := make(map[string]interface{}, len(s))
			for k, v := range s {
				si[k] = v
			}
			return si, nil
		case FieldTypeInt:
			var ints map[string]int
			if err := node.Decode(&ints); err != nil {
				return nil, err
			}
			ii := make(map[string]interface{}, len(ints))
			for k, v := range ints {
				ii[k] = v
			}
			return ii, nil
		case FieldTypeFloat:
			var f map[string]float64
			if err := node.Decode(&f); err != nil {
				return nil, err
			}
			fi := make(map[string]interface{}, len(f))
			for k, v := range f {
				fi[k] = v
			}
			return fi, nil
		case FieldTypeBool:
			var b map[string]bool
			if err := node.Decode(&b); err != nil {
				return nil, err
			}
			bi := make(map[string]interface{}, len(b))
			for k, v := range b {
				bi[k] = v
			}
			return bi, nil
		case FieldTypeObject:
			var c map[string]yaml.Node
			if err := node.Decode(&c); err != nil {
				return nil, err
			}
			ci := make(map[string]interface{}, len(c))
			for k, v := range c {
				var err error
				if ci[k], err = f.Children.YAMLToMap(passive, &v); err != nil {
					return nil, err
				}
			}
			return ci, nil
		}
	}
	switch f.Type {
	case FieldTypeString:
		var s string
		if err := node.Decode(&s); err != nil {
			return nil, err
		}
		return s, nil
	case FieldTypeInt:
		var i int
		if err := node.Decode(&i); err != nil {
			return nil, err
		}
		return i, nil
	case FieldTypeFloat:
		var f float64
		if err := node.Decode(&f); err != nil {
			return nil, err
		}
		return f, nil
	case FieldTypeBool:
		var b bool
		if err := node.Decode(&b); err != nil {
			return nil, err
		}
		return b, nil
	case FieldTypeObject:
		return f.Children.YAMLToMap(passive, node)
	}
	var v interface{}
	if err := node.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// YAMLToMap converts a yaml node into a generic map structure by referencing
// expected fields, adding default values to the map when the node does not
// contain them.
func (f FieldSpecs) YAMLToMap(passive bool, node *yaml.Node) (map[string]interface{}, error) {
	node = unwrapDocumentNode(node)

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
			if resultMap[fieldName], err = f.YAMLToValue(passive, node.Content[i+1]); err != nil {
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
		defValue, err := getDefault(k, v)
		if err != nil {
			if !passive {
				return nil, err
			}
			continue
		}
		resultMap[k] = defValue
	}

	return resultMap, nil
}

//------------------------------------------------------------------------------

func unwrapDocumentNode(node *yaml.Node) *yaml.Node {
	if node != nil && node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		return node.Content[0]
	}
	return node
}
