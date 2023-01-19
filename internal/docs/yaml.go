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

	field := newField(name, "")

	switch node.Kind {
	case yaml.MappingNode:
		field = field.WithChildren(FieldsFromYAML(node)...)
		field.Type = FieldTypeObject
		if len(field.Children) == 0 {
			var defaultI any = map[string]any{}
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

				var defaultI any = defaultArray
				field.Default = &defaultI
			case FieldTypeInt:
				var defaultArray []int64
				_ = node.Decode(&defaultArray)

				var defaultI any = defaultArray
				field.Default = &defaultI
			}
		} else {
			var defaultI any = []any{}
			field.Default = &defaultI
		}
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!bool":
			field.Type = FieldTypeBool

			var defaultBool bool
			_ = node.Decode(&defaultBool)

			var defaultI any = defaultBool
			field.Default = &defaultI
		case "!!int":
			field.Type = FieldTypeInt

			var defaultInt int64
			_ = node.Decode(&defaultInt)

			var defaultI any = defaultInt
			field.Default = &defaultI
		case "!!float":
			field.Type = FieldTypeFloat

			var defaultFloat float64
			_ = node.Decode(&defaultFloat)

			var defaultI any = defaultFloat
			field.Default = &defaultI
		default:
			field.Type = FieldTypeString

			var defaultStr string
			_ = node.Decode(&defaultStr)

			var defaultI any = defaultStr
			field.Default = &defaultI
		}
	}

	return field
}

// GetInferenceCandidateFromYAML checks a yaml node config structure for a
// component and returns either the inferred type name or an error if one cannot
// be inferred.
func GetInferenceCandidateFromYAML(docProv Provider, t Type, node *yaml.Node) (string, ComponentSpec, error) {
	node = unwrapDocumentNode(node)

	if node.Kind != yaml.MappingNode {
		return "", ComponentSpec{}, fmt.Errorf("invalid type %v, expected object", node.Kind)
	}

	var keys []string
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "type" {
			tStr := node.Content[i+1].Value
			spec, exists := docProv.GetDocs(tStr, t)
			if !exists {
				return "", ComponentSpec{}, fmt.Errorf("%v type '%v' was not recognised", string(t), tStr)
			}
			return tStr, spec, nil
		}
		keys = append(keys, node.Content[i].Value)
	}

	return getInferenceCandidateFromList(docProv, t, keys)
}

// GetPluginConfigYAML extracts a plugin configuration node from a component
// config. This exists because there are two styles of plugin config.
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

func (f FieldSpec) shouldOmitYAML(parentFields FieldSpecs, fieldNode, parentNode *yaml.Node) (why string, shouldOmit bool) {
	conf := ToValueConfig{
		Passive:             true,
		FallbackToInterface: true,
	}

	if f.omitWhenFn == nil {
		return
	}
	field, err := f.YAMLToValue(fieldNode, conf)
	if err != nil {
		// If we weren't able to infer a value type then it's assumed
		// that we'll capture this type error elsewhere.
		return
	}
	parent, err := parentFields.YAMLToMap(parentNode, conf)
	if err != nil {
		// If we weren't able to infer a value type then it's assumed
		// that we'll capture this type error elsewhere.
		return
	}
	return f.omitWhenFn(field, parent)
}

// SanitiseYAML takes a yaml.Node and a config spec and sorts the fields of the
// node according to the spec. Also optionally removes the `type` field from
// this and all nested components.
func SanitiseYAML(cType Type, node *yaml.Node, conf SanitiseConfig) error {
	node = unwrapDocumentNode(node)

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
			if name = node.Content[i+1].Value; name == "" {
				continue
			}
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
		if name, _, err = getInferenceCandidateFromList(conf.DocsProvider, cType, keys); err != nil {
			return err
		}
	}

	cSpec, exists := conf.DocsProvider.GetDocs(name, cType)
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

	reservedFields := ReservedFieldsByType(cType)
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
	} else if f.Scrubber != "" {
		scrubNode := func(n *yaml.Node) error {
			var scrubValue any
			err := n.Decode(&scrubValue)
			if err != nil {
				return err
			}
			if scrubValue, err = f.scrubValue(scrubValue); err != nil {
				return err
			}
			if err := n.Encode(scrubValue); err != nil {
				return err
			}
			return nil
		}
		switch f.Kind {
		case Kind2DArray:
			for i := 0; i < len(node.Content); i++ {
				for j := 0; j < len(node.Content[i].Content); j++ {
					if err := scrubNode(node.Content[i].Content[j]); err != nil {
						return err
					}
				}
			}
		case KindArray:
			for i := 0; i < len(node.Content); i++ {
				if err := scrubNode(node.Content[i]); err != nil {
					return err
				}
			}
		case KindMap:
			for i := 0; i < len(node.Content)-1; i += 2 {
				if err := scrubNode(node.Content[i+1]); err != nil {
					return err
				}
			}
		default:
			if err := scrubNode(node); err != nil {
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

	nodeKeys := map[string]*yaml.Node{}
	for i := 0; i < len(node.Content)-1; i += 2 {
		nodeKeys[node.Content[i].Value] = node.Content[i+1]
	}

	// Following the order of our field specs, extract each field.
	newNodes := []*yaml.Node{}
	for _, field := range f {
		if field.IsDeprecated && conf.RemoveDeprecated {
			continue
		}
		if conf.Filter.shouldDrop(field) {
			continue
		}
		value, exists := nodeKeys[field.Name]
		if !exists {
			continue
		}
		if _, omit := field.shouldOmitYAML(f, value, node); omit {
			continue
		}
		if err := field.SanitiseYAML(value, conf); err != nil {
			return err
		}
		var keyNode yaml.Node
		if err := keyNode.Encode(field.Name); err != nil {
			return err
		}
		newNodes = append(newNodes, &keyNode, value)
	}
	node.Content = newNodes
	return nil
}

//------------------------------------------------------------------------------

func lintYAMLFromOmit(parentSpec FieldSpecs, lintTargetSpec FieldSpec, parent, node *yaml.Node) []Lint {
	why, shouldOmit := lintTargetSpec.shouldOmitYAML(parentSpec, node, parent)
	if shouldOmit {
		return []Lint{NewLintError(node.Line, LintShouldOmit, why)}
	}
	return nil
}

func customLintFromYAML(ctx LintContext, spec FieldSpec, node *yaml.Node) []Lint {
	lintFn := spec.getLintFunc()
	if lintFn == nil {
		return nil
	}
	fieldValue, err := spec.YAMLToValue(node, ToValueConfig{
		Passive:             true,
		FallbackToInterface: true,
	})
	if err != nil {
		// If we weren't able to infer a value type then it's assumed
		// that we'll capture this type error elsewhere.
		return []Lint{}
	}
	line := node.Line
	if node.Style == yaml.LiteralStyle {
		line++
	}

	lints := lintFn(ctx, line, node.Column, fieldValue)
	return lints
}

// LintYAML takes a yaml.Node and a config spec and returns a list of linting
// errors found in the config.
func LintYAML(ctx LintContext, cType Type, node *yaml.Node) []Lint {
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
		if name, _, err = getInferenceCandidateFromList(ctx.DocsProvider, cType, keys); err != nil {
			lints = append(lints, NewLintWarning(node.Line, LintComponentMissing, "unable to infer component type"))
			return lints
		}
	}

	cSpec, exists := ctx.DocsProvider.GetDocs(name, cType)
	if !exists {
		lints = append(lints, NewLintWarning(node.Line, LintComponentNotFound, fmt.Sprintf("failed to obtain docs for %v type %v", cType, name)))
		return lints
	}

	if ctx.RejectDeprecated && cSpec.Status == StatusDeprecated {
		lints = append(lints, NewLintError(node.Line, LintDeprecated, fmt.Sprintf("component %v is deprecated", cSpec.Name)))
	}

	nameFound := false
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name {
			nameFound = true
			lints = append(lints, cSpec.Config.LintYAML(ctx, node.Content[i+1])...)
			break
		}
	}

	reservedFields := ReservedFieldsByType(cType)
	_, canLabel := reservedFields["label"]
	hasLabel := false
	for i := 0; i < len(node.Content)-1; i += 2 {
		key := node.Content[i].Value
		if key == name || key == "type" {
			continue
		}
		if key == "plugin" {
			if nameFound || !cSpec.Plugin {
				lints = append(lints, NewLintError(node.Content[i].Line, LintShouldOmit, "plugin object is ineffective"))
			} else {
				lints = append(lints, cSpec.Config.LintYAML(ctx, node.Content[i+1])...)
			}
		}
		spec, exists := reservedFields[key]
		hasLabel = hasLabel || (key == "label")
		if exists {
			lints = append(lints, lintYAMLFromOmit(cSpec.Config.Children, spec, node, node.Content[i+1])...)
			lints = append(lints, spec.LintYAML(ctx, node.Content[i+1])...)
		} else {
			lints = append(lints, NewLintError(
				node.Content[i].Line,
				LintUnknown,
				fmt.Sprintf("field %v is invalid when the component type is %v (%v)", node.Content[i].Value, name, cType),
			))
		}
	}

	if ctx.RequireLabels && canLabel && !hasLabel && name != "resource" {
		lints = append(lints, NewLintError(node.Line, LintMissingLabel, fmt.Sprintf("label is required for %s", cSpec.Name)))
	}

	return lints
}

// LintYAML returns a list of linting errors found by checking a field
// definition against a yaml node.
func (f FieldSpec) LintYAML(ctx LintContext, node *yaml.Node) []Lint {
	node = unwrapDocumentNode(node)

	var lints []Lint

	if ctx.RejectDeprecated && f.IsDeprecated {
		lints = append(lints, NewLintError(node.Line, LintDeprecated, fmt.Sprintf("field %v is deprecated", f.Name)))
	}

	// Execute custom linters, if the kind is non-scalar this means we execute
	// the linter from the perspective of both the scalar and higher level types
	// and it's up to the linting implementation to distinguish between them.
	lints = append(lints, customLintFromYAML(ctx, f, node)...)

	// Check basic kind matches, and execute custom linters
	switch f.Kind {
	case Kind2DArray:
		if node.Kind != yaml.SequenceNode {
			lints = append(lints, NewLintError(node.Line, LintExpectedArray, "expected array value"))
			return lints
		}
		for i := 0; i < len(node.Content); i++ {
			lints = append(lints, f.Array().LintYAML(ctx, node.Content[i])...)
		}
		return lints
	case KindArray:
		if node.Kind != yaml.SequenceNode {
			lints = append(lints, NewLintError(node.Line, LintExpectedArray, "expected array value"))
			return lints
		}
		for i := 0; i < len(node.Content); i++ {
			lints = append(lints, f.Scalar().LintYAML(ctx, node.Content[i])...)
		}
		return lints
	case KindMap:
		if node.Kind != yaml.MappingNode {
			lints = append(lints, NewLintError(node.Line, LintExpectedObject, "expected object value"))
			return lints
		}
		for i := 0; i < len(node.Content)-1; i += 2 {
			lints = append(lints, f.Scalar().LintYAML(ctx, node.Content[i+1])...)
		}
		return lints
	}

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
			lints = append(lints, NewLintError(node.Line, LintExpectedScalar, fmt.Sprintf("expected %v value", f.Type)))
		}
	case FieldTypeObject:
		if node.Kind != yaml.MappingNode && node.Kind != yaml.AliasNode {
			lints = append(lints, NewLintError(node.Line, LintExpectedObject, "expected object value"))
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
		lints = append(lints, NewLintError(node.Line, LintExpectedObject, "expected object value"))
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
				lints = append(lints, NewLintError(node.Content[i].Line, LintUnknown, fmt.Sprintf("field %v not recognised", node.Content[i].Value)))
			}
			continue
		}
		lints = append(lints, lintYAMLFromOmit(f, spec, node, node.Content[i+1])...)
		lints = append(lints, spec.LintYAML(ctx, node.Content[i+1])...)
		delete(specNames, node.Content[i].Value)
	}

	for name, remaining := range specNames {
		_, isCore := remaining.Type.IsCoreComponent()
		if remaining.needsDefault() &&
			remaining.Default == nil &&
			!isCore &&
			remaining.Kind == KindScalar &&
			len(remaining.Children) == 0 {
			lints = append(lints, NewLintError(node.Line, LintMissing, fmt.Sprintf("field %v is required", name)))
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
		s := []any{}
		if err := node.Encode(s); err != nil {
			return nil, err
		}
	} else if f.Kind == KindMap || len(f.Children) > 0 {
		if len(f.Children) > 0 && recurse {
			return f.Children.ToYAML()
		}
		s := map[string]any{}
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

// ToValueConfig describes custom options for how documentation fields should be
// used to convert a parsed node to a value type.
type ToValueConfig struct {
	// Whether an problem in the config node detected during conversion
	// should return a "best attempt" structure rather than an error.
	Passive bool

	// When a field spec is for a non-scalar type (a component) fall back to
	// decoding it into an interface, otherwise the raw yaml.Node is
	// returned in its place.
	FallbackToInterface bool
}

// YAMLToValue converts a yaml node into a generic value by referencing the
// expected type.
func (f FieldSpec) YAMLToValue(node *yaml.Node, conf ToValueConfig) (any, error) {
	node = unwrapDocumentNode(node)

	switch f.Kind {
	case Kind2DArray:
		if !conf.Passive && node.Kind != yaml.SequenceNode {
			return nil, fmt.Errorf("line %v: expected array value, got %v", node.Line, node.Kind)
		}
		subSpec := f.Array()

		var s []any
		for i := 0; i < len(node.Content); i++ {
			v, err := subSpec.YAMLToValue(node.Content[i], conf)
			if err != nil {
				return nil, err
			}
			s = append(s, v)
		}
		return s, nil
	case KindArray:
		if !conf.Passive && node.Kind != yaml.SequenceNode {
			return nil, fmt.Errorf("line %v: expected array value, got %v", node.Line, node.Kind)
		}
		subSpec := f.Scalar()

		var s []any
		for i := 0; i < len(node.Content); i++ {
			v, err := subSpec.YAMLToValue(node.Content[i], conf)
			if err != nil {
				return nil, err
			}
			s = append(s, v)
		}
		return s, nil
	case KindMap:
		if !conf.Passive && node.Kind != yaml.MappingNode {
			return nil, fmt.Errorf("line %v: expected map value, got %v", node.Line, node.Kind)
		}
		subSpec := f.Scalar()

		m := map[string]any{}
		for i := 0; i < len(node.Content)-1; i += 2 {
			var err error
			if m[node.Content[i].Value], err = subSpec.YAMLToValue(node.Content[i+1], conf); err != nil {
				return nil, err
			}
		}
		return m, nil
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
	case FieldTypeUnknown:
		var i any
		if err := node.Decode(&i); err != nil {
			return nil, err
		}
		return i, nil
	case FieldTypeObject:
		return f.Children.YAMLToMap(node, conf)
	}

	if conf.FallbackToInterface {
		// We don't know what the field actually is (likely a component
		// type), so if we we can either decode into a generic interface
		// or return the raw node itself.
		var v any
		if err := node.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	}
	return node, nil
}

// YAMLToMap converts a yaml node into a generic map structure by referencing
// expected fields, adding default values to the map when the node does not
// contain them.
func (f FieldSpecs) YAMLToMap(node *yaml.Node, conf ToValueConfig) (map[string]any, error) {
	node = unwrapDocumentNode(node)

	pendingFieldsMap := map[string]FieldSpec{}
	for _, field := range f {
		pendingFieldsMap[field.Name] = field
	}

	resultMap := map[string]any{}

	for i := 0; i < len(node.Content)-1; i += 2 {
		fieldName := node.Content[i].Value

		if f, exists := pendingFieldsMap[fieldName]; exists {
			delete(pendingFieldsMap, f.Name)
			var err error
			if resultMap[fieldName], err = f.YAMLToValue(node.Content[i+1], conf); err != nil {
				return nil, fmt.Errorf("field '%v': %w", fieldName, err)
			}
		} else {
			var v any
			if err := node.Content[i+1].Decode(&v); err != nil {
				return nil, err
			}
			resultMap[fieldName] = v
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
		resultMap[k] = defValue
	}

	return resultMap, nil
}

//------------------------------------------------------------------------------

func walkComponentsYAML(cType Type, node *yaml.Node, prov Provider, fn ComponentWalkYAMLFunc) error {
	node = unwrapDocumentNode(node)

	name, spec, err := GetInferenceCandidateFromYAML(prov, cType, node)
	if err != nil {
		return err
	}

	var label string
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "label" {
			label = node.Content[i+1].Value
			break
		}
	}

	if err := fn(WalkedYAMLComponent{
		ComponentType: cType,
		Name:          name,
		Label:         label,
		Conf:          node,
	}); err != nil {
		return err
	}

	reservedFields := ReservedFieldsByType(cType)
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name {
			if err := spec.Config.WalkYAML(node.Content[i+1], prov, fn); err != nil {
				return err
			}
			continue
		}
		if node.Content[i].Value == "type" || node.Content[i].Value == "label" {
			continue
		}
		if spec, exists := reservedFields[node.Content[i].Value]; exists {
			if err := spec.WalkYAML(node.Content[i+1], prov, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// WalkYAML walks each node of a YAML tree and for any component types within
// the config a provided func is called.
func (f FieldSpec) WalkYAML(node *yaml.Node, prov Provider, fn ComponentWalkYAMLFunc) error {
	node = unwrapDocumentNode(node)

	if coreType, isCore := f.Type.IsCoreComponent(); isCore {
		switch f.Kind {
		case Kind2DArray:
			for i := 0; i < len(node.Content); i++ {
				for j := 0; j < len(node.Content[i].Content); j++ {
					if err := walkComponentsYAML(coreType, node.Content[i].Content[j], prov, fn); err != nil {
						return err
					}
				}
			}
		case KindArray:
			for i := 0; i < len(node.Content); i++ {
				if err := walkComponentsYAML(coreType, node.Content[i], prov, fn); err != nil {
					return err
				}
			}
		case KindMap:
			for i := 0; i < len(node.Content)-1; i += 2 {
				if err := walkComponentsYAML(coreType, node.Content[i+1], prov, fn); err != nil {
					return err
				}
			}
		default:
			if err := walkComponentsYAML(coreType, node, prov, fn); err != nil {
				return err
			}
		}
	} else if len(f.Children) > 0 {
		switch f.Kind {
		case Kind2DArray:
			for i := 0; i < len(node.Content); i++ {
				for j := 0; j < len(node.Content[i].Content); j++ {
					if err := f.Children.WalkYAML(node.Content[i].Content[j], prov, fn); err != nil {
						return err
					}
				}
			}
		case KindArray:
			for i := 0; i < len(node.Content); i++ {
				if err := f.Children.WalkYAML(node.Content[i], prov, fn); err != nil {
					return err
				}
			}
		case KindMap:
			for i := 0; i < len(node.Content)-1; i += 2 {
				if err := f.Children.WalkYAML(node.Content[i+1], prov, fn); err != nil {
					return err
				}
			}
		default:
			if err := f.Children.WalkYAML(node, prov, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// ComponentWalkYAMLFunc is called for each component type within a YAML config,
// where the node representing that component is provided along with the type
// and implementation name.
type ComponentWalkYAMLFunc func(c WalkedYAMLComponent) error

// WalkedYAMLComponent is a struct containing information about a component
// yielded via the WalkYAML method.
type WalkedYAMLComponent struct {
	ComponentType Type
	Name          string
	Label         string
	Conf          *yaml.Node
}

// WalkYAML walks each node of a YAML tree and for any component types within
// the config a provided func is called.
func (f FieldSpecs) WalkYAML(node *yaml.Node, prov Provider, fn ComponentWalkYAMLFunc) error {
	node = unwrapDocumentNode(node)

	nodeKeys := map[string]*yaml.Node{}
	for i := 0; i < len(node.Content)-1; i += 2 {
		nodeKeys[node.Content[i].Value] = node.Content[i+1]
	}

	// Following the order of our field specs, walk each field.
	for _, field := range f {
		value, exists := nodeKeys[field.Name]
		if !exists {
			continue
		}
		if err := field.WalkYAML(value, prov, fn); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

func unwrapDocumentNode(node *yaml.Node) *yaml.Node {
	if node != nil && node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		return node.Content[0]
	}
	return node
}
