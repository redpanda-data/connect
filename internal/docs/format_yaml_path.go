package docs

import (
	"errors"
	"fmt"
	"strconv"

	"gopkg.in/yaml.v3"
)

func removeFieldFromMapping(name string, node *yaml.Node) error {
	var newContent []*yaml.Node
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value != name {
			newContent = append(newContent, node.Content[i], node.Content[i+1])
		}
	}
	node.Content = newContent
	return nil
}

func getFieldFromMapping(name string, createMissing bool, node *yaml.Node) (*yaml.Node, error) {
	node.Kind = yaml.MappingNode
	node.Style = yaml.LiteralStyle
	var foundNode *yaml.Node
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name {
			foundNode = node.Content[i+1]
			break
		}
	}
	if foundNode == nil {
		if !createMissing {
			return nil, fmt.Errorf("%v: key not found in mapping", name)
		}
		var keyNode yaml.Node
		if err := keyNode.Encode(name); err != nil {
			return nil, fmt.Errorf("%v: failed to encode key: %w", name, err)
		}
		node.Content = append(node.Content, &keyNode)

		foundNode = &yaml.Node{}
		node.Content = append(node.Content, foundNode)
	}
	return foundNode, nil
}

func removeIndexFromMapping(name string, node *yaml.Node) error {
	removeIndex, err := strconv.Atoi(name)
	if err != nil {
		return err
	}
	if len(node.Content) <= removeIndex {
		return fmt.Errorf("invalid delete index: %v, length: %v", removeIndex, len(node.Content))
	}
	newContent := make([]*yaml.Node, 0, len(node.Content)-1)
	for i := 0; i < len(node.Content); i++ {
		if i != removeIndex {
			newContent = append(newContent, node.Content[i])
		}
	}
	node.Content = newContent
	return nil
}

func getIndexFromSequence(name string, allowAppend bool, node *yaml.Node) (*yaml.Node, error) {
	node.Kind = yaml.SequenceNode
	node.Style = yaml.LiteralStyle
	var foundNode *yaml.Node
	if name != "-" {
		index, err := strconv.Atoi(name)
		if err != nil {
			return nil, fmt.Errorf("%v: failed to parse path segment as array index: %w", name, err)
		}
		if len(node.Content) <= index {
			return nil, fmt.Errorf("%v: target index greater than array length", name)
		}
		foundNode = node.Content[index]
	} else {
		if !allowAppend {
			return nil, fmt.Errorf("%v: append directive not allowed", name)
		}
		foundNode = &yaml.Node{}
		node.Content = append(node.Content, foundNode)
	}
	return foundNode, nil
}

// SetYAMLPath sets the value of a node within a YAML document identified by a
// path to a value.
func (f FieldSpecs) SetYAMLPath(docsProvider Provider, root, value *yaml.Node, path ...string) error {
	root = unwrapDocumentNode(root)
	value = unwrapDocumentNode(value)

	var foundSpec FieldSpec
	for _, spec := range f {
		if spec.Name == path[0] {
			foundSpec = spec
			break
		}
	}
	if foundSpec.Name == "" {
		return fmt.Errorf("%v: field not recognised", path[0])
	}

	// Check for delete
	if len(path) == 1 && value == nil {
		return removeFieldFromMapping(path[0], root)
	}

	foundNode, err := getFieldFromMapping(path[0], true, root)
	if err != nil {
		return err
	}

	if err := foundSpec.SetYAMLPath(docsProvider, foundNode, value, path[1:]...); err != nil {
		return fmt.Errorf("%v.%w", path[0], err)
	}
	return nil
}

func setYAMLPathCore(docsProvider Provider, coreType Type, root, value *yaml.Node, path ...string) error {
	foundNode, err := getFieldFromMapping(path[0], true, root)
	if err != nil {
		return err
	}
	if f, exists := ReservedFieldsByType(coreType)[path[0]]; exists {
		if err = f.SetYAMLPath(docsProvider, foundNode, value, path[1:]...); err != nil {
			return fmt.Errorf("%v.%w", path[0], err)
		}
		return nil
	}
	cSpec, exists := docsProvider.GetDocs(path[0], coreType)
	if !exists {
		return fmt.Errorf("%v: field not recognised", path[0])
	}
	if err = cSpec.Config.SetYAMLPath(docsProvider, foundNode, value, path[1:]...); err != nil {
		return fmt.Errorf("%v.%w", path[0], err)
	}
	return nil
}

// SetYAMLPath sets the value of a node within a YAML document identified by a
// path to a value.
func (f FieldSpec) SetYAMLPath(docsProvider Provider, root, value *yaml.Node, path ...string) error {
	root = unwrapDocumentNode(root)
	value = unwrapDocumentNode(value)

	switch f.Kind {
	case Kind2DArray:
		if len(path) == 0 {
			if value.Kind == yaml.SequenceNode {
				*root = *value
			} else {
				root.Kind = yaml.SequenceNode
				root.Style = yaml.LiteralStyle
				root.Content = []*yaml.Node{{
					Kind:    yaml.SequenceNode,
					Style:   yaml.LiteralStyle,
					Content: []*yaml.Node{value},
				}}
			}
			return nil
		}
		if len(path) == 1 && value == nil {
			return removeIndexFromMapping(path[0], root)
		}
		target, err := getIndexFromSequence(path[0], true, root)
		if err != nil {
			return err
		}
		if err = f.Array().SetYAMLPath(docsProvider, target, value, path[1:]...); err != nil {
			return fmt.Errorf("%v.%w", path[0], err)
		}
		return nil
	case KindArray:
		if len(path) == 0 {
			if value.Kind == yaml.SequenceNode {
				*root = *value
			} else {
				root.Kind = yaml.SequenceNode
				root.Style = yaml.LiteralStyle
				root.Content = []*yaml.Node{value}
			}
			return nil
		}
		if len(path) == 1 && value == nil {
			return removeIndexFromMapping(path[0], root)
		}
		target, err := getIndexFromSequence(path[0], true, root)
		if err != nil {
			return err
		}
		if err = f.Scalar().SetYAMLPath(docsProvider, target, value, path[1:]...); err != nil {
			return fmt.Errorf("%v.%w", path[0], err)
		}
		return nil
	case KindMap:
		if len(path) == 0 {
			return errors.New("cannot set map directly")
		}
		if len(path) == 1 && value == nil {
			return removeFieldFromMapping(path[0], root)
		}
		target, err := getFieldFromMapping(path[0], true, root)
		if err != nil {
			return err
		}
		if err = f.Scalar().SetYAMLPath(docsProvider, target, value, path[1:]...); err != nil {
			return fmt.Errorf("%v.%w", path[0], err)
		}
		return nil
	}
	if len(path) == 0 {
		*root = *value
		return nil
	}
	if len(path) == 1 && value == nil {
		return removeFieldFromMapping(path[0], root)
	}
	if coreType, isCore := f.Type.IsCoreComponent(); isCore {
		if len(path) == 0 {
			return fmt.Errorf("(%v): cannot set core type directly", coreType)
		}
		return setYAMLPathCore(docsProvider, coreType, root, value, path...)
	}
	if len(f.Children) > 0 {
		return f.Children.SetYAMLPath(docsProvider, root, value, path...)
	}
	return fmt.Errorf("%v: field not recognised", path[0])
}

//------------------------------------------------------------------------------

// GetDocsForPath attempts to find the documentation for a given node of a
// config identified by a path.
func (f FieldSpecs) GetDocsForPath(docsProvider Provider, path ...string) (FieldSpec, error) {
	target := path[0]
	for _, spec := range f {
		if spec.Name == target {
			return spec.GetDocsForPath(docsProvider, path[1:]...)
		}
	}
	return FieldSpec{}, fmt.Errorf("%v: field not recognised", path[0])
}

func getDocsForPathCore(docsProvider Provider, coreType Type, path ...string) (FieldSpec, error) {
	if f, exists := ReservedFieldsByType(coreType)[path[0]]; exists {
		return f.GetDocsForPath(docsProvider, path[1:]...)
	}
	cSpec, exists := docsProvider.GetDocs(path[0], coreType)
	if !exists {
		return FieldSpec{}, fmt.Errorf("%v: field not recognised", path[0])
	}
	return cSpec.Config.GetDocsForPath(docsProvider, path[1:]...)
}

// GetDocsForPath attempts to find the documentation for a given node of a
// config identified by a path.
func (f FieldSpec) GetDocsForPath(docsProvider Provider, path ...string) (FieldSpec, error) {
	if len(path) == 0 {
		return f, nil
	}
	switch f.Kind {
	case Kind2DArray:
		return f.Array().GetDocsForPath(docsProvider, path[1:]...)
	case KindArray, KindMap:
		return f.Scalar().GetDocsForPath(docsProvider, path[1:]...)
	}
	if coreType, isCore := f.Type.IsCoreComponent(); isCore {
		return getDocsForPathCore(docsProvider, coreType, path...)
	}
	if len(f.Children) > 0 {
		return f.Children.GetDocsForPath(docsProvider, path...)
	}
	return FieldSpec{}, fmt.Errorf("%v: field not recognised", path[0])
}

// GetYAMLPath attempts to obtain a specific value within a YAML tree by
// following a sequence of path identifiers.
func GetYAMLPath(root *yaml.Node, path ...string) (*yaml.Node, error) {
	root = unwrapDocumentNode(root)

	if len(path) == 0 {
		return root, nil
	}

	if root.Kind == yaml.SequenceNode {
		newRoot, err := getIndexFromSequence(path[0], false, root)
		if err != nil {
			return nil, err
		}
		if newRoot, err = GetYAMLPath(newRoot, path[1:]...); err != nil {
			return nil, fmt.Errorf("%v.%w", path[0], err)
		}
		return newRoot, nil
	}

	newRoot, err := getFieldFromMapping(path[0], false, root)
	if err != nil {
		return nil, err
	}
	if newRoot, err = GetYAMLPath(newRoot, path[1:]...); err != nil {
		return nil, fmt.Errorf("%v.%w", path[0], err)
	}
	return newRoot, nil
}

//------------------------------------------------------------------------------

// YAMLLabelsToPaths walks a YAML tree using a field spec as a reference point.
// When a component of the YAML tree has a label field it is added to the
// provided labelsToPaths map with the path to the component.
func (f FieldSpecs) YAMLLabelsToPaths(docsProvider Provider, node *yaml.Node, labelsToPaths map[string][]string, path []string) {
	node = unwrapDocumentNode(node)

	fieldMap := map[string]FieldSpec{}
	for _, spec := range f {
		fieldMap[spec.Name] = spec
	}

	for i := 0; i < len(node.Content)-1; i += 2 {
		key := node.Content[i].Value
		if spec, exists := fieldMap[key]; exists {
			spec.YAMLLabelsToPaths(docsProvider, node.Content[i+1], labelsToPaths, append(path, key))
		}
	}
}

// YAMLLabelsToPaths walks a YAML tree using a field spec as a reference point.
// When a component of the YAML tree has a label field it is added to the
// provided labelsToPaths map with the path to the component.
func (f FieldSpec) YAMLLabelsToPaths(docsProvider Provider, node *yaml.Node, labelsToPaths map[string][]string, path []string) {
	node = unwrapDocumentNode(node)

	switch f.Kind {
	case Kind2DArray:
		nextSpec := f.Array()
		for i, child := range node.Content {
			nextSpec.YAMLLabelsToPaths(docsProvider, child, labelsToPaths, append(path, strconv.Itoa(i)))
		}
	case KindArray:
		nextSpec := f.Scalar()
		for i, child := range node.Content {
			nextSpec.YAMLLabelsToPaths(docsProvider, child, labelsToPaths, append(path, strconv.Itoa(i)))
		}
	case KindMap:
		nextSpec := f.Scalar()
		for i, child := range node.Content {
			nextSpec.YAMLLabelsToPaths(docsProvider, child, labelsToPaths, append(path, strconv.Itoa(i)))
		}
		for i := 0; i < len(node.Content)-1; i += 2 {
			key := node.Content[i].Value
			nextSpec.YAMLLabelsToPaths(docsProvider, node.Content[i+1], labelsToPaths, append(path, key))
		}
	default:
		if coreType, isCore := f.Type.IsCoreComponent(); isCore {
			coreFields := FieldSpecs{}
			for _, f := range ReservedFieldsByType(coreType) {
				coreFields = append(coreFields, f)
			}
			if inferred, cSpec, err := GetInferenceCandidateFromYAML(docsProvider, coreType, node); err == nil {
				conf := cSpec.Config
				conf.Name = inferred
				coreFields = append(coreFields, conf)
			}
			coreFields.YAMLLabelsToPaths(docsProvider, node, labelsToPaths, path)
		} else if len(f.Children) > 0 {
			f.Children.YAMLLabelsToPaths(docsProvider, node, labelsToPaths, path)
		} else if f.Name == labelField.Name && f.Description == labelField.Description {
			pathCopy := make([]string, len(path)-1)
			copy(pathCopy, path[:len(path)-1])
			labelsToPaths[node.Value] = pathCopy // Add path to the parent node
		}
	}
}
