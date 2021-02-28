package docs

import (
	"fmt"
	"sort"

	"github.com/Jeffail/benthos/v3/internal/interop/plugins"
	"gopkg.in/yaml.v3"
)

func reservedFieldsByType(t Type) map[string]FieldSpec {
	m := map[string]FieldSpec{
		"type":   FieldCommon("type", "").HasType(FieldString),
		"plugin": FieldCommon("plugin", "").HasType(FieldObject),
	}
	switch t {
	case TypeInput:
		fallthrough
	case TypeOutput:
		m["processors"] = FieldCommon("processors", "").Array().HasType(FieldProcessor).OmitWhen(func(field, _ interface{}) bool {
			if arr, ok := field.([]interface{}); ok && len(arr) == 0 {
				return true
			}
			return false
		})
	}
	return m
}

func refreshOldPlugins() {
	plugins.FlushNameTypes(func(nt [2]string) {
		RegisterDocs(ComponentSpec{
			Name:   nt[0],
			Type:   Type(nt[1]),
			Status: StatusPlugin,
		})
	})
}

// GetInferenceCandidate checks a generic config structure for a component and
// returns either the inferred type name or an error if one cannot be inferred.
func GetInferenceCandidate(t Type, defaultType string, raw interface{}) (string, ComponentSpec, error) {
	refreshOldPlugins()

	m, ok := raw.(map[string]interface{})
	if !ok {
		return "", ComponentSpec{}, fmt.Errorf("invalid config value %T, expected object", raw)
	}

	if tStr, ok := m["type"].(string); ok {
		spec, exists := GetDocs(tStr, t)
		if !exists {
			return "", ComponentSpec{}, fmt.Errorf("%v type '%v' was not recognised", string(t), tStr)
		}
		return tStr, spec, nil
	}

	ignore := reservedFieldsByType(t)

	var candidates []string
	var inferred string
	var inferredSpec ComponentSpec
	for k := range m {
		if _, exists := ignore[k]; exists {
			continue
		}
		candidates = append(candidates, k)
		if spec, exists := GetDocs(k, t); exists {
			if len(inferred) > 0 {
				candidates = []string{inferred, k}
				sort.Strings(candidates)
				return "", ComponentSpec{}, fmt.Errorf(
					"unable to infer %v type, multiple candidates '%v' and '%v'", string(t), candidates[0], candidates[1],
				)
			}
			inferred = k
			inferredSpec = spec
		}
	}

	if len(candidates) == 0 && len(defaultType) > 0 {
		// A totally empty component config results in the default.
		// TODO: V4 Disable this
		if spec, exists := GetDocs(defaultType, t); exists {
			return defaultType, spec, nil
		}
	}

	if len(inferred) == 0 {
		sort.Strings(candidates)
		return "", ComponentSpec{}, fmt.Errorf("unable to infer %v type, candidates were: %v", string(t), candidates)
	}
	return inferred, inferredSpec, nil
}

// TODO: V4 Remove this.
func sanitiseConditionConfig(raw interface{}, removeDeprecated bool) error {
	// This is a nasty hack until Benthos v4.
	m, ok := raw.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected object configuration type, found: %T", raw)
	}
	typeStr, ok := m["type"]
	if !ok {
		return nil
	}
	for k := range m {
		if k == typeStr || k == "type" || k == "plugin" {
			continue
		}
		delete(m, k)
	}
	return nil
}

// SanitiseComponentConfig reduces a raw component configuration into only the
// fields for the component name configured.
func SanitiseComponentConfig(componentType Type, raw interface{}, filter FieldFilter) error {
	if componentType == "condition" {
		return sanitiseConditionConfig(raw, false)
	}

	name, spec, err := GetInferenceCandidate(componentType, "", raw)
	if err != nil {
		return err
	}

	m, ok := raw.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected object configuration type, found: %T", raw)
	}

	if componentConfRaw, exists := m[name]; exists {
		spec.Config.sanitise(componentConfRaw, filter)
	}

	reservedFields := reservedFieldsByType(componentType)
	for k, v := range m {
		if k == name {
			continue
		}
		spec, exists := reservedFields[k]
		if !exists {
			delete(m, k)
		}
		if spec.omitWhen != nil && spec.omitWhen(v, m) {
			delete(m, k)
		}
	}

	for name, fieldSpec := range reservedFields {
		fieldSpec.sanitise(m[name], filter)
	}
	return nil
}

// OrderComponentConfig takes a raw configuration object and returns a yaml node
// type with the fields ordered by the field spec. Also optionally removes the
// `type` field from this and all nested components.
func OrderComponentConfig(cType Type, spec FieldSpec, raw interface{}, removeTypeField bool) (*yaml.Node, error) {
	var node *yaml.Node
	var ok bool
	if node, ok = raw.(*yaml.Node); !ok {
		rawBytes, err := yaml.Marshal(raw)
		if err != nil {
			return nil, err
		}
		var newNode yaml.Node
		if err = yaml.Unmarshal(rawBytes, &newNode); err != nil {
			return nil, err
		}
		if newNode.Kind != yaml.DocumentNode {
			return nil, fmt.Errorf("expected document node kind: %v", newNode.Kind)
		}
		if newNode.Content[0].Kind != yaml.MappingNode {
			return nil, fmt.Errorf("expected mapping node child kind: %v", newNode.Content[0].Kind)
		}
		node = newNode.Content[0]
	}

	if cType == "condition" {
		return node, nil
	}

	newNodes := []*yaml.Node{}

	var name string
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == "type" {
			name = node.Content[i+1].Value
			if !removeTypeField {
				newNodes = append(newNodes, node.Content[i])
				newNodes = append(newNodes, node.Content[i+1])
			}
			break
		}
	}
	if len(name) == 0 {
		if len(node.Content) == 0 {
			return node, nil
		}
		return nil, fmt.Errorf("type field not found for component %v config", cType)
	}

	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == name {
			if err := spec.configOrderedFromNode(node.Content[i+1], removeTypeField); err != nil {
				return nil, err
			}
			newNodes = append(newNodes, node.Content[i])
			newNodes = append(newNodes, node.Content[i+1])
			break
		}
	}

	reservedFields := reservedFieldsByType(cType)
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == name || node.Content[i].Value == "type" {
			continue
		}
		spec, exists := reservedFields[node.Content[i].Value]
		if exists {
			if err := spec.configOrderedFromNode(node.Content[i+1], removeTypeField); err != nil {
				return nil, err
			}
		}
		newNodes = append(newNodes, node.Content[i])
		newNodes = append(newNodes, node.Content[i+1])
	}

	node.Content = newNodes
	return node, nil
}
