package docs

import (
	"fmt"
	"reflect"
	"sort"

	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// FieldInterpolation represents a type of interpolation supported by a field.
type FieldInterpolation int

// Interpolation types.
const (
	FieldInterpolationNone FieldInterpolation = iota
	FieldInterpolationBatchWide
	FieldInterpolationIndividual
)

// FieldSpec describes a component config field.
type FieldSpec struct {
	// Name of the field (as it appears in config).
	Name string

	// Description of the field purpose (in markdown).
	Description string

	// Advanced is true for optional fields that will not be present in most
	// configs.
	Advanced bool

	// Deprecated is true for fields that are deprecated and only exist for
	// backwards compatibility reasons.
	Deprecated bool

	// Default value of the field. If left nil the docs generator will attempt
	// to infer the default value from an example config.
	Default interface{}

	// Type of the field. This is optional and doesn't prevent documentation for
	// a field.
	Type FieldType

	// Interpolation indicates the type of interpolation that this field
	// supports.
	Interpolation FieldInterpolation

	// Examples is a slice of optional example values for a field.
	Examples []interface{}

	// AnnotatedOptions for this field. Each option should have a summary.
	AnnotatedOptions [][2]string

	// Options for this field.
	Options []string

	// Children fields of this field (it must be an object).
	Children FieldSpecs

	// Version lists an explicit Benthos release where this fields behaviour was last modified.
	Version string
}

// SupportsInterpolation returns a new FieldSpec that specifies whether it
// supports function interpolation (batch wide or not).
func (f FieldSpec) SupportsInterpolation(batchWide bool) FieldSpec {
	if batchWide {
		f.Interpolation = FieldInterpolationBatchWide
	} else {
		f.Interpolation = FieldInterpolationIndividual
	}
	return f
}

// HasType returns a new FieldSpec that specifies a specific type.
func (f FieldSpec) HasType(t FieldType) FieldSpec {
	f.Type = t
	return f
}

// HasDefault returns a new FieldSpec that specifies a default value.
func (f FieldSpec) HasDefault(v interface{}) FieldSpec {
	f.Default = v
	return f
}

// AtVersion specifies the version at which this fields behaviour was last
// modified.
func (f FieldSpec) AtVersion(v string) FieldSpec {
	f.Version = v
	return f
}

// HasAnnotatedOptions returns a new FieldSpec that specifies a specific list of
// annotated options. Either
func (f FieldSpec) HasAnnotatedOptions(options ...string) FieldSpec {
	if len(f.Options) > 0 {
		panic("cannot combine annotated and non-annotated options for a field")
	}
	if len(options)%2 != 0 {
		panic("annotated field options must each have a summary")
	}
	for i := 0; i < len(options); i += 2 {
		f.AnnotatedOptions = append(f.AnnotatedOptions, [2]string{
			options[i], options[i+1],
		})
	}
	return f
}

// HasOptions returns a new FieldSpec that specifies a specific list of options.
func (f FieldSpec) HasOptions(options ...string) FieldSpec {
	if len(f.AnnotatedOptions) > 0 {
		panic("cannot combine annotated and non-annotated options for a field")
	}
	f.Options = options
	return f
}

// WithChildren returns a new FieldSpec that has child fields.
func (f FieldSpec) WithChildren(children ...FieldSpec) FieldSpec {
	if len(f.Type) == 0 {
		f.Type = "object"
	}
	f.Children = append(f.Children, children...)
	return f
}

// FieldAdvanced returns a field spec for an advanced field.
func FieldAdvanced(name, description string, examples ...interface{}) FieldSpec {
	return FieldSpec{
		Name:        name,
		Description: description,
		Advanced:    true,
		Examples:    examples,
	}
}

// FieldCommon returns a field spec for a common field.
func FieldCommon(name, description string, examples ...interface{}) FieldSpec {
	return FieldSpec{
		Name:        name,
		Description: description,
		Examples:    examples,
	}
}

// FieldDeprecated returns a field spec for a deprecated field.
func FieldDeprecated(name string) FieldSpec {
	return FieldSpec{
		Name:        name,
		Description: "DEPRECATED: Do not use.",
		Deprecated:  true,
	}
}

//------------------------------------------------------------------------------

// FieldType represents a field type.
type FieldType string

// ValueType variants.
var (
	FieldString  FieldType = "string"
	FieldNumber  FieldType = "number"
	FieldBool    FieldType = "bool"
	FieldArray   FieldType = "array"
	FieldObject  FieldType = "object"
	FieldUnknown FieldType = "unknown"
)

// GetFieldType attempts to extract a field type from a general value.
func GetFieldType(v interface{}) FieldType {
	switch reflect.TypeOf(v).Kind().String() {
	case "map":
		return FieldObject
	case "slice":
		return FieldArray
	case "float64", "int", "int64":
		return FieldNumber
	case "string":
		return FieldString
	case "bool":
		return FieldBool
	}
	return FieldUnknown
}

//------------------------------------------------------------------------------

// FieldSpecs is a slice of field specs for a component.
type FieldSpecs []FieldSpec

// Merge with another set of FieldSpecs.
func (f FieldSpecs) Merge(specs FieldSpecs) FieldSpecs {
	return append(f, specs...)
}

// Add more field specs.
func (f FieldSpecs) Add(specs ...FieldSpec) FieldSpecs {
	return append(f, specs...)
}

// RemoveDeprecated fields from a sanitized config.
func (f FieldSpecs) RemoveDeprecated(s interface{}) {
	m, ok := s.(map[string]interface{})
	if !ok {
		return
	}
	for _, spec := range f {
		if spec.Deprecated {
			delete(m, spec.Name)
		} else if len(spec.Children) > 0 {
			spec.Children.RemoveDeprecated(m[spec.Name])
		}
	}
}

// ConfigCommon takes a sanitised configuration of a component, a map of field
// docs, and removes all fields that aren't common or are deprecated.
func (f FieldSpecs) ConfigCommon(config interface{}) (interface{}, error) {
	return f.configFiltered(config, func(field FieldSpec) bool {
		return !(field.Advanced || field.Deprecated)
	})
}

// ConfigAdvanced takes a sanitised configuration of a component, a map of field
// docs, and removes all fields that are deprecated.
func (f FieldSpecs) ConfigAdvanced(config interface{}) (interface{}, error) {
	return f.configFiltered(config, func(field FieldSpec) bool {
		return !field.Deprecated
	})
}

func (f FieldSpecs) configFiltered(config interface{}, filter func(f FieldSpec) bool) (interface{}, error) {
	var asNode yaml.Node
	var ok bool
	if asNode, ok = config.(yaml.Node); !ok {
		rawBytes, err := yaml.Marshal(config)
		if err != nil {
			return asNode, err
		}
		if err = yaml.Unmarshal(rawBytes, &asNode); err != nil {
			return asNode, err
		}
	}
	if asNode.Kind != yaml.DocumentNode {
		return asNode, fmt.Errorf("expected document node kind: %v", asNode.Kind)
	}
	if asNode.Content[0].Kind != yaml.MappingNode {
		return asNode, fmt.Errorf("expected mapping node child kind: %v", asNode.Content[0].Kind)
	}
	newChild, err := f.configFilteredFromNode(*asNode.Content[0], filter)
	if err != nil {
		return asNode, err
	}
	return &newChild, nil
}

func orderNode(node *yaml.Node) {
	sourceNodes := [][2]*yaml.Node{}
	for i := 0; i < len(node.Content); i += 2 {
		sourceNodes = append(sourceNodes, [2]*yaml.Node{
			node.Content[i],
			node.Content[i+1],
		})
	}
	sort.Slice(sourceNodes, func(i, j int) bool {
		return sourceNodes[i][0].Value < sourceNodes[j][0].Value
	})
	for i, nodes := range sourceNodes {
		if nodes[1].Kind == yaml.MappingNode {
			orderNode(nodes[1])
		}
		node.Content[i*2] = nodes[0]
		node.Content[i*2+1] = nodes[1]
	}
}

func (f FieldSpecs) configFilteredFromNode(node yaml.Node, filter func(FieldSpec) bool) (*yaml.Node, error) {
	// First, order the nodes as they currently exist.
	orderNode(&node)

	// Next, following the order of our field specs, extract the fields we're
	// targetting.
	newNodes := []*yaml.Node{}
	for _, field := range f {
		if !filter(field) {
			continue
		}
	searchLoop:
		for i := 0; i < len(node.Content); i += 2 {
			if node.Content[i].Value == field.Name {
				nextNode := node.Content[i+1]
				if len(field.Children) > 0 && field.Type != FieldArray {
					if nextNode.Kind != yaml.MappingNode {
						return nil, fmt.Errorf("expected mapping node kind: %v", nextNode.Kind)
					}
					var err error
					if nextNode, err = field.Children.configFilteredFromNode(*nextNode, filter); err != nil {
						return nil, err
					}
				}
				newNodes = append(newNodes, node.Content[i])
				newNodes = append(newNodes, nextNode)
				break searchLoop
			}
		}
	}
	node.Content = newNodes
	return &node, nil
}

//------------------------------------------------------------------------------
