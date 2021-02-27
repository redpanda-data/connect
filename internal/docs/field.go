package docs

import (
	"reflect"

	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

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

	// IsArray indicates whether this field is an array of the FieldType.
	IsArray bool

	// IsMap indicates whether this field is a map of keys to the FieldType.
	IsMap bool

	// Interpolation indicates that the field supports interpolation functions.
	Interpolated bool

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

	omitWhen func(v interface{}) bool
}

// IsInterpolated indicates that the field supports interpolation functions.
func (f FieldSpec) IsInterpolated() FieldSpec {
	f.Interpolated = true
	return f
}

// HasType returns a new FieldSpec that specifies a specific type.
func (f FieldSpec) HasType(t FieldType) FieldSpec {
	f.Type = t
	return f
}

// Array determines that this field is an array of the field type.
func (f FieldSpec) Array() FieldSpec {
	f.IsMap = false
	f.IsArray = true
	return f
}

// Map determines that this field is a map of arbitrary keys to a field type.
func (f FieldSpec) Map() FieldSpec {
	f.IsMap = true
	f.IsArray = false
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
		f.Type = FieldObject
	}
	f.Children = append(f.Children, children...)
	return f
}

// OmitWhen specifies a custom func that, when provided a generic config struct,
// returns a boolean indicating when the field can be safely omitted from a
// config.
func (f FieldSpec) OmitWhen(fn func(c interface{}) bool) FieldSpec {
	f.omitWhen = fn
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

// FieldComponent returns a field spec for a component.
func FieldComponent() FieldSpec {
	return FieldSpec{}
}

// FieldDeprecated returns a field spec for a deprecated field.
func FieldDeprecated(name string) FieldSpec {
	return FieldSpec{
		Name:        name,
		Description: "DEPRECATED: Do not use.",
		Deprecated:  true,
	}
}

func (f FieldSpec) sanitise(s interface{}, filter FieldFilter) {
	if coreType, isCore := f.Type.IsCoreComponent(); isCore {
		if f.IsArray {
			if arr, ok := s.([]interface{}); ok {
				for _, ele := range arr {
					_ = SanitiseComponentConfig(coreType, ele, filter)
				}
			}
		} else if f.IsMap {
			if obj, ok := s.(map[string]interface{}); ok {
				for _, v := range obj {
					_ = SanitiseComponentConfig(coreType, v, filter)
				}
			}
		} else {
			SanitiseComponentConfig(coreType, s, filter)
		}
	} else if len(f.Children) > 0 {
		if f.IsArray {
			if arr, ok := s.([]interface{}); ok {
				for _, ele := range arr {
					f.Children.sanitise(ele, filter)
				}
			}
		} else if f.IsMap {
			if obj, ok := s.(map[string]interface{}); ok {
				for _, v := range obj {
					f.Children.sanitise(v, filter)
				}
			}
		} else {
			f.Children.sanitise(s, filter)
		}
	}
}

func (f FieldSpec) configOrderedFromNode(node *yaml.Node, removeTypeField bool) error {
	if coreType, isCore := f.Type.IsCoreComponent(); isCore {
		if f.IsArray {
			// TODO
		} else if f.IsMap {
			for i := 0; i < len(node.Content); i += 2 {
				newNode, err := OrderComponentConfig(coreType, f, node.Content[i+1], removeTypeField)
				if err != nil {
					return err
				}
				node.Content[i+1] = newNode
			}
		} else {
			newNode, err := OrderComponentConfig(coreType, f, node, removeTypeField)
			if err != nil {
				return err
			}
			node.Content = newNode.Content
		}
	} else if len(f.Children) > 0 {
		if f.IsArray {
			// TODO
		} else if f.IsMap {
			for i := 0; i < len(node.Content); i += 2 {
				if err := f.Children.configOrderedFromNode(node.Content[i+1], removeTypeField); err != nil {
					return err
				}
			}
		} else {
			if err := f.Children.configOrderedFromNode(node, removeTypeField); err != nil {
				return err
			}
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// FieldType represents a field type.
type FieldType string

// ValueType variants.
var (
	FieldString  FieldType = "string"
	FieldNumber  FieldType = "number"
	FieldBool    FieldType = "bool"
	FieldObject  FieldType = "object"
	FieldUnknown FieldType = "unknown"

	// Core component types, only components that can be a child of another
	// component config are listed here.
	FieldInput     FieldType = "input"
	FieldCondition FieldType = "condition"
	FieldProcessor FieldType = "processor"
	FieldOutput    FieldType = "output"
	FieldMetrics   FieldType = "metrics"
)

// IsCoreComponent returns the core component type of a field if applicable.
func (t FieldType) IsCoreComponent() (Type, bool) {
	switch t {
	case FieldInput:
		return TypeInput, true
	case FieldCondition:
		// TODO: V4 Remove this
		return "condition", true
	case FieldProcessor:
		return TypeProcessor, true
	case FieldOutput:
		return TypeOutput, true
	}
	return "", false
}

func getFieldTypeFromInterface(v interface{}) (FieldType, bool) {
	return getFieldTypeFromReflect(reflect.TypeOf(v))
}

func getFieldTypeFromReflect(t reflect.Type) (FieldType, bool) {
	switch t.Kind().String() {
	case "map":
		return FieldObject, false
	case "slice":
		ft, _ := getFieldTypeFromReflect(t.Elem())
		return ft, true
	case "float64", "int", "int64":
		return FieldNumber, false
	case "string":
		return FieldString, false
	case "bool":
		return FieldBool, false
	}
	return FieldUnknown, false
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

// FieldFilter defines a filter closure that returns a boolean for a component
// field indicating whether the field should be kept within a generated config.
type FieldFilter func(spec FieldSpec) bool

func (f FieldFilter) shouldDrop(spec FieldSpec) bool {
	if f == nil {
		return false
	}
	return !f(spec)
}

// ShouldDropDeprecated returns a field filter that removes all deprecated
// fields when the boolean argument is true.
func ShouldDropDeprecated(b bool) FieldFilter {
	if !b {
		return nil
	}
	return func(spec FieldSpec) bool {
		return !spec.Deprecated
	}
}

func (f FieldSpecs) sanitise(s interface{}, filter FieldFilter) {
	m, ok := s.(map[string]interface{})
	if !ok {
		return
	}
	for _, spec := range f {
		if filter.shouldDrop(spec) {
			delete(m, spec.Name)
			continue
		}
		v := m[spec.Name]
		if spec.omitWhen != nil && spec.omitWhen(v) {
			delete(m, spec.Name)
		} else {
			spec.sanitise(v, filter)
		}
	}
}

func (f FieldSpecs) configOrderedFromNode(node *yaml.Node, removeTypeField bool) error {
	// Following the order of our field specs, extract each field.
	newNodes := []*yaml.Node{}
	for _, field := range f {
	searchLoop:
		for i := 0; i < len(node.Content); i += 2 {
			if node.Content[i].Value == field.Name {
				nextNode := node.Content[i+1]
				if err := field.configOrderedFromNode(nextNode, removeTypeField); err != nil {
					return err
				}
				newNodes = append(newNodes, node.Content[i])
				newNodes = append(newNodes, nextNode)
				break searchLoop
			}
		}
	}
	node.Content = newNodes
	return nil
}

//------------------------------------------------------------------------------
