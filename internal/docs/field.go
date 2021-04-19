package docs

import (
	"fmt"
	"reflect"

	"gopkg.in/yaml.v3"
)

// LintContext is provided to linting functions, and provides context about the
// wider configuration.
type LintContext struct {
	// A map of label names to the line they were defined at.
	Labels map[string]int
}

// NewLintContext creates a new linting context.
func NewLintContext() LintContext {
	return LintContext{
		Labels: map[string]int{},
	}
}

// LintFunc is a common linting function for field values.
type LintFunc func(ctx LintContext, line, col int, value interface{}) []Lint

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

	omitWhenFn   func(field, parent interface{}) (string, bool)
	customLintFn LintFunc
	skipLint     bool
}

// IsInterpolated indicates that the field supports interpolation functions.
func (f FieldSpec) IsInterpolated() FieldSpec {
	f.Interpolated = true
	f.customLintFn = LintBloblangField
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
func (f FieldSpec) OmitWhen(fn func(field, parent interface{}) (string, bool)) FieldSpec {
	f.omitWhenFn = fn
	return f
}

// Linter adds a linting function to a field. When linting is performed on a
// config the provided function will be called with a boxed variant of the field
// value, allowing it to perform linting on that value.
func (f FieldSpec) Linter(fn LintFunc) FieldSpec {
	f.customLintFn = fn
	return f
}

// Unlinted returns a field spec that will not be lint checked during a config
// parse.
func (f FieldSpec) Unlinted() FieldSpec {
	f.skipLint = true
	return f
}

func (f FieldSpec) shouldOmit(field, parent interface{}) (string, bool) {
	if f.omitWhenFn == nil {
		return "", false
	}
	return f.omitWhenFn(field, parent)
}

func (f FieldSpec) shouldOmitNode(fieldNode, parentNode *yaml.Node) (string, bool) {
	if f.omitWhenFn == nil {
		return "", false
	}
	var field, parent interface{}
	if err := fieldNode.Decode(&field); err != nil {
		return "", false
	}
	if err := parentNode.Decode(&parent); err != nil {
		return "", false
	}
	return f.omitWhenFn(field, parent)
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
			_ = SanitiseComponentConfig(coreType, s, filter)
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

// SanitiseNode attempts to reduce a parsed config (as a *yaml.Node) down into a
// minimal representation without changing the behaviour of the config. The
// fields of the result will also be sorted according to the field spec.
func (f FieldSpec) SanitiseNode(node *yaml.Node, conf SanitiseConfig) error {
	if coreType, isCore := f.Type.IsCoreComponent(); isCore {
		if f.IsArray {
			for i := 0; i < len(node.Content); i++ {
				if err := SanitiseNode(coreType, node.Content[i], conf); err != nil {
					return err
				}
			}
		} else if f.IsMap {
			for i := 0; i < len(node.Content)-1; i += 2 {
				if err := SanitiseNode(coreType, node.Content[i+1], conf); err != nil {
					return err
				}
			}
		} else if err := SanitiseNode(coreType, node, conf); err != nil {
			return err
		}
	} else if len(f.Children) > 0 {
		if f.IsArray {
			for i := 0; i < len(node.Content); i++ {
				if err := f.Children.SanitiseNode(node.Content[i], conf); err != nil {
					return err
				}
			}
		} else if f.IsMap {
			for i := 0; i < len(node.Content)-1; i += 2 {
				if err := f.Children.SanitiseNode(node.Content[i+1], conf); err != nil {
					return err
				}
			}
		} else if err := f.Children.SanitiseNode(node, conf); err != nil {
			return err
		}
	}
	return nil
}

// LintLevel describes the severity level of a linting error.
type LintLevel int

// Lint levels
const (
	LintError   LintLevel = iota
	LintWarning LintLevel = iota
)

// Lint describes a single linting issue found with a Benthos config.
type Lint struct {
	Line   int
	Column int // Optional, omitted from lint report unless >= 1
	Level  LintLevel
	What   string
}

// NewLintError returns an error lint.
func NewLintError(line int, msg string) Lint {
	return Lint{Line: line, Level: LintError, What: msg}
}

// NewLintWarning returns a warning lint.
func NewLintWarning(line int, msg string) Lint {
	return Lint{Line: line, Level: LintWarning, What: msg}
}

func (f FieldSpec) lintNode(ctx LintContext, node *yaml.Node) []Lint {
	if f.skipLint {
		return nil
	}
	var lints []Lint
	if f.IsArray {
		if node.Kind != yaml.SequenceNode {
			lints = append(lints, NewLintError(node.Line, "expected array value"))
			return lints
		}
		for i := 0; i < len(node.Content); i++ {
			lints = append(lints, customLint(ctx, f, node.Content[i])...)
		}
	} else if f.IsMap {
		if node.Kind != yaml.MappingNode {
			lints = append(lints, NewLintError(node.Line, "expected object value"))
			return lints
		}
		for i := 0; i < len(node.Content)-1; i += 2 {
			lints = append(lints, customLint(ctx, f, node.Content[i+1])...)
		}
	} else {
		lints = append(lints, customLint(ctx, f, node)...)
	}
	if coreType, isCore := f.Type.IsCoreComponent(); isCore {
		if f.IsArray {
			for i := 0; i < len(node.Content); i++ {
				lints = append(lints, LintNode(ctx, coreType, node.Content[i])...)
			}
		} else if f.IsMap {
			for i := 0; i < len(node.Content)-1; i += 2 {
				lints = append(lints, LintNode(ctx, coreType, node.Content[i+1])...)
			}
		} else {
			lints = append(lints, LintNode(ctx, coreType, node)...)
		}
	} else if len(f.Children) > 0 {
		if f.IsArray {
			for i := 0; i < len(node.Content); i++ {
				lints = append(lints, f.Children.LintNode(ctx, node.Content[i])...)
			}
		} else if f.IsMap {
			for i := 0; i < len(node.Content)-1; i += 2 {
				lints = append(lints, f.Children.LintNode(ctx, node.Content[i+1])...)
			}
		} else {
			lints = append(lints, f.Children.LintNode(ctx, node)...)
		}
	}
	return lints
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
	FieldBuffer    FieldType = "buffer"
	FieldCache     FieldType = "cache"
	FieldCondition FieldType = "condition"
	FieldProcessor FieldType = "processor"
	FieldRateLimit FieldType = "rate_limit"
	FieldOutput    FieldType = "output"
	FieldMetrics   FieldType = "metrics"
	FieldTracer    FieldType = "tracer"
)

// IsCoreComponent returns the core component type of a field if applicable.
func (t FieldType) IsCoreComponent() (Type, bool) {
	switch t {
	case FieldInput:
		return TypeInput, true
	case FieldBuffer:
		return TypeBuffer, true
	case FieldCache:
		return TypeCache, true
	case FieldCondition:
		// TODO: V4 Remove this
		return "condition", true
	case FieldProcessor:
		return TypeProcessor, true
	case FieldRateLimit:
		return TypeRateLimit, true
	case FieldOutput:
		return TypeOutput, true
	case FieldTracer:
		return TypeTracer, true
	case FieldMetrics:
		return TypeMetrics, true
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
		if _, omit := spec.shouldOmit(v, m); omit {
			delete(m, spec.Name)
		} else {
			spec.sanitise(v, filter)
		}
	}
}

// SanitiseNode attempts to reduce a parsed config (as a *yaml.Node) down into a
// minimal representation without changing the behaviour of the config. The
// fields of the result will also be sorted according to the field spec.
func (f FieldSpecs) SanitiseNode(node *yaml.Node, conf SanitiseConfig) error {
	// Following the order of our field specs, extract each field.
	newNodes := []*yaml.Node{}
	for _, field := range f {
		if field.Deprecated && conf.RemoveDeprecated {
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
			if _, omit := field.shouldOmitNode(nextNode, node); omit {
				break
			}
			if err := field.SanitiseNode(nextNode, conf); err != nil {
				return err
			}
			newNodes = append(newNodes, node.Content[i], nextNode)
			break
		}
	}
	node.Content = newNodes
	return nil
}

func nodeToInterface(node *yaml.Node) (interface{}, error) {
	var i interface{}
	if err := node.Decode(&i); err != nil {
		return nil, err
	}
	return i, nil
}

func lintFromOmit(spec FieldSpec, parent, node *yaml.Node) []Lint {
	var lints []Lint
	if spec.omitWhenFn != nil {
		fieldValue, err := nodeToInterface(node)
		if err != nil {
			lints = append(lints, NewLintWarning(node.Line, "failed to marshal value"))
			return lints
		}
		parentMap, err := nodeToInterface(parent)
		if err != nil {
			lints = append(lints, NewLintWarning(node.Line, "failed to marshal parent"))
			return lints
		}
		if why, omit := spec.shouldOmit(fieldValue, parentMap); omit {
			lints = append(lints, NewLintError(node.Line, why))
		}
	}
	return lints
}

func customLint(ctx LintContext, spec FieldSpec, node *yaml.Node) []Lint {
	if spec.customLintFn == nil {
		return nil
	}
	fieldValue, err := nodeToInterface(node)
	if err != nil {
		return []Lint{NewLintWarning(node.Line, "failed to marshal value")}
	}
	lints := spec.customLintFn(ctx, node.Line, node.Column, fieldValue)
	return lints
}

// LintNode walks a yaml node and returns a list of linting errors found.
func (f FieldSpecs) LintNode(ctx LintContext, node *yaml.Node) []Lint {
	var lints []Lint

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
		lints = append(lints, lintFromOmit(spec, node, node.Content[i+1])...)
		lints = append(lints, spec.lintNode(ctx, node.Content[i+1])...)
	}
	return lints
}

//------------------------------------------------------------------------------
