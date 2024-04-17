package docs

import (
	"errors"
	"fmt"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/value"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

// FieldType represents a field type.
type FieldType string

// ValueType variants.
var (
	FieldTypeString  FieldType = "string"
	FieldTypeInt     FieldType = "int"
	FieldTypeFloat   FieldType = "float"
	FieldTypeBool    FieldType = "bool"
	FieldTypeObject  FieldType = "object"
	FieldTypeUnknown FieldType = "unknown"

	// Core component types, only components that can be a child of another
	// component config are listed here.
	FieldTypeInput     FieldType = "input"
	FieldTypeBuffer    FieldType = "buffer"
	FieldTypeCache     FieldType = "cache"
	FieldTypeProcessor FieldType = "processor"
	FieldTypeRateLimit FieldType = "rate_limit"
	FieldTypeOutput    FieldType = "output"
	FieldTypeMetrics   FieldType = "metrics"
	FieldTypeTracer    FieldType = "tracer"
	FieldTypeScanner   FieldType = "scanner"
)

// IsCoreComponent returns the core component type of a field if applicable.
func (t FieldType) IsCoreComponent() (Type, bool) {
	switch t {
	case FieldTypeInput:
		return TypeInput, true
	case FieldTypeBuffer:
		return TypeBuffer, true
	case FieldTypeCache:
		return TypeCache, true
	case FieldTypeProcessor:
		return TypeProcessor, true
	case FieldTypeRateLimit:
		return TypeRateLimit, true
	case FieldTypeOutput:
		return TypeOutput, true
	case FieldTypeTracer:
		return TypeTracer, true
	case FieldTypeMetrics:
		return TypeMetrics, true
	case FieldTypeScanner:
		return TypeScanner, true
	}
	return "", false
}

// FieldKind represents a field kind.
type FieldKind string

// ValueType variants.
var (
	KindScalar  FieldKind = "scalar"
	KindArray   FieldKind = "array"
	Kind2DArray FieldKind = "2darray"
	KindMap     FieldKind = "map"
)

//------------------------------------------------------------------------------

// FieldSpec describes a component config field.
type FieldSpec struct {
	// Name of the field (as it appears in config).
	Name string `json:"name"`

	// Type of the field.
	Type FieldType `json:"type"`

	// Kind of the field.
	Kind FieldKind `json:"kind"`

	// Description of the field purpose (in markdown).
	Description string `json:"description,omitempty"`

	// IsAdvanced is true for optional fields that will not be present in most
	// configs.
	IsAdvanced bool `json:"is_advanced,omitempty"`

	// IsDeprecated is true for fields that are deprecated and only exist
	// for backwards compatibility reasons.
	IsDeprecated bool `json:"is_deprecated,omitempty"`

	// IsOptional is a boolean flag indicating that a field is optional, even
	// if there is no default. This prevents linting errors when the field
	// is missing.
	IsOptional bool `json:"is_optional,omitempty"`

	// IsSecret indicates whether the field represents information that is
	// generally considered sensitive such as passwords or access tokens.
	IsSecret bool `json:"is_secret,omitempty"`

	// Default value of the field.
	Default *any `json:"default,omitempty"`

	// Interpolation indicates that the field supports interpolation
	// functions.
	Interpolated bool `json:"interpolated,omitempty"`

	// Bloblang indicates that a string field is a Bloblang mapping.
	Bloblang bool `json:"bloblang,omitempty"`

	// Examples is a slice of optional example values for a field.
	Examples []any `json:"examples,omitempty"`

	// AnnotatedOptions for this field. Each option should have a summary.
	AnnotatedOptions [][2]string `json:"annotated_options,omitempty"`

	// Options for this field.
	Options []string `json:"options,omitempty"`

	// Children fields of this field (it must be an object).
	Children FieldSpecs `json:"children,omitempty"`

	// Version is an explicit version when this field was introduced.
	Version string `json:"version,omitempty"`

	// Linter is an optional bloblang mapping that should be used in order to
	// lint a field.
	Linter string `json:"linter,omitempty"`

	// Scrubber is an optional bloblang mapping that should be used in order to
	// scrub sensitive information from field values when echoed.
	Scrubber string `json:"scrubber,omitempty"`

	omitWhenFn   func(field, parent any) (why string, shouldOmit bool)
	customLintFn LintFunc
}

// IsInterpolated indicates that the field supports interpolation functions.
func (f FieldSpec) IsInterpolated() FieldSpec {
	f.Interpolated = true
	return f
}

// IsBloblang indicates that the field is a Bloblang mapping.
func (f FieldSpec) IsBloblang() FieldSpec {
	f.Bloblang = true
	return f
}

// HasType returns a new FieldSpec that specifies a specific type.
func (f FieldSpec) HasType(t FieldType) FieldSpec {
	f.Type = t
	return f
}

// Optional marks this field as being optional, and therefore its absence in a
// config is not considered an error even when a default value is not provided.
func (f FieldSpec) Optional() FieldSpec {
	f.IsOptional = true
	return f
}

const bloblREEnvVar = `\${[0-9A-Za-z_.]+(:((\${[^}]+})|[^}])*)?}`

// Secret marks this field as being a secret, which means it represents
// information that is generally considered sensitive such as passwords or
// access tokens.
func (f FieldSpec) Secret() FieldSpec {
	f.IsSecret = true
	f.Scrubber = fmt.Sprintf(`root = if this != null && this != "" && !this.trim().re_match("""^%v$""") {
  "!!!SECRET_SCRUBBED!!!"
} else if this == null { "" }`, bloblREEnvVar)
	return f
}

// Advanced marks this field as being advanced, and therefore not commonly used.
func (f FieldSpec) Advanced() FieldSpec {
	f.IsAdvanced = true
	for i, v := range f.Children {
		f.Children[i] = v.Advanced()
	}
	return f
}

// Deprecated marks this field as being deprecated.
func (f FieldSpec) Deprecated() FieldSpec {
	f.IsDeprecated = true
	for i, v := range f.Children {
		f.Children[i] = v.Deprecated()
	}
	return f
}

// Array determines that this field is an array of the field type.
func (f FieldSpec) Array() FieldSpec {
	f.Kind = KindArray
	return f
}

// ArrayOfArrays determines that this is an array of arrays of the field type.
func (f FieldSpec) ArrayOfArrays() FieldSpec {
	f.Kind = Kind2DArray
	return f
}

// Map determines that this field is a map of arbitrary keys to a field type.
func (f FieldSpec) Map() FieldSpec {
	f.Kind = KindMap
	return f
}

// Scalar determines that this field is a scalar type (the default).
func (f FieldSpec) Scalar() FieldSpec {
	f.Kind = KindScalar
	return f
}

// HasDefault returns a new FieldSpec that specifies a default value.
func (f FieldSpec) HasDefault(v any) FieldSpec {
	f.Default = &v
	return f
}

// AtVersion specifies the version at which this fields behaviour was last
// modified.
func (f FieldSpec) AtVersion(v string) FieldSpec {
	f.Version = v
	return f
}

// HasAnnotatedOptions returns a new FieldSpec that specifies a specific list of
// annotated options. Field values are linted to ensure they match one of the
// given options by a case insensitive match, use a custom lint function in
// order to change this default behaviour.
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
	return f.lintOptions(false)
}

// HasOptions returns a new FieldSpec that specifies a specific list of options.
// Field values are linted to ensure they match one of the given options by a
// case insensitive match, use a custom lint function in order to change this
// default behaviour.
func (f FieldSpec) HasOptions(options ...string) FieldSpec {
	if len(f.AnnotatedOptions) > 0 {
		panic("cannot combine annotated and non-annotated options for a field")
	}
	f.Options = options
	return f.lintOptions(false)
}

// WithChildren returns a new FieldSpec that has child fields.
func (f FieldSpec) WithChildren(children ...FieldSpec) FieldSpec {
	if len(f.Type) == 0 {
		f.Type = FieldTypeObject
	}
	if f.IsAdvanced {
		for i, v := range children {
			children[i] = v.Advanced()
		}
	}
	f.Children = append(f.Children, children...)
	return f
}

// OmitWhen specifies a custom func that, when provided a generic config struct,
// returns a boolean indicating when the field can be safely omitted from a
// config.
func (f FieldSpec) OmitWhen(fn func(field, parent any) (why string, shouldOmit bool)) FieldSpec {
	f.omitWhenFn = fn
	return f
}

// LinterFunc adds a linting function to a field. When linting is performed on a
// config the provided function will be called with a boxed variant of the field
// value, allowing it to perform linting on that value.
//
// It is important to note that for fields defined as a non-scalar (array,
// array of arrays, map, etc) the linting rule will be executed on the highest
// level (array) and also the individual scalar values. If your field is a high
// level type then make sure your linting rule checks the type of the value
// provided in order to limit when the linting is performed.
//
// Note that a linting rule defined this way will only be effective in the
// binary that defines it as the function cannot be serialized into a portable
// schema.
func (f FieldSpec) LinterFunc(fn LintFunc) FieldSpec {
	f.Linter = ""
	f.customLintFn = fn
	return f
}

func lintsFromAny(line int, v any) (lints []Lint) {
	switch t := v.(type) {
	case []any:
		for _, e := range t {
			lints = append(lints, lintsFromAny(line, e)...)
		}
	case map[string]any:
		// Note: this is a long winded way to do IGetInt from the internal
		// package, I'm doing it so that this package no longer depends on other
		// internal packages (when possible).
		var typeInt int64
		_ = bloblang.NewArgSpec().Int64Var(&typeInt).Extract([]any{t["type"]})
		lints = append(lints, NewLintError(line, LintType(typeInt), errors.New(t["what"].(string))))
	case string:
		if t != "" {
			lints = append(lints, NewLintError(line, LintCustom, errors.New(t)))
		}
	}
	return
}

// LinterBlobl adds a linting function to a field. When linting is performed on
// a config the provided bloblang mapping will be called with a boxed variant of
// the field value, allowing it to perform linting on that value, where an array
// of lints (strings) should be returned.
//
// It is important to note that for fields defined as a non-scalar (array,
// array of arrays, map, etc) the linting rule will be executed on the highest
// level (array) and also the individual scalar values. If your field is a high
// level type then make sure your linting rule checks the type of the value
// provided in order to limit when the linting is performed.
//
// Note that a linting rule defined this way will only be effective in the
// binary that defines it as the function cannot be serialized into a portable
// schema.
func (f FieldSpec) LinterBlobl(blobl string) FieldSpec {
	if blobl == "" {
		f.Linter = blobl
		f.customLintFn = nil
		return f
	}

	env := bloblang.NewEnvironment().OnlyPure()

	m, err := env.Parse(blobl)
	if err != nil {
		f.customLintFn = func(ctx LintContext, line, col int, value any) (lints []Lint) {
			return []Lint{NewLintError(line, LintCustom, fmt.Errorf("field lint mapping itself failed to parse: %w", err))}
		}
		return f
	}

	f.Linter = blobl
	f.customLintFn = func(ctx LintContext, line, col int, value any) (lints []Lint) {
		var res any
		err := m.Overlay(value, &res)
		if err != nil {
			if errors.Is(err, bloblang.ErrRootDeleted) {
				return
			}
			return []Lint{NewLintError(line, LintCustom, err)}
		}
		lints = append(lints, lintsFromAny(line, res)...)
		return
	}
	return f
}

// lintOptions enforces that a field value matches one of the provided options
// and returns a linting error if that is not the case. This is currently opt-in
// because some fields express options that are only a subset due to deprecated
// functionality.
func (f FieldSpec) lintOptions(caseSensitive bool) FieldSpec {
	var optionsBuilder strings.Builder
	_, _ = optionsBuilder.WriteString("{\n")
	addFn := func(o string) {
		if !caseSensitive {
			o = strings.ToLower(o)
		}
		_, _ = fmt.Fprintf(&optionsBuilder, "  %q: true,\n", o)
	}

	for _, o := range f.Options {
		addFn(o)
	}
	for _, kv := range f.AnnotatedOptions {
		addFn(kv[0])
	}
	_, _ = optionsBuilder.WriteString("}\n")

	maybeLowerCase := ""
	if !caseSensitive {
		maybeLowerCase = ".lowercase()"
	}

	f.Linter = fmt.Sprintf(`
let options = %v
root = if !$options.exists(this.string()%v) {
  {"type": 2, "what": "value %%v is not a valid option for this field".format(this.string())}
}
`, optionsBuilder.String(), maybeLowerCase)
	return f
}

func (f FieldSpec) ScrubValue(v any) (any, error) {
	if f.Scrubber == "" {
		return v, nil
	}

	env := bloblang.NewEnvironment().OnlyPure()

	m, err := env.Parse(f.Scrubber)
	if err != nil {
		return nil, fmt.Errorf("scrubber mapping failed to parse: %w", err)
	}

	res, err := m.Query(v)
	if err != nil {
		if errors.Is(err, bloblang.ErrRootDeleted) {
			return nil, nil
		}
		return nil, err
	}
	return res, nil
}

func (f FieldSpec) GetLintFunc() LintFunc {
	fn := f.customLintFn
	if fn == nil && f.Linter != "" {
		fn = f.LinterBlobl(f.Linter).customLintFn
	}
	if f.Interpolated {
		if fn != nil {
			innerFn := fn
			fn = func(ctx LintContext, line, col int, value any) []Lint {
				lints := innerFn(ctx, line, col, value)
				moreLints := LintBloblangField(ctx, line, col, value)
				return append(lints, moreLints...)
			}
		} else {
			fn = LintBloblangField
		}
	}
	if f.Bloblang {
		if fn != nil {
			innerFn := fn
			fn = func(ctx LintContext, line, col int, value any) []Lint {
				lints := innerFn(ctx, line, col, value)
				moreLints := LintBloblangMapping(ctx, line, col, value)
				return append(lints, moreLints...)
			}
		} else {
			fn = LintBloblangMapping
		}
	}
	return fn
}

// FieldAnything returns a field spec for any typed field.
func FieldAnything(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeUnknown)
}

// FieldObject returns a field spec for an object typed field.
func FieldObject(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeObject)
}

// FieldString returns a field spec for a common string typed field.
func FieldString(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeString)
}

// FieldInterpolatedString returns a field spec for a string typed field
// supporting dynamic interpolated functions.
func FieldInterpolatedString(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeString).IsInterpolated()
}

// FieldBloblang returns a field spec for a string typed field containing a
// Bloblang mapping.
func FieldBloblang(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeString).IsBloblang()
}

// FieldInt returns a field spec for a common int typed field.
func FieldInt(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeInt)
}

// FieldFloat returns a field spec for a common float typed field.
func FieldFloat(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeFloat)
}

// FieldBool returns a field spec for a common bool typed field.
func FieldBool(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeBool)
}

// FieldURL returns a field spec for a string typed field containing a URL, both
// linting rules and scrubbers are added.
func FieldURL(name, description string, examples ...any) FieldSpec {
	f := newField(name, description, examples...).HasType(FieldTypeString) /*.LinterBlobl(`
	root = this.parse_url().(deleted()).catch(err -> err)
	`)*/
	f.Scrubber = fmt.Sprintf(`
let pass = this.parse_url().user.password.or("")
root = if $pass != "" && !$pass.trim().re_match("""^%v$""") {
  "!!!SECRET_SCRUBBED!!!"
}
`, bloblREEnvVar)
	return f
}

// FieldInput returns a field spec for an input typed field.
func FieldInput(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeInput)
}

// FieldProcessor returns a field spec for a processor typed field.
func FieldProcessor(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeProcessor)
}

// FieldOutput returns a field spec for an output typed field.
func FieldOutput(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeOutput)
}

// FieldBuffer returns a field spec for a buffer typed field.
func FieldBuffer(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeBuffer)
}

// FieldCache returns a field spec for a cache typed field.
func FieldCache(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeCache)
}

// FieldRateLimit returns a field spec for a rate limit typed field.
func FieldRateLimit(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeRateLimit)
}

// FieldMetrics returns a field spec for a metrics typed field.
func FieldMetrics(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeMetrics)
}

// FieldTracer returns a field spec for a tracer typed field.
func FieldTracer(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeTracer)
}

// FieldScanner returns a field spec for a scanner typed field.
func FieldScanner(name, description string, examples ...any) FieldSpec {
	return newField(name, description, examples...).HasType(FieldTypeScanner)
}

func newField(name, description string, examples ...any) FieldSpec {
	return FieldSpec{
		Name:        name,
		Description: description,
		Kind:        KindScalar,
		Examples:    examples,
	}
}

// FieldComponent returns a field spec for a component.
func FieldComponent() FieldSpec {
	return FieldSpec{
		Kind: KindScalar,
	}
}

// CheckRequired returns true if this field, due to various factors, is a field
// that must be specified within a config. The factors at play are:
//
// - Whether the field has a default value
// - Whether the field was explicitly marked as optional
// - Whether the field is an object with children, none of which are required.
func (f FieldSpec) CheckRequired() bool {
	if f.IsOptional {
		return false
	}
	if f.Default != nil {
		return false
	}
	if len(f.Children) == 0 {
		return true
	}

	// If none of the children are required then this field is not required.
	for _, child := range f.Children {
		if child.CheckRequired() {
			return true
		}
	}
	return false
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
type FieldFilter func(spec FieldSpec, v any) bool

func (f FieldFilter) shouldDrop(spec FieldSpec, v any) bool {
	if f == nil {
		return false
	}
	return !f(spec, v)
}

// ShouldDropDeprecated returns a field filter that removes all deprecated
// fields when the boolean argument is true.
func ShouldDropDeprecated(b bool) FieldFilter {
	if !b {
		return nil
	}
	return func(spec FieldSpec, _ any) bool {
		return !spec.IsDeprecated
	}
}

// SetDefault attempts to override the current default value of a field,
// identified by a series of names used to walk the config spec in order to
// reach the intended field. This function currently does NOT support walking
// through arrays.
func (f FieldSpecs) SetDefault(v any, path ...string) {
	if len(path) == 0 {
		return
	}
	for i, child := range f {
		if child.Name != path[0] {
			continue
		}
		if len(path) > 1 {
			child.Children.SetDefault(v, path[1:]...)
		} else {
			f[i] = child.HasDefault(v)
		}
	}
}

//------------------------------------------------------------------------------

// LintConfig describes which rules apply when linting benthos configs, and also
// determines which component and bloblang environments are used.
type LintConfig struct {
	// Provides documentation for component implementations.
	DocsProvider Provider

	// Provides an isolated context for Bloblang parsing.
	BloblangEnv *bloblang.Environment

	// Reject any deprecated components or fields as linting errors.
	RejectDeprecated bool

	// Require labels for components.
	RequireLabels bool
}

// NewLintConfig creates a default linting config.
func NewLintConfig(prov Provider) LintConfig {
	return LintConfig{
		DocsProvider: prov,
		BloblangEnv:  bloblang.GlobalEnvironment().Deactivated(),
	}
}

// LintContext is provided to linting functions, and provides context about the
// wider configuration.
type LintContext struct {
	// A map of label names to the line they were defined at.
	labelsToLine map[string]int

	conf LintConfig
}

// NewLintContext creates a new linting context.
func NewLintContext(conf LintConfig) LintContext {
	return LintContext{
		labelsToLine: map[string]int{},
		conf:         conf,
	}
}

// LintFunc is a common linting function for field values.
type LintFunc func(ctx LintContext, line, col int, value any) []Lint

// LintLevel describes the severity level of a linting error.
type LintLevel int

// Lint levels.
const (
	LintError   LintLevel = iota
	LintWarning LintLevel = iota
)

// LintType is a discrete linting type.
type LintType int

const (
	// LintCustom means a custom linting rule failed.
	LintCustom LintType = iota

	// LintFailedRead means a configuration could not be read.
	LintFailedRead LintType = iota

	// LintMissingEnvVar means a configuration contained an environment variable
	// interpolation without a default and the variable was undefined.
	LintMissingEnvVar LintType = iota

	// LintInvalidOption means the field value was not one of the explicit list
	// of options.
	LintInvalidOption LintType = iota

	// LintBadLabel means the label contains invalid characters.
	LintBadLabel LintType = iota

	// LintMissingLabel means the label is missing when required.
	LintMissingLabel LintType = iota

	// LintDuplicateLabel means the label collides with another label.
	LintDuplicateLabel LintType = iota

	// LintBadBloblang means the field contains invalid Bloblang.
	LintBadBloblang LintType = iota

	// LintShouldOmit means the field should be omitted.
	LintShouldOmit LintType = iota

	// LintComponentMissing means a component value was expected but the type is
	// missing.
	LintComponentMissing LintType = iota

	// LintComponentNotFound means the specified component value is not
	// recognised.
	LintComponentNotFound LintType = iota

	// LintUnknown means the field is unknown.
	LintUnknown LintType = iota

	// LintMissing means a field was required but missing.
	LintMissing LintType = iota

	// LintExpectedArray means an array value was expected but something else
	// was provided.
	LintExpectedArray LintType = iota

	// LintExpectedObject means an object value was expected but something else
	// was provided.
	LintExpectedObject LintType = iota

	// LintExpectedScalar means a scalar value was expected but something else
	// was provided.
	LintExpectedScalar LintType = iota

	// LintDeprecated means a field is deprecated and should not be used.
	LintDeprecated LintType = iota
)

// Lint describes a single linting issue found with a Benthos config.
type Lint struct {
	Line   int
	Column int // Optional, set to 1 by default
	Level  LintLevel
	Type   LintType
	What   string
}

// NewLintError returns an error lint.
func NewLintError(line int, t LintType, err error) Lint {
	var inner Lint
	if errors.As(err, &inner) {
		return inner
	}
	return Lint{Line: line, Column: 1, Level: LintError, Type: t, What: err.Error()}
}

// NewLintWarning returns a warning lint.
func NewLintWarning(line int, t LintType, msg string) Lint {
	return Lint{Line: line, Column: 1, Level: LintWarning, Type: t, What: msg}
}

// Error returns a formatted string explaining the lint error prefixed with its
// location within the file.
func (l Lint) Error() string {
	return fmt.Sprintf("(%v,%v) %v", l.Line, l.Column, l.What)
}

//------------------------------------------------------------------------------

func (f FieldSpec) needsDefault() bool {
	if f.IsOptional {
		return false
	}
	if f.IsDeprecated {
		return false
	}
	return true
}

func getDefault(pathName string, field FieldSpec) (any, error) {
	if field.Default != nil {
		if len(field.Children) > 0 && field.Kind == KindScalar {
			if tmp, ok := value.IClone(*field.Default).(map[string]any); ok {
				for _, v := range field.Children {
					if _, exists := tmp[v.Name]; exists {
						continue
					}
					defV, err := getDefault(pathName+"."+v.Name, v)
					if err == nil {
						tmp[v.Name] = defV
					} else if v.needsDefault() {
						return nil, err
					}
				}
				return tmp, nil
			}
		}
		return *field.Default, nil
	} else if field.Kind == KindArray {
		return []any{}, nil
	} else if field.Kind == Kind2DArray {
		return []any{}, nil
	} else if field.Kind == KindMap {
		return map[string]any{}, nil
	} else if len(field.Children) > 0 {
		m := map[string]any{}
		for _, v := range field.Children {
			defV, err := getDefault(pathName+"."+v.Name, v)
			if err == nil {
				m[v.Name] = defV
			} else if v.needsDefault() {
				return nil, err
			}
		}
		return m, nil
	}
	return nil, fmt.Errorf("field '%v' is required and was not present in the config", pathName)
}
