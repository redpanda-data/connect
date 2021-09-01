package query

// ExampleSpec provides a mapping example and some input/output results to
// display.
type ExampleSpec struct {
	Mapping string
	Summary string
	Results [][2]string
}

// NewExampleSpec creates a new example spec.
func NewExampleSpec(summary, mapping string, results ...string) ExampleSpec {
	structuredResults := make([][2]string, 0, len(results)/2)
	for i, res := range results {
		if i%2 == 1 {
			structuredResults = append(structuredResults, [2]string{results[i-1], res})
		}
	}
	return ExampleSpec{
		Mapping: mapping,
		Summary: summary,
		Results: structuredResults,
	}
}

//------------------------------------------------------------------------------

// Status of a function or method.
type Status string

// Component statuses.
var (
	StatusStable     Status = "stable"
	StatusBeta       Status = "beta"
	StatusDeprecated Status = "deprecated"
	StatusHidden     Status = "hidden"
)

//------------------------------------------------------------------------------

// FunctionCategory is an abstract title for functions of a similar purpose.
type FunctionCategory string

// Function categories.
var (
	FunctionCategoryGeneral     FunctionCategory = "General"
	FunctionCategoryMessage     FunctionCategory = "Message Info"
	FunctionCategoryEnvironment FunctionCategory = "Environment"
	FunctionCategoryDeprecated  FunctionCategory = "Deprecated"
	FunctionCategoryPlugin      FunctionCategory = "Plugin"
)

// FunctionSpec describes a Bloblang function.
type FunctionSpec struct {
	// The release status of the function.
	Status Status

	// A category to place the function within.
	Category FunctionCategory

	// Name of the function (as it appears in config).
	Name string

	// Description of the functions purpose (in markdown).
	Description string

	// Params defines the expected arguments of the function.
	Params Params

	// Examples shows general usage for the function.
	Examples []ExampleSpec

	// Impure indicates that a function accesses or interacts with the outter
	// environment, and is therefore unsafe to execute in shared environments.
	Impure bool
}

// NewFunctionSpec creates a new function spec.
func NewFunctionSpec(category FunctionCategory, name, description string, examples ...ExampleSpec) FunctionSpec {
	return FunctionSpec{
		Status:      StatusStable,
		Category:    category,
		Name:        name,
		Description: description,
		Examples:    examples,
		Params:      NewParams(),
	}
}

// Beta flags the function as a beta component.
func (s FunctionSpec) Beta() FunctionSpec {
	s.Status = StatusBeta
	return s
}

// MarkImpure flags the function as being impure, meaning it access or interacts
// with the environment.
func (s FunctionSpec) MarkImpure() FunctionSpec {
	s.Impure = true
	return s
}

// Param adds a parameter to the function.
func (s FunctionSpec) Param(def ParamDefinition) FunctionSpec {
	s.Params = s.Params.Add(def)
	return s
}

// NewDeprecatedFunctionSpec creates a new function spec that is deprecated.
func NewDeprecatedFunctionSpec(name, description string, examples ...ExampleSpec) FunctionSpec {
	return FunctionSpec{
		Status:      StatusDeprecated,
		Category:    FunctionCategoryDeprecated,
		Name:        name,
		Description: description,
		Examples:    examples,
		Params:      NewParams(),
	}
}

// NewHiddenFunctionSpec creates a new function spec that is hidden from the docs.
func NewHiddenFunctionSpec(name string) FunctionSpec {
	return FunctionSpec{
		Status: StatusHidden,
		Name:   name,
		Params: NewParams(),
	}
}

//------------------------------------------------------------------------------

// MethodCategory is an abstract title for methods of a similar purpose.
type MethodCategory string

// Method categories.
var (
	MethodCategoryStrings        MethodCategory = "String Manipulation"
	MethodCategoryNumbers        MethodCategory = "Number Manipulation"
	MethodCategoryTime           MethodCategory = "Timestamp Manipulation"
	MethodCategoryRegexp         MethodCategory = "Regular Expressions"
	MethodCategoryEncoding       MethodCategory = "Encoding and Encryption"
	MethodCategoryCoercion       MethodCategory = "Type Coercion"
	MethodCategoryParsing        MethodCategory = "Parsing"
	MethodCategoryObjectAndArray MethodCategory = "Object & Array Manipulation"
	MethodCategoryDeprecated     MethodCategory = "Deprecated"
	MethodCategoryPlugin         MethodCategory = "Plugin"
)

// MethodCatSpec describes how a method behaves in the context of a given
// category.
type MethodCatSpec struct {
	Category    MethodCategory
	Description string
	Examples    []ExampleSpec
}

// MethodSpec describes a Bloblang method.
type MethodSpec struct {
	// The release status of the function.
	Status Status

	// Name of the method (as it appears in config).
	Name string

	// Description of the method purpose (in markdown).
	Description string

	// Params defines the expected arguments of the method.
	Params Params

	// Examples shows general usage for the method.
	Examples []ExampleSpec

	// Categories that this method fits within.
	Categories []MethodCatSpec
}

// NewMethodSpec creates a new method spec.
func NewMethodSpec(name, description string, examples ...ExampleSpec) MethodSpec {
	return MethodSpec{
		Name:        name,
		Status:      StatusStable,
		Description: description,
		Examples:    examples,
		Params:      NewParams(),
	}
}

// NewDeprecatedMethodSpec creates a new method spec that is deprecated. The
// method will not appear in docs or searches but will still be usable in
// mappings.
func NewDeprecatedMethodSpec(name, description string, examples ...ExampleSpec) MethodSpec {
	return MethodSpec{
		Name:     name,
		Status:   StatusDeprecated,
		Examples: examples,
		Params:   NewParams(),
	}
}

// NewHiddenMethodSpec creates a new method spec that is hidden from docs.
func NewHiddenMethodSpec(name string) MethodSpec {
	return MethodSpec{
		Name:   name,
		Status: StatusHidden,
		Params: NewParams(),
	}
}

// Beta flags the function as a beta component.
func (m MethodSpec) Beta() MethodSpec {
	m.Status = StatusBeta
	return m
}

// Param adds a parameter to the function.
func (m MethodSpec) Param(def ParamDefinition) MethodSpec {
	m.Params = m.Params.Add(def)
	return m
}

// VariadicParams configures the method spec to allow variadic parameters.
func (m MethodSpec) VariadicParams() MethodSpec {
	m.Params = VariadicParams()
	return m
}

// InCategory describes the methods behaviour in the context of a given
// category, methods can belong to multiple categories. For example, the
// `contains` method behaves differently in the object and array category versus
// the strings one, but belongs in both.
func (m MethodSpec) InCategory(category MethodCategory, description string, examples ...ExampleSpec) MethodSpec {
	if m.Status == StatusDeprecated {
		category = MethodCategoryDeprecated
	}

	cats := make([]MethodCatSpec, 0, len(m.Categories)+1)
	cats = append(cats, m.Categories...)
	cats = append(cats, MethodCatSpec{
		Category:    category,
		Description: description,
		Examples:    examples,
	})
	m.Categories = cats
	return m
}
