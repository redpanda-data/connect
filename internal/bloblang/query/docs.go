package query

// ExampleSpec provides a mapping example and some input/output results to
// display.
type ExampleSpec struct {
	Mapping     string      `json:"mapping"`
	Summary     string      `json:"summary"`
	Results     [][2]string `json:"results"`
	SkipTesting bool        `json:"skip_testing"`
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

// NewNotTestedExampleSpec creates a new not tested example spec.
func NewNotTestedExampleSpec(summary, mapping string, results ...string) ExampleSpec {
	structuredResults := make([][2]string, 0, len(results)/2)
	for i, res := range results {
		if i%2 == 1 {
			structuredResults = append(structuredResults, [2]string{results[i-1], res})
		}
	}
	return ExampleSpec{
		Mapping:     mapping,
		Summary:     summary,
		Results:     structuredResults,
		SkipTesting: true,
	}
}

//------------------------------------------------------------------------------

// Status of a function or method.
type Status string

// Component statuses.
var (
	StatusStable       Status = "stable"
	StatusBeta         Status = "beta"
	StatusExperimental Status = "experimental"
	StatusDeprecated   Status = "deprecated"
	StatusHidden       Status = "hidden"
)

//------------------------------------------------------------------------------

// Function categories.
var (
	FunctionCategoryGeneral     = "General"
	FunctionCategoryMessage     = "Message Info"
	FunctionCategoryEnvironment = "Environment"
	FunctionCategoryDeprecated  = "Deprecated"
	FunctionCategoryPlugin      = "Plugin"
	FunctionCategoryFakeData    = "Fake Data Generation"
)

// FunctionSpec describes a Bloblang function.
type FunctionSpec struct {
	// The release status of the function.
	Status Status `json:"status"`

	// A category to place the function within.
	Category string `json:"category"`

	// Name of the function (as it appears in config).
	Name string `json:"name"`

	// Description of the functions purpose (in markdown).
	Description string `json:"description,omitempty"`

	// Params defines the expected arguments of the function.
	Params Params `json:"params"`

	// Examples shows general usage for the function.
	Examples []ExampleSpec `json:"examples,omitempty"`

	// Impure indicates that a function accesses or interacts with the outer
	// environment, and is therefore unsafe to execute in shared environments.
	Impure bool `json:"impure"`

	// Version is the Benthos version this component was introduced.
	Version string `json:"version,omitempty"`
}

// NewFunctionSpec creates a new function spec.
func NewFunctionSpec(category, name, description string, examples ...ExampleSpec) FunctionSpec {
	return FunctionSpec{
		Status:      StatusStable,
		Category:    category,
		Name:        name,
		Description: description,
		Examples:    examples,
		Params:      NewParams(),
	}
}

// Experimental flags the function as an experimental component.
func (s FunctionSpec) Experimental() FunctionSpec {
	s.Status = StatusExperimental
	return s
}

// Beta flags the function as a beta component.
func (s FunctionSpec) Beta() FunctionSpec {
	s.Status = StatusBeta
	return s
}

// AtVersion sets the Benthos version this component was introduced.
func (s FunctionSpec) AtVersion(v string) FunctionSpec {
	s.Version = v
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

// Method categories.
var (
	MethodCategoryStrings        = "String Manipulation"
	MethodCategoryNumbers        = "Number Manipulation"
	MethodCategoryTime           = "Timestamp Manipulation"
	MethodCategoryRegexp         = "Regular Expressions"
	MethodCategoryEncoding       = "Encoding and Encryption"
	MethodCategoryCoercion       = "Type Coercion"
	MethodCategoryParsing        = "Parsing"
	MethodCategoryObjectAndArray = "Object & Array Manipulation"
	MethodCategoryJWT            = "JSON Web Tokens"
	MethodCategoryGeoIP          = "GeoIP"
	MethodCategoryDeprecated     = "Deprecated"
	MethodCategoryPlugin         = "Plugin"
)

// MethodCatSpec describes how a method behaves in the context of a given
// category.
type MethodCatSpec struct {
	Category    string
	Description string
	Examples    []ExampleSpec
}

// MethodSpec describes a Bloblang method.
type MethodSpec struct {
	// The release status of the function.
	Status Status `json:"status"`

	// Name of the method (as it appears in config).
	Name string `json:"name"`

	// Description of the method purpose (in markdown).
	Description string `json:"description,omitempty"`

	// Params defines the expected arguments of the method.
	Params Params `json:"params"`

	// Examples shows general usage for the method.
	Examples []ExampleSpec `json:"examples,omitempty"`

	// Categories that this method fits within.
	Categories []MethodCatSpec `json:"categories,omitempty"`

	// Impure indicates that a method accesses or interacts with the outer
	// environment, and is therefore unsafe to execute in shared environments.
	Impure bool `json:"impure"`

	// Version is the Benthos version this component was introduced.
	Version string `json:"version,omitempty"`
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

// Experimental flags the method as an experimental component.
func (m MethodSpec) Experimental() MethodSpec {
	m.Status = StatusExperimental
	return m
}

// Beta flags the function as a beta component.
func (m MethodSpec) Beta() MethodSpec {
	m.Status = StatusBeta
	return m
}

// AtVersion sets the Benthos version this component was introduced.
func (m MethodSpec) AtVersion(v string) MethodSpec {
	m.Version = v
	return m
}

// MarkImpure flags the method as being impure, meaning it access or interacts
// with the environment.
func (m MethodSpec) MarkImpure() MethodSpec {
	m.Impure = true
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
func (m MethodSpec) InCategory(category, description string, examples ...ExampleSpec) MethodSpec {
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
