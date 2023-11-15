package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ParamDefinition describes a single parameter for a function or method.
type ParamDefinition struct {
	Name             string    `json:"name"`
	Description      string    `json:"description,omitempty"`
	ValueType        ValueType `json:"type"`
	NoDynamic        bool      `json:"no_dynamic"`
	ScalarsToLiteral bool      `json:"scalars_to_literal"`

	// IsOptional is implicit when there's a DefaultValue. However, there are
	// times when a parameter is used to change behaviour without having a
	// default.
	IsOptional   bool `json:"is_optional,omitempty"`
	DefaultValue *any `json:"default,omitempty"`
}

func (d ParamDefinition) validate() error {
	if !nameRegexp.MatchString(d.Name) {
		return fmt.Errorf("parameter name '%v' does not match the required regular expression /%v/", d.Name, nameRegexpRaw)
	}
	return nil
}

// ParamString creates a new string typed parameter.
func ParamString(name, description string) ParamDefinition {
	return ParamDefinition{
		Name: name, Description: description,
		ValueType: ValueString,
	}
}

// ParamTimestamp creates a new timestamp typed parameter.
func ParamTimestamp(name, description string) ParamDefinition {
	return ParamDefinition{
		Name: name, Description: description,
		ValueType: ValueTimestamp,
	}
}

// ParamInt64 creates a new integer typed parameter.
func ParamInt64(name, description string) ParamDefinition {
	return ParamDefinition{
		Name: name, Description: description,
		ValueType: ValueInt,
	}
}

// ParamFloat creates a new float typed parameter.
func ParamFloat(name, description string) ParamDefinition {
	return ParamDefinition{
		Name: name, Description: description,
		ValueType: ValueFloat,
	}
}

// ParamBool creates a new bool typed parameter.
func ParamBool(name, description string) ParamDefinition {
	return ParamDefinition{
		Name: name, Description: description,
		ValueType: ValueBool,
	}
}

// ParamArray creates a new array typed parameter.
func ParamArray(name, description string) ParamDefinition {
	return ParamDefinition{
		Name: name, Description: description,
		ValueType: ValueArray,
	}
}

// ParamObject creates a new object typed parameter.
func ParamObject(name, description string) ParamDefinition {
	return ParamDefinition{
		Name: name, Description: description,
		ValueType: ValueObject,
	}
}

// ParamQuery creates a new query typed parameter. The field wrapScalars
// determines whether non-query arguments are allowed, in which case they will
// be converted into literal functions.
func ParamQuery(name, description string, wrapScalars bool) ParamDefinition {
	return ParamDefinition{
		Name: name, Description: description,
		ValueType:        ValueQuery,
		ScalarsToLiteral: wrapScalars,
	}
}

// ParamAny creates a new parameter that can be any type (excluding query).
func ParamAny(name, description string) ParamDefinition {
	return ParamDefinition{
		Name: name, Description: description,
		ValueType: ValueUnknown,
	}
}

// NoDynamic disables any form of dynamic assignment for this parameter. This is
// quite limiting (prevents variables from being used, etc) and so should only
// be used with caution.
func (d ParamDefinition) DisableDynamic() ParamDefinition {
	d.NoDynamic = true
	return d
}

// Optional marks the parameter as optional.
func (d ParamDefinition) Optional() ParamDefinition {
	d.IsOptional = true
	return d
}

// Default adds a default value to a parameter, also making it implicitly
// optional.
func (d ParamDefinition) Default(v any) ParamDefinition {
	d.DefaultValue = &v
	return d
}

// PrettyDefault returns a marshalled version of the parameters default value,
// or an empty string if there isn't one.
func (d ParamDefinition) PrettyDefault() string {
	if d.DefaultValue == nil {
		return ""
	}
	b, err := json.Marshal(*d.DefaultValue)
	if err != nil {
		return ""
	}
	return string(b)
}

func (d ParamDefinition) parseArgValue(v any) (any, error) {
	switch d.ValueType {
	case ValueInt:
		return IGetInt(v)
	case ValueFloat, ValueNumber:
		return IGetNumber(v)
	case ValueString:
		switch t := v.(type) {
		case string:
			return t, nil
		case []byte:
			return string(t), nil
		}
	case ValueTimestamp:
		return IGetTimestamp(v)
	case ValueBool:
		return IGetBool(v)
	case ValueArray:
		if _, isArray := v.([]any); isArray {
			return v, nil
		}
	case ValueObject:
		if _, isObj := v.(map[string]any); isObj {
			return v, nil
		}
	case ValueQuery:
		if _, isDyn := v.(Function); isDyn {
			return v, nil
		}
		if d.ScalarsToLiteral {
			return NewLiteralFunction("", v), nil
		}
	case ValueUnknown:
		return v, nil
	}
	return nil, fmt.Errorf("wrong argument type, expected %v, got %v", d.ValueType, ITypeOf(v))
}

//------------------------------------------------------------------------------

// Params defines the expected arguments of a function or method.
type Params struct {
	Variadic    bool              `json:"variadic,omitempty"`
	Definitions []ParamDefinition `json:"named,omitempty"`

	// Used by parsed param frames, we instantiate this here so that it's
	// allocated only once at parse time rather than execution time.
	nameToIndex map[string]int
}

// NewParams creates a new empty parameters definition.
func NewParams() Params {
	return Params{
		nameToIndex: map[string]int{},
	}
}

// VariadicParams creates a new empty parameters definition where any number of
// nameless arguments are considered valid.
func VariadicParams() Params {
	return Params{
		Variadic:    true,
		nameToIndex: map[string]int{},
	}
}

// Add a parameter to the spec.
func (p Params) Add(def ParamDefinition) Params {
	p.Definitions = append(p.Definitions, def)
	p.nameToIndex[def.Name] = len(p.Definitions) - 1
	return p
}

// PopulateNameless returns a set of populated arguments from a list of nameless
// parameters.
func (p Params) PopulateNameless(args ...any) (*ParsedParams, error) {
	procParams, err := p.processNameless(args)
	if err != nil {
		return nil, err
	}

	dynArgs, err := p.gatherDynamicArgs(procParams)
	if err != nil {
		return nil, err
	}
	return &ParsedParams{
		source:  p,
		dynArgs: dynArgs,
		values:  procParams,
	}, nil
}

// PopulateNamed returns a set of populated arguments from a map of named
// parameters.
func (p Params) PopulateNamed(args map[string]any) (*ParsedParams, error) {
	procParams, err := p.processNamed(args)
	if err != nil {
		return nil, err
	}

	dynArgs, err := p.gatherDynamicArgs(procParams)
	if err != nil {
		return nil, err
	}
	return &ParsedParams{
		source:  p,
		dynArgs: dynArgs,
		values:  procParams,
	}, nil
}

func (p Params) validate() error {
	if p.Variadic && len(p.Definitions) > 0 {
		return errors.New("cannot add named parameters to a variadic parameter definition")
	}

	seen := map[string]struct{}{}
	for _, param := range p.Definitions {
		if err := param.validate(); err != nil {
			return err
		}
		if _, exists := seen[param.Name]; exists {
			return fmt.Errorf("duplicate parameter name: %v", param.Name)
		}
		seen[param.Name] = struct{}{}
	}

	return nil
}

type dynamicArgIndex struct {
	index int
	fn    Function
}

func (p Params) gatherDynamicArgs(args []any) (dynArgs []dynamicArgIndex, err error) {
	if p.Variadic {
		for i, arg := range args {
			if fn, isFn := arg.(Function); isFn {
				dynArgs = append(dynArgs, dynamicArgIndex{index: i, fn: fn})
			}
		}
		return
	}
	for i, param := range p.Definitions {
		if param.ValueType == ValueQuery {
			continue
		}
		if fn, isFn := args[i].(Function); isFn {
			if param.NoDynamic {
				err = fmt.Errorf("param %v must not be dynamic", param.Name)
				return
			}
			dynArgs = append(dynArgs, dynamicArgIndex{index: i, fn: fn})
		}
	}
	return
}

func expandLiteralArgs(args []any) {
	for i, dArg := range args {
		if lit, isLit := dArg.(*Literal); isLit {
			args[i] = lit.Value
		}
	}
}

// processNameless attempts to validate a list of unnamed arguments, and
// populates elements with default values if they are omitted.
func (p Params) processNameless(args []any) ([]any, error) {
	if p.Variadic {
		expandLiteralArgs(args)
		return args, nil
	}

	if len(args) > len(p.Definitions) {
		return nil, fmt.Errorf("wrong number of arguments, expected %v, got %v", len(p.Definitions), len(args))
	}

	newArgs := args
	if len(newArgs) != len(p.Definitions) {
		newArgs = make([]any, len(p.Definitions))
		copy(newArgs, args)
	}

	var missingParams []string
	for i, param := range p.Definitions {
		var v any
		if len(args) > i {
			v = newArgs[i]
		} else if param.DefaultValue != nil {
			v = *param.DefaultValue
		} else if param.IsOptional {
			continue
		} else {
			missingParams = append(missingParams, param.Name)
			continue
		}

		if lit, isLit := v.(*Literal); isLit && param.ValueType != ValueQuery {
			// Literal functions are expanded automatically when the parameter
			// type is not a dynamic query.
			v = lit.Value
		}
		if _, isDyn := v.(Function); isDyn {
			// Dynamic arguments pass through as these need to be dealt with
			// during execution.
			newArgs[i] = v
			continue
		}

		var err error
		if newArgs[i], err = param.parseArgValue(v); err != nil {
			return nil, fmt.Errorf("field %v: %w", param.Name, err)
		}
	}

	if len(missingParams) == 1 {
		return nil, fmt.Errorf("missing parameter: %v", missingParams[0])
	} else if len(missingParams) > 1 {
		return nil, fmt.Errorf("missing parameters: %v", strings.Join(missingParams, ", "))
	}
	return newArgs, nil
}

// processNamed attempts to validate a map of named arguments, and populates
// elements with default values if they are omitted.
func (p Params) processNamed(args map[string]any) ([]any, error) {
	if p.Variadic {
		return nil, errors.New("named arguments are not supported")
	}

	newArgs := make([]any, len(p.Definitions))

	var missingParams []string
	for i, param := range p.Definitions {
		v, exists := args[param.Name]
		if !exists {
			if param.IsOptional {
				continue
			}
			if param.DefaultValue == nil {
				missingParams = append(missingParams, param.Name)
				continue
			}
			v = *param.DefaultValue
		}

		// Remove found parameter names, any left over are unexpected.
		delete(args, param.Name)

		if lit, isLit := v.(*Literal); isLit && param.ValueType != ValueQuery {
			// Literal functions are expanded automatically when the parameter
			// type is not a dynamic query.
			v = lit.Value
		}
		if _, isDyn := v.(Function); isDyn {
			// Dynamic arguments pass through as these need to be dealt with
			// during execution.
			newArgs[i] = v
			continue
		}

		var err error
		if newArgs[i], err = param.parseArgValue(v); err != nil {
			return nil, fmt.Errorf("field %v: %w", param.Name, err)
		}
	}

	if len(args) > 0 {
		var unexpected []string
		for k := range args {
			unexpected = append(unexpected, k)
		}
		sort.Strings(unexpected)

		optionsStr := ""
		if len(missingParams) == 1 {
			optionsStr = fmt.Sprintf(", did you mean %v?", missingParams[0])
		} else if len(missingParams) > 1 {
			optionsStr = fmt.Sprintf(", expected %v", strings.Join(missingParams, ", "))
		}

		if len(unexpected) == 1 {
			return nil, fmt.Errorf("unknown parameter %v%v", unexpected[0], optionsStr)
		}
		return nil, fmt.Errorf("unknown parameters %v%v", strings.Join(unexpected, ", "), optionsStr)
	}

	if len(missingParams) == 1 {
		return nil, fmt.Errorf("missing parameter: %v", missingParams[0])
	} else if len(missingParams) > 1 {
		return nil, fmt.Errorf("missing parameters: %v", strings.Join(missingParams, ", "))
	}
	return newArgs, nil
}

//------------------------------------------------------------------------------

// ParsedParams is a reference to the arguments of a method or function
// instantiation.
type ParsedParams struct {
	source  Params
	dynArgs []dynamicArgIndex
	values  []any
}

// dynamic returns any argument functions that must be evaluated at query time.
// The purpose of this method is to use the list to extract function targets and
// other info, use ResolveDynamic for populating these values with a function
// context.
func (p *ParsedParams) dynamic() []Function {
	if p == nil || len(p.dynArgs) == 0 {
		return nil
	}
	fns := make([]Function, len(p.dynArgs))
	for i, v := range p.dynArgs {
		fns[i] = v.fn
	}
	return fns
}

// ResolveDynamic attempts to execute all dynamic arguments with a given context
// and populate a new parsed parameters set with the values, ready to be used in
// a function or method.
func (p *ParsedParams) ResolveDynamic(ctx FunctionContext) (*ParsedParams, error) {
	if len(p.dynArgs) == 0 {
		return p, nil
	}
	newValues := make([]any, len(p.values))
	copy(newValues, p.values)
	for i, dyn := range p.dynArgs {
		var sourceDef ParamDefinition
		if len(p.source.Definitions) > dyn.index {
			sourceDef = p.source.Definitions[dyn.index]
		} else {
			// Must be variadic arguments.
			sourceDef = ParamAny(strconv.Itoa(i), "")
		}
		tmpValue, err := dyn.fn.Exec(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to extract input arg '%v': %w", sourceDef.Name, err)
		}
		if newValues[dyn.index], err = sourceDef.parseArgValue(tmpValue); err != nil {
			return nil, fmt.Errorf("failed to extract input arg '%v': %w", sourceDef.Name, err)
		}
	}
	return &ParsedParams{
		source: p.source,
		values: newValues,
	}, nil
}

// Raw returns the arguments as a generic slice.
func (p *ParsedParams) Raw() []any {
	if p == nil {
		return nil
	}
	return p.values
}

// Index returns an argument value at a given index.
func (p *ParsedParams) Index(i int) (any, error) {
	if i < 0 || len(p.values) <= i {
		return nil, fmt.Errorf("parameter index %v out of bounds", i)
	}
	return p.values[i], nil
}

// Field returns an argument value with a given name.
func (p *ParsedParams) Field(n string) (any, error) {
	index, ok := p.source.nameToIndex[n]
	if !ok {
		return nil, fmt.Errorf("parameter %v not found", n)
	}
	if index < 0 || len(p.values) <= index {
		return nil, fmt.Errorf("parameter index %v out of bounds", index)
	}
	return p.values[index], nil
}

// FieldArray returns an array value with a given name.
func (p *ParsedParams) FieldArray(n string) ([]any, error) {
	v, err := p.Field(n)
	if err != nil {
		return nil, err
	}
	a, ok := v.([]any)
	if !ok {
		return nil, NewTypeError(v, ValueArray)
	}
	return a, nil
}

// FieldOptionalArray returns an optional array value with a given name.
func (p *ParsedParams) FieldOptionalArray(n string) (*[]any, error) {
	v, err := p.Field(n)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	a, ok := v.([]any)
	if !ok {
		return nil, NewTypeError(v, ValueArray)
	}
	return &a, nil
}

// FieldString returns a string argument value with a given name.
func (p *ParsedParams) FieldString(n string) (string, error) {
	v, err := p.Field(n)
	if err != nil {
		return "", err
	}
	str, ok := v.(string)
	if !ok {
		return "", NewTypeError(v, ValueString)
	}
	return str, nil
}

// FieldOptionalString returns a string argument value with a given name if it
// was defined, otherwise nil.
func (p *ParsedParams) FieldOptionalString(n string) (*string, error) {
	v, err := p.Field(n)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	str, ok := v.(string)
	if !ok {
		return nil, NewTypeError(v, ValueString)
	}
	return &str, nil
}

// FieldTimestamp returns a timestamp argument value with a given name.
func (p *ParsedParams) FieldTimestamp(n string) (time.Time, error) {
	v, err := p.Field(n)
	if err != nil {
		return time.Time{}, err
	}
	t, ok := v.(time.Time)
	if !ok {
		return time.Time{}, NewTypeError(v, ValueTimestamp)
	}
	return t, nil
}

// FieldOptionalTimestamp returns a timestamp argument value with a given name
// if it was defined, otherwise nil.
func (p *ParsedParams) FieldOptionalTimestamp(n string) (*time.Time, error) {
	v, err := p.Field(n)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	t, ok := v.(time.Time)
	if !ok {
		return nil, NewTypeError(v, ValueTimestamp)
	}
	return &t, nil
}

// FieldInt64 returns an integer argument value with a given name.
func (p *ParsedParams) FieldInt64(n string) (int64, error) {
	v, err := p.Field(n)
	if err != nil {
		return 0, err
	}
	i, ok := v.(int64)
	if !ok {
		return 0, NewTypeError(v, ValueInt)
	}
	return i, nil
}

// FieldOptionalInt64 returns an int argument value with a given name if it was
// defined, otherwise nil.
func (p *ParsedParams) FieldOptionalInt64(n string) (*int64, error) {
	v, err := p.Field(n)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	i, ok := v.(int64)
	if !ok {
		return nil, NewTypeError(v, ValueInt)
	}
	return &i, nil
}

// FieldFloat returns a float argument value with a given name.
func (p *ParsedParams) FieldFloat(n string) (float64, error) {
	v, err := p.Field(n)
	if err != nil {
		return 0, err
	}
	f, ok := v.(float64)
	if !ok {
		return 0, NewTypeError(v, ValueFloat)
	}
	return f, nil
}

// FieldOptionalFloat returns a float argument value with a given name if it was
// defined, otherwise nil.
func (p *ParsedParams) FieldOptionalFloat(n string) (*float64, error) {
	v, err := p.Field(n)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	f, ok := v.(float64)
	if !ok {
		return nil, NewTypeError(v, ValueFloat)
	}
	return &f, nil
}

// FieldBool returns a bool argument value with a given name.
func (p *ParsedParams) FieldBool(n string) (bool, error) {
	v, err := p.Field(n)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, NewTypeError(v, ValueBool)
	}
	return b, nil
}

// FieldOptionalBool returns a bool argument value with a given name if it was
// defined, otherwise nil.
func (p *ParsedParams) FieldOptionalBool(n string) (*bool, error) {
	v, err := p.Field(n)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	b, ok := v.(bool)
	if !ok {
		return nil, NewTypeError(v, ValueBool)
	}
	return &b, nil
}

// FieldQuery returns a query argument value with a given name.
func (p *ParsedParams) FieldQuery(n string) (Function, error) {
	v, err := p.Field(n)
	if err != nil {
		return nil, err
	}
	f, ok := v.(Function)
	if !ok {
		return nil, NewTypeError(v, ValueQuery)
	}
	return f, nil
}

// FieldOptionalQuery returns a query argument value with a given name if it
// was defined, otherwise nil.
func (p *ParsedParams) FieldOptionalQuery(n string) (Function, error) {
	v, err := p.Field(n)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	f, ok := v.(Function)
	if !ok {
		return nil, NewTypeError(v, ValueQuery)
	}
	return f, nil
}
