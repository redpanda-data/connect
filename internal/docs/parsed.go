package docs

import (
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/gabs/v2"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/value"
)

func (f FieldSpec) ParsedConfigFromAny(v any) (pConf *ParsedConfig, err error) {
	pConf = &ParsedConfig{}
	switch t := v.(type) {
	case *yaml.Node:
		pConf.line = &t.Line
		pConf.generic, err = f.YAMLToValue(t, ToValueConfig{})
	default:
		pConf.generic, err = f.AnyToValue(v, ToValueConfig{})
	}
	return
}

func (f FieldSpecs) ParsedConfigFromAny(v any) (pConf *ParsedConfig, err error) {
	pConf = &ParsedConfig{}
	switch t := v.(type) {
	case *yaml.Node:
		pConf.line = &t.Line
		pConf.generic, err = f.YAMLToMap(t, ToValueConfig{})
	default:
		pConf.generic, err = f.AnyToMap(v, ToValueConfig{})
	}
	return
}

// ParsedConfig represents a plugin configuration that has been validated and
// parsed from a ConfigSpec, and allows plugin constructors to access
// configuration fields.
type ParsedConfig struct {
	hiddenPath []string
	generic    any
	line       *int
}

func (p *ParsedConfig) Raw() any {
	return p.generic
}

func (p *ParsedConfig) Line() (int, bool) {
	if p.line == nil {
		return 0, false
	}
	return *p.line, true
}

// Namespace returns a version of the parsed config at a given field namespace.
// This is useful for extracting multiple fields under the same grouping.
func (p *ParsedConfig) Namespace(path ...string) *ParsedConfig {
	tmpConfig := *p
	tmpConfig.hiddenPath = append([]string{}, p.hiddenPath...)
	tmpConfig.hiddenPath = append(tmpConfig.hiddenPath, path...)
	return &tmpConfig
}

// Field accesses a Field from the parsed config by its name and returns the
// value if the Field is found and a boolean indicating whether it was found.
// Nested fields can be accessed by specifying the series of Field names.
func (p *ParsedConfig) Field(path ...string) (any, bool) {
	gObj := gabs.Wrap(p.generic).S(p.hiddenPath...)
	if exists := gObj.Exists(path...); !exists {
		return nil, false
	}
	return gObj.S(path...).Data(), true
}

func (p *ParsedConfig) FullDotPath(path ...string) string {
	var fullPath []string
	fullPath = append(fullPath, p.hiddenPath...)
	fullPath = append(fullPath, path...)
	return strings.Join(fullPath, ".")
}

// Contains checks whether the parsed config contains a given field identified
// by its name.
func (p *ParsedConfig) Contains(path ...string) bool {
	gObj := gabs.Wrap(p.generic).S(p.hiddenPath...)
	return gObj.Exists(path...)
}

// FieldAny accesses a field from the parsed config by its name that can assume
// any value type. If the field is not found an error is returned.
func (p *ParsedConfig) FieldAny(path ...string) (any, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	return v, nil
}

// FieldAnyList accesses a field that is a list of any value types from the
// parsed config by its name and returns the value as an array of *ParsedConfig
// types, where each one represents an object or value in the list. Returns an
// error if the field is not found, or is not a list of values.
func (p *ParsedConfig) FieldAnyList(path ...string) ([]*ParsedConfig, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iList, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected field '%v' to be a list, got %T", p.FullDotPath(path...), v)
	}
	sList := make([]*ParsedConfig, len(iList))
	for i, ev := range iList {
		sList[i] = &ParsedConfig{
			generic: ev,
		}
	}
	return sList, nil
}

// FieldAnyMap accesses a field that is an object of arbitrary keys and any
// values from the parsed config by its name and returns a map of *ParsedConfig
// types, where each one represents an object or value in the map. Returns an
// error if the field is not found, or is not an object.
func (p *ParsedConfig) FieldAnyMap(path ...string) (map[string]*ParsedConfig, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iMap, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected field '%v' to be a string map, got %T", p.FullDotPath(path...), v)
	}
	sMap := make(map[string]*ParsedConfig, len(iMap))
	for k, v := range iMap {
		sMap[k] = &ParsedConfig{
			generic: v,
		}
	}
	return sMap, nil
}

// FieldString accesses a string field from the parsed config by its name. If
// the field is not found or is not a string an error is returned.
func (p *ParsedConfig) FieldString(path ...string) (string, error) {
	v, exists := p.Field(path...)
	if !exists {
		return "", fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	str, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("expected field '%v' to be a string, got %T", p.FullDotPath(path...), v)
	}
	return str, nil
}

// FieldDuration accesses a duration string field from the parsed config by its
// name. If the field is not found or is not a valid duration string an error is
// returned.
func (p *ParsedConfig) FieldDuration(path ...string) (time.Duration, error) {
	v, exists := p.Field(path...)
	if !exists {
		return 0, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	str, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("expected field '%v' to be a string, got %T", p.FullDotPath(path...), v)
	}
	d, err := time.ParseDuration(str)
	if err != nil {
		return 0, fmt.Errorf("failed to parse '%v' as a duration string: %w", p.FullDotPath(path...), err)
	}
	return d, nil
}

// FieldStringList accesses a field that is a list of strings from the parsed
// config by its name and returns the value. Returns an error if the field is
// not found, or is not a list of strings.
func (p *ParsedConfig) FieldStringList(path ...string) ([]string, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iList, ok := v.([]any)
	if !ok {
		if sList, ok := v.([]string); ok {
			return sList, nil
		}
		return nil, fmt.Errorf("expected field '%v' to be a string list, got %T", p.FullDotPath(path...), v)
	}
	sList := make([]string, len(iList))
	for i, ev := range iList {
		if sList[i], ok = ev.(string); !ok {
			return nil, fmt.Errorf("expected field '%v' to be a string list, found an element of type %T", p.FullDotPath(path...), ev)
		}
	}
	return sList, nil
}

// FieldStringListOfLists accesses a field that is a list of lists of strings
// from the parsed config by its name and returns the value. Returns an error if
// the field is not found, or is not a list of lists of strings.
func (p *ParsedConfig) FieldStringListOfLists(path ...string) ([][]string, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iList, ok := v.([]any)
	if !ok {
		if sList, ok := v.([][]string); ok {
			return sList, nil
		}
		return nil, fmt.Errorf("expected field '%v' to be a list of string lists, got %T", p.FullDotPath(path...), v)
	}
	sList := make([][]string, len(iList))
	for i, ev := range iList {
		switch t := ev.(type) {
		case []string:
			sList[i] = t
		case []any:
			tmpList := make([]string, len(t))
			for j, evv := range t {
				if tmpList[j], ok = evv.(string); !ok {
					return nil, fmt.Errorf("expected field '%v' to be a string list, found an element of type %T", p.FullDotPath(path...), evv)
				}
			}
			sList[i] = tmpList
		}
	}
	return sList, nil
}

// FieldStringMap accesses a field that is an object of arbitrary keys and
// string values from the parsed config by its name and returns the value.
// Returns an error if the field is not found, or is not an object of strings.
func (p *ParsedConfig) FieldStringMap(path ...string) (map[string]string, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iMap, ok := v.(map[string]any)
	if !ok {
		if sMap, ok := v.(map[string]string); ok {
			return sMap, nil
		}
		return nil, fmt.Errorf("expected field '%v' to be a string map, got %T", p.FullDotPath(path...), v)
	}
	sMap := make(map[string]string, len(iMap))
	for k, ev := range iMap {
		if sMap[k], ok = ev.(string); !ok {
			return nil, fmt.Errorf("expected field '%v' to be a string map, found an element of type %T", p.FullDotPath(path...), ev)
		}
	}
	return sMap, nil
}

// FieldInt accesses an int field from the parsed config by its name and returns
// the value. Returns an error if the field is not found or is not an int.
func (p *ParsedConfig) FieldInt(path ...string) (int, error) {
	v, exists := p.Field(path...)
	if !exists {
		return 0, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	i, err := value.IGetInt(v)
	if err != nil {
		return 0, fmt.Errorf("expected field '%v' to be an int, got %T", p.FullDotPath(path...), v)
	}
	return int(i), nil
}

// FieldIntList accesses a field that is a list of integers from the parsed
// config by its name and returns the value. Returns an error if the field is
// not found, or is not a list of integers.
func (p *ParsedConfig) FieldIntList(path ...string) ([]int, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iList, ok := v.([]any)
	if !ok {
		if sList, ok := v.([]int); ok {
			return sList, nil
		}
		return nil, fmt.Errorf("expected field '%v' to be an integer list, got %T", p.FullDotPath(path...), v)
	}
	sList := make([]int, len(iList))
	for i, ev := range iList {
		iv, err := value.IToInt(ev)
		if err != nil {
			return nil, fmt.Errorf("expected field '%v' to be an integer list, found an element of type %T", p.FullDotPath(path...), ev)
		}
		sList[i] = int(iv)
	}
	return sList, nil
}

// FieldIntMap accesses a field that is an object of arbitrary keys and
// integer values from the parsed config by its name and returns the value.
// Returns an error if the field is not found, or is not an object of integers.
func (p *ParsedConfig) FieldIntMap(path ...string) (map[string]int, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iMap, ok := v.(map[string]any)
	if !ok {
		if sMap, ok := v.(map[string]int); ok {
			return sMap, nil
		}
		return nil, fmt.Errorf("expected field '%v' to be an integer map, got %T", p.FullDotPath(path...), v)
	}
	sMap := make(map[string]int, len(iMap))
	for k, ev := range iMap {
		iv, err := value.IToInt(ev)
		if err != nil {
			return nil, fmt.Errorf("expected field '%v' to be an integer map, found an element of type %T", p.FullDotPath(path...), ev)
		}
		sMap[k] = int(iv)
	}
	return sMap, nil
}

// FieldFloat accesses a float field from the parsed config by its name and
// returns the value. Returns an error if the field is not found or is not a
// float.
func (p *ParsedConfig) FieldFloat(path ...string) (float64, error) {
	v, exists := p.Field(path...)
	if !exists {
		return 0, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	f, err := value.IGetNumber(v)
	if err != nil {
		return 0, fmt.Errorf("expected field '%v' to be a float, got %T", p.FullDotPath(path...), v)
	}
	return f, nil
}

// FieldFloatList accesses a field that is a list of floats from the parsed
// config by its name and returns the value. Returns an error if the field is
// not found, or is not a list of floats.
func (p *ParsedConfig) FieldFloatList(path ...string) ([]float64, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iList, ok := v.([]any)
	if !ok {
		if sList, ok := v.([]float64); ok {
			return sList, nil
		}
		return nil, fmt.Errorf("expected field '%v' to be an float list, got %T", p.FullDotPath(path...), v)
	}
	sList := make([]float64, len(iList))
	for i, ev := range iList {
		var err error
		if sList[i], err = value.IGetNumber(ev); err != nil {
			return nil, fmt.Errorf("expected field '%v' to be an float list, found an element of type %T", p.FullDotPath(path...), ev)
		}
	}
	return sList, nil
}

// FieldFloatMap accesses a field that is an object of arbitrary keys and
// float values from the parsed config by its name and returns the value.
// Returns an error if the field is not found, or is not an object of floats.
func (p *ParsedConfig) FieldFloatMap(path ...string) (map[string]float64, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iMap, ok := v.(map[string]any)
	if !ok {
		if sMap, ok := v.(map[string]float64); ok {
			return sMap, nil
		}
		return nil, fmt.Errorf("expected field '%v' to be an float map, got %T", p.FullDotPath(path...), v)
	}
	sMap := make(map[string]float64, len(iMap))
	for k, ev := range iMap {
		var err error
		if sMap[k], err = value.IGetNumber(ev); err != nil {
			return nil, fmt.Errorf("expected field '%v' to be an float map, found an element of type %T", p.FullDotPath(path...), ev)
		}
	}
	return sMap, nil
}

// FieldBool accesses a bool field from the parsed config by its name and
// returns the value. Returns an error if the field is not found or is not a
// bool.
func (p *ParsedConfig) FieldBool(path ...string) (bool, error) {
	v, e := p.Field(path...)
	if !e {
		return false, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("expected field '%v' to be a bool, got %T", p.FullDotPath(path...), v)
	}
	return b, nil
}

// FieldObjectList accesses a field that is a list of objects from the parsed
// config by its name and returns the value as an array of *ParsedConfig types,
// where each one represents an object in the list. Returns an error if the
// field is not found, or is not a list of objects.
func (p *ParsedConfig) FieldObjectList(path ...string) ([]*ParsedConfig, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iList, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected field '%v' to be a list, got %T", p.FullDotPath(path...), v)
	}
	sList := make([]*ParsedConfig, len(iList))
	for i, ev := range iList {
		sList[i] = &ParsedConfig{
			generic: ev,
		}
	}
	return sList, nil
}

// FieldObjectListOfLists accesses a field that is a list of lists of objects
// from the parsed config by its name and returns the value as an array of
// arrays of *ParsedConfig types, where each one represents an object in the
// list. Returns an error if the field is not found, or is not a list of lists
// of objects.
func (p *ParsedConfig) FieldObjectListOfLists(path ...string) ([][]*ParsedConfig, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iList, ok := v.([]any)
	if !ok {
		if sList, ok := v.([][]*ParsedConfig); ok {
			return sList, nil
		}
		return nil, fmt.Errorf("expected field '%v' to be a list of object lists, got %T", p.FullDotPath(path...), v)
	}
	sList := make([][]*ParsedConfig, len(iList))
	for i, ev := range iList {
		switch t := ev.(type) {
		case []*ParsedConfig:
			sList[i] = t
		case []any:
			tmpList := make([]*ParsedConfig, len(t))
			for j, evv := range t {
				tmpList[j] = &ParsedConfig{
					generic: evv,
				}
			}
			sList[i] = tmpList
		}
	}
	return sList, nil
}

// FieldObjectMap accesses a field that is a map of objects from the parsed
// config by its name and returns the value as a map of *ParsedConfig types,
// where each one represents an object in the map. Returns an error if the
// field is not found, or is not a map of objects.
func (p *ParsedConfig) FieldObjectMap(path ...string) (map[string]*ParsedConfig, error) {
	v, exists := p.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.FullDotPath(path...))
	}
	iMap, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected field '%v' to be a map, got %T", p.FullDotPath(path...), v)
	}
	sMap := make(map[string]*ParsedConfig, len(iMap))
	for i, ev := range iMap {
		sMap[i] = &ParsedConfig{
			generic: ev,
		}
	}
	return sMap, nil
}
