package service

import (
	"fmt"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

// NewInterpolatedStringField defines a new config field that describes a
// dynamic string that supports Bloblang interpolation functions. It is then
// possible to extract an *InterpolatedString from the resulting parsed config
// with the method FieldInterpolatedString.
func NewInterpolatedStringField(name string) *ConfigField {
	tf := docs.FieldString(name, "").IsInterpolated()
	return &ConfigField{field: tf}
}

// NewInterpolatedStringMapField describes a new config field consisting of an
// object of arbitrary keys with interpolated string values. It is then
// possible to extract an *InterpolatedString from the resulting parsed config
// with the method FieldInterpolatedStringMap.
func NewInterpolatedStringMapField(name string) *ConfigField {
	tf := docs.FieldString(name, "").IsInterpolated().Map()
	return &ConfigField{field: tf}
}

// FieldInterpolatedString accesses a field from a parsed config that was
// defined with NewInterpolatedStringField and returns either an
// *InterpolatedString or an error if the string was invalid.
func (p *ParsedConfig) FieldInterpolatedString(path ...string) (*InterpolatedString, error) {
	v, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	str, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("expected field '%v' to be a string, got %T", strings.Join(path, "."), v)
	}

	e, err := p.mgr.BloblEnvironment().NewField(str)
	if err != nil {
		return nil, fmt.Errorf("failed to parse interpolated field '%v': %v", strings.Join(path, "."), err)
	}

	return &InterpolatedString{expr: e}, nil
}

// FieldInterpolatedStringMap accesses a field that is an object of arbitrary
// keys and interpolated string values from the parsed config by its name and
// returns the value.
//
// Returns an error if the field is not found, or is not an object of
// interpolated strings.
func (p *ParsedConfig) FieldInterpolatedStringMap(path ...string) (map[string]*InterpolatedString, error) {
	v, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.fullDotPath(path...))
	}
	iMap, ok := v.(map[string]interface{})
	if !ok {
		if sMap, ok := v.(map[string]string); ok {
			iMap := make(map[string]*InterpolatedString, len(sMap))
			for k, sv := range sMap {
				e, err := p.mgr.BloblEnvironment().NewField(sv)
				if err != nil {
					return nil, fmt.Errorf("failed to parse interpolated field '%v': %v", strings.Join(path, "."), err)
				}
				iMap[k] = &InterpolatedString{expr: e}
			}
			return iMap, nil
		}
		return nil, fmt.Errorf("expected field '%v' to be a string map, got %T", p.fullDotPath(path...), v)
	}
	sMap := make(map[string]*InterpolatedString, len(iMap))
	for k, ev := range iMap {
		str, ok := ev.(string)
		if !ok {
			return nil, fmt.Errorf("expected field '%v' to be a string map, found an element of type %T", p.fullDotPath(path...), ev)
		}
		e, err := p.mgr.BloblEnvironment().NewField(str)
		if err != nil {
			return nil, fmt.Errorf("failed to parse interpolated field '%v': %v", strings.Join(path, "."), err)
		}
		sMap[k] = &InterpolatedString{expr: e}
	}
	return sMap, nil
}
