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
	tf := docs.FieldCommon(name, "").HasType(docs.FieldTypeString).IsInterpolated()
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
