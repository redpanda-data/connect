package service

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

// NewURLField defines a new config field that describes a string that should
// contain a valid URL. It is then possible to extract either a string or a
// *url.URL from the resulting parsed config with the methods FieldString or
// FieldURL respectively.
func NewURLField(name string) *ConfigField {
	tf := docs.FieldURL(name, "")
	return &ConfigField{field: tf}
}

// FieldURL accesses a field from a parsed config that was defined with
// NewURLField and returns either a *url.URL or an error if the string was
// invalid.
func (p *ParsedConfig) FieldURL(path ...string) (*url.URL, error) {
	v, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	str, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("expected field '%v' to be a string, got %T", strings.Join(path, "."), v)
	}

	u, err := url.Parse(str)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url field '%v': %v", strings.Join(path, "."), err)
	}

	return u, nil
}
