package service

import (
	"fmt"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

// NewBloblangField defines a new config field that describes a Bloblang mapping
// string. It is then possible to extract a *bloblang.Executor from the
// resulting parsed config with the method FieldBloblang.
func NewBloblangField(name string) *ConfigField {
	tf := docs.FieldBloblang(name, "")
	return &ConfigField{field: tf}
}

// FieldBloblang accesses a field from a parsed config that was defined with
// NewBloblangField and returns either a *bloblang.Executor or an error if the
// mapping was invalid.
func (p *ParsedConfig) FieldBloblang(path ...string) (*bloblang.Executor, error) {
	v, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	str, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("expected field '%v' to be a string, got %T", strings.Join(path, "."), v)
	}

	exec, err := bloblang.XWrapEnvironment(p.mgr.BloblEnvironment()).Parse(str)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bloblang mapping '%v': %v", strings.Join(path, "."), err)
	}
	return exec, nil
}
