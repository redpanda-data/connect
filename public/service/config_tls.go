package service

import (
	"crypto/tls"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// NewTLSField defines a new object type config field that describes TLS
// settings for networked components. It is then possible to extract a
// *tls.Config from the resulting parsed config with the method FieldTLS.
func NewTLSField(name string) *ConfigField {
	tf := btls.FieldSpec()
	tf.Name = name
	var newChildren []docs.FieldSpec
	for _, f := range tf.Children {
		if f.Name != "enabled" {
			newChildren = append(newChildren, f)
		}
	}
	tf.Children = newChildren
	return &ConfigField{field: tf}
}

// FieldTLS accesses a field from a parsed config that was defined with
// NewTLSField and returns a *tls.Config, or an error if the configuration was
// invalid.
func (p *ParsedConfig) FieldTLS(path ...string) (*tls.Config, error) {
	v, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	var node yaml.Node
	if err := node.Encode(v); err != nil {
		return nil, err
	}

	conf := btls.NewConfig()
	if err := node.Decode(&conf); err != nil {
		return nil, err
	}

	return conf.GetNonToggled(p.mgr.FS())
}

// NewTLSToggledField defines a new object type config field that describes TLS
// settings for networked components. This field differs from a standard
// TLSField as it includes a boolean field `enabled` which allows users to
// explicitly configure whether TLS should be enabled or not.
//
// A *tls.Config as well as an enabled boolean value can be extracted from the
// resulting parsed config with the method FieldTLSToggled.
func NewTLSToggledField(name string) *ConfigField {
	tf := btls.FieldSpec()
	tf.Name = name
	return &ConfigField{field: tf}
}

// FieldTLSToggled accesses a field from a parsed config that was defined with
// NewTLSFieldToggled and returns a *tls.Config and a boolean flag indicating
// whether tls is explicitly enabled, or an error if the configuration was
// invalid.
func (p *ParsedConfig) FieldTLSToggled(path ...string) (tconf *tls.Config, enabled bool, err error) {
	v, exists := p.field(path...)
	if !exists {
		return nil, false, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	var node yaml.Node
	if err = node.Encode(v); err != nil {
		return
	}

	conf := btls.NewConfig()
	if err = node.Decode(&conf); err != nil {
		return
	}

	if enabled = conf.Enabled; !enabled {
		return
	}

	tconf, err = conf.Get(p.mgr.FS())
	return
}
