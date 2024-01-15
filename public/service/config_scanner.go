package service

import (
	"fmt"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// NewScannerField defines a new scanner field, it is then possible to extract
// an OwnedScannerCreator from the resulting parsed config with the method
// FieldScanner.
func NewScannerField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldScanner(name, ""),
	}
}

func ownedScannerCreatorFromConfAny(mgr bundle.NewManagement, field any) (*OwnedScannerCreator, error) {
	pluginConf, err := scanner.FromAny(mgr.Environment(), field)
	if err != nil {
		return nil, err
	}

	irdr, err := mgr.NewScanner(pluginConf)
	if err != nil {
		return nil, err
	}
	return &OwnedScannerCreator{rdr: irdr}, nil
}

// FieldScanner accesses a field from a parsed config that was defined with
// NewScannerField and returns an OwnedScannerCreator, or an error if the
// configuration was invalid.
func (p *ParsedConfig) FieldScanner(path ...string) (*OwnedScannerCreator, error) {
	field, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}
	return ownedScannerCreatorFromConfAny(p.mgr.IntoPath(path...), field)
}
