package service

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/metadata"
)

// NewMetadataFilterField creates a config field spec for describing which
// metadata keys to include for a given purpose. This includes prefix based and
// regular expression based methods. This field is often used for making
// metadata written to output destinations explicit.
func NewMetadataFilterField(name string) *ConfigField {
	field := docs.FieldObject(name, "").WithChildren(metadata.IncludeFilterDocs()...)
	return &ConfigField{field: field}
}

// MetadataFilter provides a configured mechanism for filtering metadata
// key/values from a message.
type MetadataFilter struct {
	f *metadata.IncludeFilter
}

// Walk iterates the filtered metadata key/value pairs from a message and
// executes a provided closure function for each pair. An error returned by the
// closure will be returned by this function and prevent subsequent pairs from
// being accessed.
func (m *MetadataFilter) Walk(msg *Message, fn func(key, value string) error) error {
	if m == nil {
		return nil
	}
	return msg.MetaWalk(func(key, value string) error {
		if !m.f.Match(key) {
			return nil
		}
		return fn(key, value)
	})
}

// FieldMetadataFilter accesses a field from a parsed config that was defined
// with NewMetdataFilterField and returns a MetadataFilter, or an error if the
// configuration was invalid.
func (p *ParsedConfig) FieldMetadataFilter(path ...string) (f *MetadataFilter, err error) {
	confNode, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", p.fullDotPath(path...))
	}

	var node yaml.Node
	if err = node.Encode(confNode); err != nil {
		return
	}

	conf := metadata.NewIncludeFilterConfig()
	if err = node.Decode(&conf); err != nil {
		return
	}

	var filter *metadata.IncludeFilter
	if filter, err = conf.CreateFilter(); err != nil {
		return
	}

	f = &MetadataFilter{f: filter}
	return
}
