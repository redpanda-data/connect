package output

import (
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// MetadataFields returns a docs spec for the fields within a metadata config
// struct.
func MetadataFields() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon("exclude_prefixes", "Provide a list of explicit metadata key prefixes to be excluded when adding metadata to sent messages.").Array(),
	}
}

// Metadata describes actions to be performed on message metadata before being
// sent to an output destination.
type Metadata struct {
	ExcludePrefixes []string `json:"exclude_prefixes" yaml:"exclude_prefixes"`
}

// NewMetadata returns a Metadata configuration struct with default values.
func NewMetadata() Metadata {
	return Metadata{
		ExcludePrefixes: []string{},
	}
}

// Filter attempts to construct a metadata filter.
func (m Metadata) Filter() (*MetadataFilter, error) {
	return &MetadataFilter{
		m.ExcludePrefixes,
	}, nil
}

// MetadataFilter provides a way to filter metadata keys based on a Metadata
// config.
type MetadataFilter struct {
	excludePrefixes []string
}

// Iter applies a function to each metadata key value pair that passes the
// filter.
func (f *MetadataFilter) Iter(m types.Metadata, fn func(k, v string) error) error {
	return m.Iter(func(k, v string) error {
		for _, prefix := range f.excludePrefixes {
			if strings.HasPrefix(k, prefix) {
				return nil
			}
		}
		return fn(k, v)
	})
}
