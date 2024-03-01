package metadata

import (
	"slices"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// ExcludeFilterFields returns a docs spec for the fields within a metadata
// config struct.
func ExcludeFilterFields() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString("exclude_prefixes", "Provide a list of explicit metadata key prefixes to be excluded when adding metadata to sent messages.").
			Array().HasDefault([]any{}),
	}
}

// ExcludeFilterConfig describes actions to be performed on message metadata
// before being sent to an output destination.
type ExcludeFilterConfig struct {
	ExcludePrefixes []string `json:"exclude_prefixes" yaml:"exclude_prefixes"`
}

// NewExcludeFilterConfig returns a Metadata configuration struct with default values.
func NewExcludeFilterConfig() ExcludeFilterConfig {
	return ExcludeFilterConfig{
		ExcludePrefixes: []string{},
	}
}

// Filter attempts to construct a metadata filter.
func (m ExcludeFilterConfig) Filter() (*ExcludeFilter, error) {
	return &ExcludeFilter{
		excludePrefixes: m.ExcludePrefixes,
	}, nil
}

// ExcludeFilter provides a way to filter metadata keys based on a Metadata
// config.
type ExcludeFilter struct {
	excludePrefixes []string
}

// Match returns false if the provided string matches the configured filters and
// true otherwise. It also returns true if no filters are configured.
func (f *ExcludeFilter) Match(str string) bool {
	for _, prefix := range f.excludePrefixes {
		if strings.HasPrefix(str, prefix) || slices.Contains(f.excludePrefixes, "*") {
			return false
		}
	}
	return true
}

// Iter applies a function to each metadata key value pair that passes the
// filter.
func (f *ExcludeFilter) Iter(m *message.Part, fn func(k string, v any) error) error {
	return m.MetaIterMut(func(k string, v any) error {
		if !f.Match(k) {
			return nil
		}
		return fn(k, v)
	})
}

// IterStr applies a function to each metadata key value pair that passes the
// filter with the value serialised as a string.
func (f *ExcludeFilter) IterStr(m *message.Part, fn func(k, v string) error) error {
	return m.MetaIterStr(func(k, v string) error {
		if !f.Match(k) {
			return nil
		}
		return fn(k, v)
	})
}
