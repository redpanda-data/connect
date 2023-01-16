package metadata

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// IncludeFilterDocs returns a docs spec for a metadata filter where keys are
// ignored by default and must be explicitly included.
func IncludeFilterDocs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString(
			"include_prefixes", "Provide a list of explicit metadata key prefixes to match against.",
			[]string{"foo_", "bar_"},
			[]string{"kafka_"},
			[]string{"content-"},
		).Array().HasDefault([]any{}),
		docs.FieldString(
			"include_patterns", "Provide a list of explicit metadata key regular expression (re2) patterns to match against.",
			[]string{".*"},
			[]string{"_timestamp_unix$"},
		).Array().HasDefault([]any{}),
	}
}

// IncludeFilterConfig contains configuration fields for a metadata filter where
// keys are ignored by default and must be explicitly included.
type IncludeFilterConfig struct {
	IncludePrefixes []string `json:"include_prefixes" yaml:"include_prefixes"`
	IncludePatterns []string `json:"include_patterns" yaml:"include_patterns"`
}

// NewIncludeFilterConfig returns an IncludeFilterConfig struct with default
// values.
func NewIncludeFilterConfig() IncludeFilterConfig {
	return IncludeFilterConfig{
		IncludePrefixes: []string{},
		IncludePatterns: []string{},
	}
}

// CreateFilter attempts to construct a filter object.
func (c IncludeFilterConfig) CreateFilter() (*IncludeFilter, error) {
	var includePatterns []*regexp.Regexp
	for _, pattern := range c.IncludePatterns {
		compiledPattern, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to compile regexp %q: %s", pattern, err)
		}
		includePatterns = append(includePatterns, compiledPattern)
	}
	return &IncludeFilter{
		includePrefixes: c.IncludePrefixes,
		includePatterns: includePatterns,
	}, nil
}

// IncludeFilter provides a way to filter keys based on a Config.
type IncludeFilter struct {
	includePrefixes []string
	includePatterns []*regexp.Regexp
}

// IsSet returns true if there are any rules configured for matching keys.
func (f *IncludeFilter) IsSet() bool {
	return len(f.includePrefixes) > 0 || len(f.includePatterns) > 0
}

// Match returns true if the provided string matches the configured filters and
// false otherwise. It also returns false if no filters are configured.
func (f *IncludeFilter) Match(str string) bool {
	for _, prefix := range f.includePrefixes {
		if strings.HasPrefix(str, prefix) {
			return true
		}
	}
	for _, pattern := range f.includePatterns {
		if matched := pattern.MatchString(str); matched {
			return true
		}
	}
	return false
}

// Iter applies a function to each metadata key value pair that passes the
// filter.
func (f *IncludeFilter) Iter(m *message.Part, fn func(k string, v any) error) error {
	return m.MetaIterMut(func(k string, v any) error {
		if !f.Match(k) {
			return nil
		}
		return fn(k, v)
	})
}

// IterStr applies a function to each metadata key value pair that passes the
// filter with the value serialised as a string.
func (f *IncludeFilter) IterStr(m *message.Part, fn func(k, v string) error) error {
	return m.MetaIterStr(func(k, v string) error {
		if !f.Match(k) {
			return nil
		}
		return fn(k, v)
	})
}
