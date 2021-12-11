package metadata

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
)

// IncludeFilterDocs returns a docs spec for a metadata filter where keys are
// ignored by default and must be explicitly included.
func IncludeFilterDocs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString("include_prefixes", "Provide a list of explicit metadata key prefixes to be included when adding metadata to sent messages.").Array(),
		docs.FieldString("include_patterns", "Provide a list of explicit metadata key regexp patterns to be included when adding metadata to sent messages.").Array(),
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
func (c IncludeFilterConfig) CreateFilter() (*Filter, error) {
	var includePatterns []*regexp.Regexp
	for _, pattern := range c.IncludePatterns {
		compiledPattern, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to compile regexp %q: %s", pattern, err)
		}
		includePatterns = append(includePatterns, compiledPattern)
	}
	return &Filter{
		includePrefixes: c.IncludePrefixes,
		includePatterns: includePatterns,
	}, nil
}

// Filter provides a way to filter keys based on a Config.
type Filter struct {
	includePrefixes []string
	includePatterns []*regexp.Regexp
}

// IsSet returns true if there are any rules configured for matching keys.
func (f *Filter) IsSet() bool {
	return len(f.includePrefixes) > 0 || len(f.includePatterns) > 0
}

// Match returns true if the provided string matches the configured filters and
// false otherwise. It also returns false if no filters are configured.
func (f *Filter) Match(str string) bool {
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
