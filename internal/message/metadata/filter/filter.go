package filter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
)

// DocsFields returns a docs spec for the available config fields.
func DocsFields() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString("include_prefixes", "Provide a list of explicit metadata key prefixes to be included when adding metadata to sent messages.").Array(),
		docs.FieldString("include_patterns", "Provide a list of explicit metadata key regexp patterns to be included when adding metadata to sent messages.").Array(),
	}
}

// Config describes filtering actions to be performed on provided input strings.
type Config struct {
	IncludePrefixes []string `json:"include_prefixes" yaml:"include_prefixes"`
	IncludePatterns []string `json:"include_patterns" yaml:"include_patterns"`
}

// NewConfig returns a Config struct with default values.
func NewConfig() Config {
	return Config{
		IncludePrefixes: []string{},
		IncludePatterns: []string{},
	}
}

// New attempts to construct a filter object.
func (m Config) New() (*Filter, error) {
	var includePatterns []*regexp.Regexp
	for _, pattern := range m.IncludePatterns {
		compiledPattern, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to compile regexp %q: %s", pattern, err)
		}
		includePatterns = append(includePatterns, compiledPattern)
	}
	return &Filter{
		inclduePrefixes: m.IncludePrefixes,
		inclduePatterns: includePatterns,
	}, nil
}

// Filter provides a way to filter keys based on a Config.
type Filter struct {
	inclduePrefixes []string
	inclduePatterns []*regexp.Regexp
}

// IsSet returns true if any filters are configured and false otherwise.
func (f *Filter) IsSet() bool {
	return len(f.inclduePrefixes) > 0 || len(f.inclduePatterns) > 0
}

// Match checks if the provided string matches the configured filters.
func (f *Filter) Match(str string) bool {
	for _, prefix := range f.inclduePrefixes {
		if strings.HasPrefix(str, prefix) {
			return true
		}
	}
	for _, pattern := range f.inclduePatterns {
		if matched := pattern.MatchString(str); matched {
			return true
		}
	}
	return false
}
