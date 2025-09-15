// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package confx

import (
	"fmt"
	"regexp"
)

// RegexpFilter provides include/exclude filtering using regular expressions.
type RegexpFilter struct {
	// Include filters subjects to include by regex. Empty slice matches all subjects.
	Include []*regexp.Regexp
	// Exclude filters subjects to exclude by regex. Empty slice disables exclusion.
	Exclude []*regexp.Regexp
}

// Filtered returns a list values filtered by include and exclude patterns.
// See Matches for details.
func (f RegexpFilter) Filtered(all []string) []string {
	if len(f.Include) == 0 && len(f.Exclude) == 0 {
		return all
	}

	filtered := make([]string, 0, len(all))
	for _, s := range all {
		if f.Matches(s) {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

// Matches returns true if the given string matches at least one include
// pattern (or no include patterns are set) and does not match any exclude pattern.
func (f RegexpFilter) Matches(s string) bool {
	if len(f.Include) == 0 && len(f.Exclude) == 0 {
		return true
	}

	// Check include patterns - must match at least one if any are set
	if len(f.Include) > 0 {
		matched := false
		for _, re := range f.Include {
			if re.MatchString(s) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Check exclude patterns - must not match any
	for _, re := range f.Exclude {
		if re.MatchString(s) {
			return false
		}
	}

	return true
}

// ParseRegexpPatterns compiles a list of regular expression patterns.
// Empty patterns are ignored. Returns an error if any pattern is invalid.
func ParseRegexpPatterns(patterns []string) ([]*regexp.Regexp, error) {
	if len(patterns) == 0 {
		return nil, nil
	}

	regexps := make([]*regexp.Regexp, 0, len(patterns))
	for i, pattern := range patterns {
		if pattern == "" {
			continue
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern at index %d (%q): %w", i, pattern, err)
		}
		regexps = append(regexps, re)
	}
	return regexps, nil
}
