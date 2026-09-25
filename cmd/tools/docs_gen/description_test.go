// Copyright 2026 Redpanda Data, Inc.
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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// These cases match the JavaScript generator's tests in
// docs-extensions-and-macros, so both generators render the same way.

func TestProtectCodeSpans(t *testing.T) {
	escape := func(s string) string { return protectCodeSpans(escapePlaceholderBraces(s)) }

	assert.Equal(t, "custom objects end with `+__c+`; Big Objects end with `+__b+`",
		escape("custom objects end with `__c`; Big Objects end with `__b`"))
	assert.Equal(t, "exclude `+'.git/**', '**/*.png'+`", escape("exclude `'.git/**', '**/*.png'`"))
	assert.Equal(t, "a `+__$x {name}+` span", escape("a `__$x {name}` span"))

	unchanged := "plain `snake_case` and `+__c+`\n\n----\nkey: `__c`\n----"
	assert.Equal(t, unchanged, escape(unchanged))
}

func TestNormalizeMetadata(t *testing.T) {
	assert.Contains(t,
		normalizeMetadata("== Metadata\n\n- lsn (The commit LSN. Not present on snapshot (`read`) messages.)"),
		"- `lsn` (The commit LSN. Not present on snapshot (`read`) messages.)")

	assert.Equal(t,
		"== Metadata\n\nIf HTTPS is enabled, the following fields are added as well:\n\n- `tls_version`\n- `tls_subject`",
		normalizeMetadata("== Metadata\n\nIf HTTPS is enabled, the following fields are added as well:\n- `tls_version`\n- `tls_subject`"))

	wrapped := "== Metadata\n\n- `a` (first line\n  continues)\n- `b`"
	assert.Equal(t, wrapped, normalizeMetadata(wrapped))
}
