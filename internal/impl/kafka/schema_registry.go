// Copyright 2024 Redpanda Data, Inc.
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

package kafka

import (
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	franz_sr "github.com/twmb/franz-go/pkg/sr"
)

// srResourceKey is a type that represents a key for registering a `schema_registry` resource.
type srResourceKey string

// SchemasEqual compares two schema objects for equality, ignoring newlines and leading/trailing spaces in the schema string.
func SchemasEqual(lhs, rhs franz_sr.Schema) bool {
	// TODO: Remove this utility after https://github.com/redpanda-data/redpanda/issues/26331 is resolved.

	// Remove newlines and leading/trailing spaces from the schemas before comparison.
	lhsSchema := strings.TrimSpace(strings.ReplaceAll(lhs.Schema, "\n", ""))
	rhsSchema := strings.TrimSpace(strings.ReplaceAll(rhs.Schema, "\n", ""))

	if lhsSchema != rhsSchema {
		return false
	}

	// Compare the rest of the fields.
	return cmp.Equal(lhs, rhs, cmpopts.IgnoreFields(franz_sr.Schema{}, "Schema"))
}
