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

package s3

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestObjectCannedACLConfigParsing exercises the full parse path for the
// object_canned_acl field: schema lint plus the runtime validator in
// s3oConfigFromParsed. It guards the regression fixed in PR #4413 where the
// default value "" was rejected by the validator.
func TestObjectCannedACLConfigParsing(t *testing.T) {
	const baseConfig = `
bucket: foo
region: eu-west-1
credentials:
  id: xxxxx
  secret: xxxxx
`

	tests := []struct {
		name        string
		aclYAML     string
		expectedACL types.ObjectCannedACL
	}{
		{
			name:        "field omitted uses empty default",
			aclYAML:     "",
			expectedACL: types.ObjectCannedACL(""),
		},
		{
			name:        "explicit empty string",
			aclYAML:     `object_canned_acl: ""`,
			expectedACL: types.ObjectCannedACL(""),
		},
		{
			name:        "valid canned ACL bucket-owner-full-control",
			aclYAML:     `object_canned_acl: bucket-owner-full-control`,
			expectedACL: types.ObjectCannedACLBucketOwnerFullControl,
		},
		{
			name:        "valid canned ACL private",
			aclYAML:     `object_canned_acl: private`,
			expectedACL: types.ObjectCannedACLPrivate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yaml := baseConfig + tt.aclYAML + "\n"

			parsed, err := s3oOutputSpec().ParseYAML(yaml, nil)
			require.NoError(t, err)

			conf, err := s3oConfigFromParsed(parsed)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedACL, conf.ObjectCannedACL)
		})
	}
}

// TestObjectCannedACLLinting verifies that the schema's enum linter accepts
// the empty string as well as valid SDK canned ACL values, and rejects bogus
// ones. This locks in the agreement between the schema and the runtime
// validator after PR #4413.
func TestObjectCannedACLLinting(t *testing.T) {
	const baseConfig = `
aws_s3:
  bucket: foo
  region: eu-west-1
  credentials:
    id: xxxxx
    secret: xxxxx
`

	tests := []struct {
		name         string
		aclYAML      string
		lintContains string // empty means: expect no lint errors
	}{
		{
			name:    "field omitted",
			aclYAML: "",
		},
		{
			name:    "explicit empty string",
			aclYAML: `  object_canned_acl: ""`,
		},
		{
			name:    "valid canned ACL bucket-owner-full-control",
			aclYAML: `  object_canned_acl: bucket-owner-full-control`,
		},
		{
			name:    "valid canned ACL private",
			aclYAML: `  object_canned_acl: private`,
		},
		{
			name:         "invalid canned ACL value is rejected",
			aclYAML:      `  object_canned_acl: not-a-real-acl`,
			lintContains: "not-a-real-acl",
		},
	}

	linter := service.GlobalEnvironment().NewComponentConfigLinter()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yaml := baseConfig + tt.aclYAML + "\n"
			lints, err := linter.LintOutputYAML([]byte(yaml))
			require.NoError(t, err)

			if tt.lintContains == "" {
				assert.Empty(t, lints, "expected no lint errors, got: %v", lints)
				return
			}

			require.NotEmpty(t, lints, "expected a lint error containing %q", tt.lintContains)
			var combined strings.Builder
			for _, l := range lints {
				fmt.Fprintf(&combined, "%v\n", l)
			}
			assert.Contains(t, combined.String(), tt.lintContains)
		})
	}
}
