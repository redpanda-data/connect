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

package confluent

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	franz_sr "github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

func TestSchemaRegistryClient_GetSchemaBySubjectAndVersion(t *testing.T) {
	ctx := t.Context()
	fooFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchema,
		ID:     3,
	})
	require.NoError(t, err)

	version := 4

	type args struct {
		subject string
		version *int
	}
	tests := []struct {
		name                    string
		schemaRegistryServerURL string
		args                    args
		wantResPayload          franz_sr.SubjectSchema
		wantErr                 assert.ErrorAssertionFunc
	}{
		{
			name:                    "sanity",
			schemaRegistryServerURL: "/subjects/foo/versions/latest",
			args: args{
				subject: "foo",
				version: nil,
			},
			wantResPayload: franz_sr.SubjectSchema{
				ID:     3,
				Schema: franz_sr.Schema{Schema: testSchema},
			},
			wantErr: assert.NoError,
		},
		{
			name:                    "contains sep (%2F)",
			schemaRegistryServerURL: "/subjects/main%2Fcommon/versions/latest",
			args: args{
				subject: "main/common",
				version: nil,
			},
			wantResPayload: franz_sr.SubjectSchema{
				ID:     3,
				Schema: franz_sr.Schema{Schema: testSchema},
			},
			wantErr: assert.NoError,
		},
		{
			name:                    "sanity with version",
			schemaRegistryServerURL: "/subjects/foo/versions/4",
			args: args{
				subject: "foo",
				version: &version,
			},
			wantResPayload: franz_sr.SubjectSchema{
				ID:     3,
				Schema: franz_sr.Schema{Schema: testSchema},
			},
			wantErr: assert.NoError,
		},
		{
			name:                    "contains sep (%2F)  with version",
			schemaRegistryServerURL: "/subjects/main%2Fcommon/versions/4",
			args: args{
				subject: "main/common",
				version: &version,
			},
			wantResPayload: franz_sr.SubjectSchema{
				ID:     3,
				Schema: franz_sr.Schema{Schema: testSchema},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
				if path == tt.schemaRegistryServerURL {
					return fooFirst, nil
				}
				return nil, errors.New("nope")
			})
			c, err := sr.NewClient(urlStr, noopReqSign, nil, service.MockResources())
			require.NoError(t, err)

			gotResPayload, err := c.GetSchemaBySubjectAndVersion(ctx, tt.args.subject, tt.args.version, false)
			if !tt.wantErr(t, err, fmt.Sprintf("GetSchemaBySubjectAndVersion(%v, %v, %v)", ctx, tt.args.subject, tt.args.version)) {
				return
			}
			assert.Equalf(t, tt.wantResPayload, gotResPayload, "GetSchemaBySubjectAndVersion(%v, %v, %v)", ctx, tt.args.subject, tt.args.version)
		})
	}
}
