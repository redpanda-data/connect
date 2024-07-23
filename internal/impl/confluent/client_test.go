package confluent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestSchemaRegistryClient_GetSchemaBySubjectAndVersion(t *testing.T) {
	ctx := context.Background()
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
		wantResPayload          SchemaInfo
		wantErr                 assert.ErrorAssertionFunc
	}{
		{
			name:                    "sanity",
			schemaRegistryServerURL: "/subjects/foo/versions/latest",
			args: args{
				subject: "foo",
				version: nil,
			},
			wantResPayload: SchemaInfo{
				ID:     3,
				Schema: testSchema,
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
			wantResPayload: SchemaInfo{
				ID:     3,
				Schema: testSchema,
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
			wantResPayload: SchemaInfo{
				ID:     3,
				Schema: testSchema,
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
			wantResPayload: SchemaInfo{
				ID:     3,
				Schema: testSchema,
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

			c, err := newSchemaRegistryClient(urlStr, noopReqSign, nil, service.MockResources())
			require.NoError(t, err)

			gotResPayload, err := c.GetSchemaBySubjectAndVersion(ctx, tt.args.subject, tt.args.version)
			if !tt.wantErr(t, err, fmt.Sprintf("GetSchemaBySubjectAndVersion(%v, %v, %v)", ctx, tt.args.subject, tt.args.version)) {
				return
			}
			assert.Equalf(t, tt.wantResPayload, gotResPayload, "GetSchemaBySubjectAndVersion(%v, %v, %v)", ctx, tt.args.subject, tt.args.version)
		})
	}
}
