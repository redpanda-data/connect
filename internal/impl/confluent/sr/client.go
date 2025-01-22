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

package sr

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"slices"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Client is used to make requests to a schema registry.
type Client struct {
	Client *sr.Client
}

// NewClient creates a new schema registry client.
func NewClient(
	urlStr string,
	reqSigner func(f fs.FS, req *http.Request) error,
	tlsConf *tls.Config,
	mgr *service.Resources,
) (*Client, error) {
	_, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url: %w", err)
	}

	clientSR, err := sr.NewClient(sr.URLs(urlStr), sr.PreReq(func(req *http.Request) error { return reqSigner(mgr.FS(), req) }))
	if err != nil {
		return nil, fmt.Errorf("failed to init client: %w", err)
	}

	return &Client{
		Client: clientSR,
	}, nil
}

// GetSchemaByID gets a schema by its global identifier.
func (c *Client) GetSchemaByID(ctx context.Context, id int, includeDeleted bool) (sr.Schema, error) {
	if includeDeleted {
		ctx = sr.WithParams(ctx, sr.ShowDeleted)
	}

	schema, err := c.Client.SchemaByID(ctx, id)
	if err != nil {
		return sr.Schema{}, fmt.Errorf("schema %d not found by registry: %s", id, err)
	}
	return schema, nil
}

// GetSubjectsBySchemaID returns the registered subjects for a given schema ID.
func (c *Client) GetSubjectsBySchemaID(ctx context.Context, id int, includeDeleted bool) ([]string, error) {
	if includeDeleted {
		ctx = sr.WithParams(ctx, sr.ShowDeleted)
	}

	return c.Client.SubjectsByID(ctx, id)
}

// GetLatestSchemaVersionForSchemaIDAndSubject gets the latest version of a schema by its global identifier scoped to the provided subject.
func (c *Client) GetLatestSchemaVersionForSchemaIDAndSubject(ctx context.Context, id int, subject string) (versionID int, err error) {
	svs, err := c.Client.SchemaVersionsByID(ctx, id)
	if err != nil {
		return -1, fmt.Errorf("failed to fetch schema versions for ID %d and subject %q", id, subject)
	}

	versions := []int{}
	for _, sv := range svs {
		if sv.Subject == subject {
			versions = append(versions, sv.Version)
		}
	}

	if len(versions) == 0 {
		return -1, fmt.Errorf("no schema versions found for ID %d and subject %q", id, subject)
	}

	slices.Sort(versions)
	return versions[len(versions)-1], nil
}

// GetSchemaBySubjectAndVersion returns the schema by its subject and optional version. A `nil` version returns the latest schema.
func (c *Client) GetSchemaBySubjectAndVersion(ctx context.Context, subject string, version *int, includeDeleted bool) (sr.SubjectSchema, error) {
	if includeDeleted {
		ctx = sr.WithParams(ctx, sr.ShowDeleted)
	}

	var schema sr.SubjectSchema
	var err error
	if version != nil {
		schema, err = c.Client.SchemaByVersion(ctx, subject, *version)
	} else {
		// Setting version to -1 will return the latest schema.
		schema, err = c.Client.SchemaByVersion(ctx, subject, -1)
	}
	if err != nil {
		return sr.SubjectSchema{}, err
	}

	return schema, nil
}

// GetMode returns the mode of the Schema Registry instance.
func (c *Client) GetMode(ctx context.Context) (string, error) {
	res := c.Client.Mode(ctx)
	// There will be one and only one element in the response.
	if res[0].Err != nil {
		return "", fmt.Errorf("request failed: %s", res[0].Err)
	}

	return res[0].Mode.String(), nil
}

// GetSubjects returns the registered subjects.
func (c *Client) GetSubjects(ctx context.Context, includeDeleted bool) ([]string, error) {
	if includeDeleted {
		ctx = sr.WithParams(ctx, sr.ShowDeleted)
	}

	return c.Client.Subjects(ctx)
}

// GetVersionsForSubject returns the versions for a given subject.
func (c *Client) GetVersionsForSubject(ctx context.Context, subject string, includeDeleted bool) ([]int, error) {
	if includeDeleted {
		ctx = sr.WithParams(ctx, sr.ShowDeleted)
	}

	return c.Client.SubjectVersions(ctx, subject)
}

// CreateSchema creates a new schema for the given subject.
func (c *Client) CreateSchema(ctx context.Context, subject string, schema sr.Schema) (int, error) {
	ss, err := c.Client.CreateSchema(ctx, subject, schema)
	if err != nil {
		return -1, fmt.Errorf("failed to create schema for subject %q: %s", subject, err)
	}

	return ss.ID, nil
}

type refWalkFn func(ctx context.Context, name string, info sr.Schema) error

// WalkReferences goes through the provided schema info and for each reference
// the provided closure is called recursively, which means each reference obtained
// will also be walked.
//
// If a reference of a given subject but differing version is detected an error
// is returned as this would put us in an invalid state.
func (c *Client) WalkReferences(ctx context.Context, refs []sr.SchemaReference, fn refWalkFn) error {
	return c.walkReferencesTracked(ctx, map[string]int{}, refs, fn)
}

func (c *Client) walkReferencesTracked(ctx context.Context, seen map[string]int, refs []sr.SchemaReference, fn refWalkFn) error {
	for _, ref := range refs {
		if i, exists := seen[ref.Name]; exists {
			if i != ref.Version {
				return fmt.Errorf("duplicate reference '%v' version mismatch of %v and %v, aborting in order to avoid invalid state", ref.Name, i, ref.Version)
			}
			continue
		}
		info, err := c.GetSchemaBySubjectAndVersion(ctx, ref.Subject, &ref.Version, false)
		if err != nil {
			return err
		}
		if err := fn(ctx, ref.Name, info.Schema); err != nil {
			return err
		}
		seen[ref.Name] = ref.Version
		if err := c.walkReferencesTracked(ctx, seen, info.References, fn); err != nil {
			return err
		}
	}
	return nil
}
