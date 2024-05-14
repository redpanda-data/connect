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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"regexp"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var (
	EscapedSepRegexp = regexp.MustCompile("(?i)%2F")
)

// Client is used to make requests to a schema registry.
type Client struct {
	SchemaRegistryBaseURL *url.URL
	client                *http.Client
	requestSigner         func(f fs.FS, req *http.Request) error
	mgr                   *service.Resources
}

// NewClient creates a new schema registry client.
func NewClient(
	urlStr string,
	reqSigner func(f fs.FS, req *http.Request) error,
	tlsConf *tls.Config,
	mgr *service.Resources,
) (*Client, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url: %w", err)
	}

	hClient := http.DefaultClient
	if tlsConf != nil {
		hClient = &http.Client{}
		if c, ok := http.DefaultTransport.(*http.Transport); ok {
			cloned := c.Clone()
			cloned.TLSClientConfig = tlsConf
			hClient.Transport = cloned
		} else {
			hClient.Transport = &http.Transport{
				TLSClientConfig: tlsConf,
			}
		}
	}

	return &Client{
		client:                hClient,
		SchemaRegistryBaseURL: u,
		requestSigner:         reqSigner,
		mgr:                   mgr,
	}, nil
}

// SchemaInfo is the information about a schema stored in the registry.
type SchemaInfo struct {
	ID         int               `json:"id"`
	Type       string            `json:"schemaType"`
	Schema     string            `json:"schema"`
	References []SchemaReference `json:"references"`
}

// SchemaReference is a reference to another schema within the registry.
//
// TODO: further reading https://www.confluent.io/blog/multiple-event-types-in-the-same-kafka-topic/
type SchemaReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// GetSchemaByID gets a schema by it's global identifier.
func (c *Client) GetSchemaByID(ctx context.Context, id int) (resPayload SchemaInfo, err error) {
	var resCode int
	var resBody []byte
	if resCode, resBody, err = c.doRequest(ctx, "GET", fmt.Sprintf("/schemas/ids/%v", id)); err != nil {
		err = fmt.Errorf("request failed for schema '%v': %v", id, err)
		c.mgr.Logger().Errorf(err.Error())
		return
	}

	if resCode == http.StatusNotFound {
		err = fmt.Errorf("schema '%v' not found by registry", id)
		c.mgr.Logger().Errorf(err.Error())
		return
	}

	if len(resBody) == 0 {
		c.mgr.Logger().Errorf("request for schema '%v' returned an empty body", id)
		err = errors.New("schema request returned an empty body")
		return
	}

	if err = json.Unmarshal(resBody, &resPayload); err != nil {
		c.mgr.Logger().Errorf("failed to parse response for schema '%v': %v", id, err)
		return
	}
	return
}

// GetSchemaBySubjectAndVersion returns the schema by it's subject and optional version. A `nil` version returns the latest schema.
func (c *Client) GetSchemaBySubjectAndVersion(ctx context.Context, subject string, version *int) (resPayload SchemaInfo, err error) {
	var path string
	if version != nil {
		path = fmt.Sprintf("/subjects/%s/versions/%v", url.PathEscape(subject), *version)
	} else {
		path = fmt.Sprintf("/subjects/%s/versions/latest", url.PathEscape(subject))
	}

	var resCode int
	var resBody []byte
	if resCode, resBody, err = c.doRequest(ctx, "GET", path); err != nil {
		err = fmt.Errorf("request failed for schema subject '%v': %v", subject, err)
		c.mgr.Logger().Errorf(err.Error())
		return
	}

	if resCode == http.StatusNotFound {
		err = fmt.Errorf("schema subject '%v' not found by registry", subject)
		c.mgr.Logger().Errorf(err.Error())
		return
	}

	if len(resBody) == 0 {
		c.mgr.Logger().Errorf("request for schema subject '%v' returned an empty body", subject)
		err = errors.New("schema request returned an empty body")
		return
	}

	if err = json.Unmarshal(resBody, &resPayload); err != nil {
		c.mgr.Logger().Errorf("failed to parse response for schema subject '%v': %v", subject, err)
		return
	}
	return
}

type refWalkFn func(ctx context.Context, name string, info SchemaInfo) error

// WalkReferences goes through the provided schema info and for each reference
// the provided closure is called recursively, which means each reference obtained
// will also be walked.
//
// If a reference of a given subject but differing version is detected an error
// is returned as this would put us in an invalid state.
func (c *Client) WalkReferences(ctx context.Context, refs []SchemaReference, fn refWalkFn) error {
	return c.walkReferencesTracked(ctx, map[string]int{}, refs, fn)
}

func (c *Client) walkReferencesTracked(ctx context.Context, seen map[string]int, refs []SchemaReference, fn refWalkFn) error {
	for _, ref := range refs {
		if i, exists := seen[ref.Name]; exists {
			if i != ref.Version {
				return fmt.Errorf("duplicate reference '%v' version mismatch of %v and %v, aborting in order to avoid invalid state", ref.Name, i, ref.Version)
			}
			continue
		}
		info, err := c.GetSchemaBySubjectAndVersion(ctx, ref.Subject, &ref.Version)
		if err != nil {
			return err
		}
		if err := fn(ctx, ref.Name, info); err != nil {
			return err
		}
		seen[ref.Name] = ref.Version
		if err := c.walkReferencesTracked(ctx, seen, info.References, fn); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) doRequest(ctx context.Context, verb, reqPath string) (resCode int, resBody []byte, err error) {
	reqURL := *c.SchemaRegistryBaseURL
	if reqURL.Path, err = url.JoinPath(reqURL.Path, reqPath); err != nil {
		return
	}

	reqUrlString := reqURL.String()
	if match := EscapedSepRegexp.MatchString(reqPath); match {
		// Supporting '%2f' in the request url bypassing
		// Workaround for Golang issue https://github.com/golang/go/issues/3659
		if reqUrlString, err = url.PathUnescape(reqUrlString); err != nil {
			return
		}
	}

	var req *http.Request
	if req, err = http.NewRequestWithContext(ctx, verb, reqUrlString, http.NoBody); err != nil {
		return
	}
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json")
	if err = c.requestSigner(c.mgr.FS(), req); err != nil {
		return
	}

	for i := 0; i < 3; i++ {
		var res *http.Response
		if res, err = c.client.Do(req); err != nil {
			c.mgr.Logger().Errorf("request failed: %v", err)
			continue
		}

		if resCode = res.StatusCode; resCode == http.StatusNotFound {
			break
		}

		resBody, err = io.ReadAll(res.Body)
		_ = res.Body.Close()
		if err != nil {
			c.mgr.Logger().Errorf("failed to read response body: %v", err)
			break
		}

		if resCode != http.StatusOK {
			if len(resBody) > 0 {
				err = fmt.Errorf("status code %v: %s", resCode, bytes.TrimSpace(resBody))
			} else {
				err = fmt.Errorf("status code %v", resCode)
			}
			c.mgr.Logger().Errorf(err.Error())
		}
		break
	}
	return
}
