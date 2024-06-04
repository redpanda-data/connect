package confluent

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

	"github.com/redpanda-data/benthos/v4/public/service"
)

type schemaRegistryClient struct {
	client                *http.Client
	schemaRegistryBaseURL *url.URL
	requestSigner         func(f fs.FS, req *http.Request) error
	mgr                   *service.Resources
}

func newSchemaRegistryClient(
	urlStr string,
	reqSigner func(f fs.FS, req *http.Request) error,
	tlsConf *tls.Config,
	mgr *service.Resources,
) (*schemaRegistryClient, error) {
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

	return &schemaRegistryClient{
		client:                hClient,
		schemaRegistryBaseURL: u,
		requestSigner:         reqSigner,
		mgr:                   mgr,
	}, nil
}

type SchemaInfo struct {
	ID         int               `json:"id"`
	Type       string            `json:"schemaType"`
	Schema     string            `json:"schema"`
	References []SchemaReference `json:"references"`
}

// TODO: Further reading:
// https://www.confluent.io/blog/multiple-event-types-in-the-same-kafka-topic/
type SchemaReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

func (c *schemaRegistryClient) GetSchemaByID(ctx context.Context, id int) (resPayload SchemaInfo, err error) {
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

func (c *schemaRegistryClient) GetSchemaBySubjectAndVersion(ctx context.Context, subject string, version *int) (resPayload SchemaInfo, err error) {
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

type RefWalkFn func(ctx context.Context, name string, info SchemaInfo) error

// For each reference provided the schema info is obtained and the provided
// closure is called recursively, which means each reference obtained will also
// be walked.
//
// If a reference of a given subject but differing version is detected an error
// is returned as this would put us in an invalid state.
func (c *schemaRegistryClient) WalkReferences(ctx context.Context, refs []SchemaReference, fn RefWalkFn) error {
	return c.walkReferencesTracked(ctx, map[string]int{}, refs, fn)
}

func (c *schemaRegistryClient) walkReferencesTracked(ctx context.Context, seen map[string]int, refs []SchemaReference, fn RefWalkFn) error {
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

func (c *schemaRegistryClient) doRequest(ctx context.Context, verb, reqPath string) (resCode int, resBody []byte, err error) {
	reqURL := *c.schemaRegistryBaseURL
	if reqURL.Path, err = url.JoinPath(reqURL.Path, reqPath); err != nil {
		return
	}

	var req *http.Request
	if req, err = http.NewRequestWithContext(ctx, verb, reqURL.String(), http.NoBody); err != nil {
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
