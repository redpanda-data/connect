package httpclient

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/metadata"
)

// MultipartExpressions represents three dynamic expressions that define a
// multipart message part in an HTTP request. Specifying one or more of these
// can be used as a way of creating HTTP requests that overrides the default
// behaviour.
type MultipartExpressions struct {
	ContentDisposition *field.Expression
	ContentType        *field.Expression
	Body               *field.Expression
}

// RequestSigner is a closure configured to enrich requests with various
// functions, usually authentication.
type RequestSigner func(f ifs.FS, req *http.Request) error

// RequestCreator creates *http.Request types from messages based on various
// configurable parameters.
type RequestCreator struct {
	// Explicit body overrides, in order of precedence
	explicitBody       *field.Expression
	explicitMultiparts []MultipartExpressions

	fs        ifs.FS
	reqSigner RequestSigner

	url              *field.Expression
	host             *field.Expression
	verb             string
	headers          map[string]*field.Expression
	metaInsertFilter *metadata.IncludeFilter
}

// RequestOpt represents a customisation of a request creator.
type RequestOpt func(r *RequestCreator)

// RequestCreatorFromOldConfig creates a new request creator from an old struct
// style config. Eventually I'd like to phase these out for the more dynamic
// service style parses, but it'll take a while so we have this for now.
func RequestCreatorFromOldConfig(conf OldConfig, mgr bundle.NewManagement, opts ...RequestOpt) (*RequestCreator, error) {
	r := &RequestCreator{
		fs:        mgr.FS(),
		reqSigner: conf.AuthConfig.Sign,
		verb:      conf.Verb,
		headers:   map[string]*field.Expression{},
	}
	for _, opt := range opts {
		opt(r)
	}

	var err error
	if r.url, err = mgr.BloblEnvironment().NewField(conf.URL); err != nil {
		return nil, fmt.Errorf("failed to parse URL expression: %v", err)
	}

	for k, v := range conf.Headers {
		if strings.EqualFold(k, "host") {
			if r.host, err = mgr.BloblEnvironment().NewField(v); err != nil {
				return nil, fmt.Errorf("failed to parse header 'host' expression: %v", err)
			}
		} else {
			if r.headers[k], err = mgr.BloblEnvironment().NewField(v); err != nil {
				return nil, fmt.Errorf("failed to parse header '%v' expression: %v", k, err)
			}
		}
	}

	if r.metaInsertFilter, err = conf.Metadata.CreateFilter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}
	return r, nil
}

// WithExplicitBody modifies the request creator to instead only use input
// reference messages for headers and metadata, and use the expression for
// creating a body.
func WithExplicitBody(e *field.Expression) RequestOpt {
	return func(r *RequestCreator) {
		r.explicitBody = e
	}
}

// WithExplicitMultipart modifies the request creator to instead only use input
// reference messages for headers and metadata, and use a list of multipart
// expressions for creating a body.
func WithExplicitMultipart(m []MultipartExpressions) RequestOpt {
	return func(r *RequestCreator) {
		r.explicitMultiparts = m
	}
}

func (r *RequestCreator) bodyFromExplicit(refBatch message.Batch) (body io.Reader, overrideContentType string, err error) {
	if _, exists := r.headers["Content-Type"]; !exists {
		overrideContentType = "application/octet-stream"
	}
	var bBytes []byte
	if bBytes, err = r.explicitBody.Bytes(0, refBatch); err != nil {
		return
	}
	body = bytes.NewBuffer(bBytes)
	return
}

func (r *RequestCreator) bodyFromExplicitMultipart(refBatch message.Batch) (body io.Reader, overrideContentType string, err error) {
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	for _, v := range r.explicitMultiparts {
		mh := make(textproto.MIMEHeader)
		var cTypeStr, cDispStr string
		if cTypeStr, err = v.ContentType.String(0, refBatch); err != nil {
			err = fmt.Errorf("content-type interpolation error: %w", err)
			return
		}
		if cDispStr, err = v.ContentDisposition.String(0, refBatch); err != nil {
			err = fmt.Errorf("content-disposition interpolation error: %w", err)
			return
		}
		mh.Set("Content-Type", cTypeStr)
		mh.Set("Content-Disposition", cDispStr)

		var part io.Writer
		if part, err = writer.CreatePart(mh); err != nil {
			return
		}
		var partBytes []byte
		if partBytes, err = v.Body.Bytes(0, refBatch); err != nil {
			err = fmt.Errorf("part body interpolation error: %w", err)
			return
		}
		if _, err = io.Copy(part, bytes.NewReader(partBytes)); err != nil {
			return
		}
	}
	writer.Close()
	body = buf
	overrideContentType = writer.FormDataContentType()
	return
}

func (r *RequestCreator) body(refBatch message.Batch) (body io.Reader, overrideContentType string, err error) {
	if r.explicitBody != nil {
		body, overrideContentType, err = r.bodyFromExplicit(refBatch)
		return
	}

	if len(r.explicitMultiparts) > 0 {
		body, overrideContentType, err = r.bodyFromExplicitMultipart(refBatch)
		return
	}

	if len(refBatch) == 0 {
		return
	}

	if len(refBatch) == 1 {
		if _, exists := r.headers["Content-Type"]; !exists {
			overrideContentType = "application/octet-stream"
		}
		body = bytes.NewBuffer(refBatch[0].AsBytes())
		return
	}

	// More than one message in the batch, create a multipart message by
	// default.
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)

	for i, p := range refBatch {
		contentType := "application/octet-stream"
		if v, exists := r.headers["Content-Type"]; exists {
			if contentType, err = v.String(i, refBatch); err != nil {
				err = fmt.Errorf("content-type interpolation error: %w", err)
				return
			}
		}

		headers := textproto.MIMEHeader{
			"Content-Type": []string{contentType},
		}
		_ = r.metaInsertFilter.Iter(p, func(k string, v any) error {
			headers[k] = append(headers[k], query.IToString(v))
			return nil
		})

		var part io.Writer
		if part, err = writer.CreatePart(headers); err != nil {
			return
		}
		if _, err = io.Copy(part, bytes.NewReader(p.AsBytes())); err != nil {
			return
		}
	}

	writer.Close()
	overrideContentType = writer.FormDataContentType()

	body = buf
	return
}

// Create an *http.Request using a reference message batch to extract the body
// and headers of the request. It's possible that the creator has been given
// explicit overrides for the body, in which case the reference batch is only
// used for general request headers/metadata enrichment.
func (r *RequestCreator) Create(refBatch message.Batch) (req *http.Request, err error) {
	var overrideContentType string
	var body io.Reader
	if body, overrideContentType, err = r.body(refBatch); err != nil {
		return
	}

	var urlStr string
	if urlStr, err = r.url.String(0, refBatch); err != nil {
		err = fmt.Errorf("url interpolation error: %w", err)
		return
	}
	if req, err = http.NewRequest(r.verb, urlStr, body); err != nil {
		return
	}

	for k, v := range r.headers {
		var hStr string
		if hStr, err = v.String(0, refBatch); err != nil {
			err = fmt.Errorf("header '%v' interpolation error: %w", k, err)
			return
		}
		req.Header.Add(k, hStr)
	}
	if len(refBatch) > 0 {
		_ = r.metaInsertFilter.Iter(refBatch[0], func(k string, v any) error {
			req.Header.Add(k, query.IToString(v))
			return nil
		})
	}

	if r.host != nil {
		if req.Host, err = r.host.String(0, refBatch); err != nil {
			err = fmt.Errorf("host interpolation error: %w", err)
			return
		}
	}
	if overrideContentType != "" {
		req.Header.Del("Content-Type")
		req.Header.Add("Content-Type", overrideContentType)
	}

	err = r.reqSigner(r.fs, req)
	return
}
