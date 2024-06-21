package httpclient

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/value"
	"github.com/benthosdev/benthos/v4/public/service"
)

// MultipartExpressions represents three dynamic expressions that define a
// multipart message part in an HTTP request. Specifying one or more of these
// can be used as a way of creating HTTP requests that overrides the default
// behaviour.
type MultipartExpressions struct {
	ContentDisposition *service.InterpolatedString
	ContentType        *service.InterpolatedString
	Body               *service.InterpolatedString
}

// RequestCreator creates *http.Request types from messages based on various
// configurable parameters.
type RequestCreator struct {
	// Explicit body overrides, in order of precedence
	explicitBody       *service.InterpolatedString
	explicitMultiparts []MultipartExpressions

	fs        fs.FS
	reqSigner RequestSigner

	url              *service.InterpolatedString
	host             *service.InterpolatedString
	verb             string
	headers          map[string]*service.InterpolatedString
	metaInsertFilter *service.MetadataFilter
}

// RequestOpt represents a customisation of a request creator.
type RequestOpt func(r *RequestCreator)

// RequestCreatorFromOldConfig creates a new request creator from an old struct
// style config. Eventually I'd like to phase these out for the more dynamic
// service style parses, but it'll take a while so we have this for now.
func RequestCreatorFromOldConfig(conf OldConfig, mgr *service.Resources, opts ...RequestOpt) (*RequestCreator, error) {
	r := &RequestCreator{
		fs:               mgr.FS(),
		url:              conf.URL,
		reqSigner:        conf.Auth.Sign,
		verb:             conf.Verb,
		headers:          conf.Headers,
		metaInsertFilter: conf.Metadata,
	}
	for _, opt := range opts {
		opt(r)
	}
	for k, v := range r.headers {
		if strings.EqualFold(k, "host") {
			r.host = v
			delete(r.headers, k)
			break
		}
	}
	return r, nil
}

// WithExplicitBody modifies the request creator to instead only use input
// reference messages for headers and metadata, and use the expression for
// creating a body.
func WithExplicitBody(e *service.InterpolatedString) RequestOpt {
	if e == nil {
		e, _ = service.NewInterpolatedString("")
	}
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

func (r *RequestCreator) bodyFromExplicit(refBatch service.MessageBatch) (body io.Reader, overrideContentType string, err error) {
	if _, exists := r.headers["Content-Type"]; !exists {
		overrideContentType = "application/octet-stream"
	}
	var bBytes []byte
	if bBytes, err = refBatch.TryInterpolatedBytes(0, r.explicitBody); err != nil {
		return
	}
	body = bytes.NewBuffer(bBytes)
	return
}

func (r *RequestCreator) bodyFromExplicitMultipart(refBatch service.MessageBatch) (body io.Reader, overrideContentType string, err error) {
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	for _, v := range r.explicitMultiparts {
		mh := make(textproto.MIMEHeader)
		var cTypeStr, cDispStr string
		if cTypeStr, err = refBatch.TryInterpolatedString(0, v.ContentType); err != nil {
			err = fmt.Errorf("content-type interpolation error: %w", err)
			return
		}
		if cDispStr, err = refBatch.TryInterpolatedString(0, v.ContentDisposition); err != nil {
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
		if partBytes, err = refBatch.TryInterpolatedBytes(0, v.Body); err != nil {
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

func (r *RequestCreator) body(refBatch service.MessageBatch) (body io.Reader, overrideContentType string, err error) {
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
		var bodyBytes []byte
		if bodyBytes, err = refBatch[0].AsBytes(); err != nil {
			return
		}
		body = bytes.NewBuffer(bodyBytes)
		return
	}

	// More than one message in the batch, create a multipart message by
	// default.
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)

	for i, p := range refBatch {
		contentType := "application/octet-stream"
		if v, exists := r.headers["Content-Type"]; exists {
			if contentType, err = refBatch.TryInterpolatedString(i, v); err != nil {
				err = fmt.Errorf("content-type interpolation error: %w", err)
				return
			}
		}

		headers := textproto.MIMEHeader{
			"Content-Type": []string{contentType},
		}
		_ = r.metaInsertFilter.WalkMut(p, func(k string, v any) error {
			headers[k] = append(headers[k], value.IToString(v))
			return nil
		})

		var part io.Writer
		if part, err = writer.CreatePart(headers); err != nil {
			return
		}

		var pBytes []byte
		if pBytes, err = p.AsBytes(); err != nil {
			return
		}
		if _, err = io.Copy(part, bytes.NewReader(pBytes)); err != nil {
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
func (r *RequestCreator) Create(refBatch service.MessageBatch) (req *http.Request, err error) {
	var overrideContentType string
	var body io.Reader
	if body, overrideContentType, err = r.body(refBatch); err != nil {
		return
	}

	var urlStr string
	if urlStr, err = refBatch.TryInterpolatedString(0, r.url); err != nil {
		err = fmt.Errorf("url interpolation error: %w", err)
		return
	}
	if req, err = http.NewRequest(r.verb, urlStr, body); err != nil {
		return
	}

	for k, v := range r.headers {
		var hStr string
		if hStr, err = refBatch.TryInterpolatedString(0, v); err != nil {
			err = fmt.Errorf("header '%v' interpolation error: %w", k, err)
			return
		}
		req.Header.Add(k, hStr)
	}
	if len(refBatch) > 0 {
		_ = r.metaInsertFilter.WalkMut(refBatch[0], func(k string, v any) error {
			req.Header.Add(k, value.IToString(v))
			return nil
		})
	}

	if r.host != nil {
		if req.Host, err = refBatch.TryInterpolatedString(0, r.host); err != nil {
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
