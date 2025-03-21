// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package rpingress_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/rpingress"
)

func TestHTTPSinglePayloads(t *testing.T) {
	t.Parallel()

	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mux := mux.NewRouter()

	pConf, err := rpingress.InputSpec().ParseYAML(`
address: 0.0.0.0:1234 # Unused
path: /testpost
`, nil)
	require.NoError(t, err)

	h, err := rpingress.InputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, h.RegisterCustomMux(tCtx, mux))

	server := httptest.NewServer(mux)
	defer server.Close()

	// Test both single and multipart messages.
	for i := 0; i < 100; i++ {
		go func() {
			batch, aFn, err := h.ReadBatch(tCtx)
			require.NoError(t, err)

			for _, m := range batch {
				mBytes, err := m.AsBytes()
				require.NoError(t, err)

				m.SetBytes(bytes.Replace(mBytes, []byte("test"), []byte("response"), -1))
			}

			require.NoError(t, batch.AddSyncResponse())
			require.NoError(t, aFn(tCtx, nil))
		}()

		// Send it as single message
		res, err := http.Post(
			server.URL+"/testpost",
			"application/octet-stream",
			bytes.NewBufferString(fmt.Sprintf("test%v", i)),
		)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		resBytes, err := io.ReadAll(res.Body)
		require.NoError(t, err)

		assert.Equal(t, fmt.Sprintf("response%v", i), string(resBytes))
	}
}

func TestHTTPBatchPayloads(t *testing.T) {
	t.Parallel()

	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mux := mux.NewRouter()

	pConf, err := rpingress.InputSpec().ParseYAML(`
address: 0.0.0.0:1234 # Unused
path: /testpost
`, nil)
	require.NoError(t, err)

	h, err := rpingress.InputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, h.RegisterCustomMux(tCtx, mux))

	server := httptest.NewServer(mux)
	defer server.Close()

	// Test both single and multipart messages.
	for i := 0; i < 100; i++ {
		go func() {
			batch, aFn, err := h.ReadBatch(tCtx)
			require.NoError(t, err)

			for _, m := range batch {
				mBytes, err := m.AsBytes()
				require.NoError(t, err)

				m.SetBytes(bytes.Replace(mBytes, []byte("test"), []byte("response"), -1))
			}

			require.NoError(t, batch.AddSyncResponse())
			require.NoError(t, aFn(tCtx, nil))
		}()

		hdr, body, err := createMultipart([]string{
			fmt.Sprintf("test 0 %v", i),
			fmt.Sprintf("test 1 %v", i),
			fmt.Sprintf("test 2 %v", i),
		}, "application/octet-stream")
		require.NoError(t, err)

		res, err := http.Post(server.URL+"/testpost", hdr, bytes.NewReader(body))
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		act, err := readMultipart(res)
		require.NoError(t, err)
		assert.Equal(t, []string{
			fmt.Sprintf("response 0 %v", i),
			fmt.Sprintf("response 1 %v", i),
			fmt.Sprintf("response 2 %v", i),
		}, act)
	}
}

func createMultipart(payloads []string, contentType string) (hdr string, bodyBytes []byte, err error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	for i := 0; i < len(payloads) && err == nil; i++ {
		var part io.Writer
		if part, err = writer.CreatePart(textproto.MIMEHeader{
			"Content-Type": []string{contentType},
		}); err == nil {
			_, err = io.Copy(part, bytes.NewReader([]byte(payloads[i])))
		}
	}

	if err != nil {
		return "", nil, err
	}

	writer.Close()
	return writer.FormDataContentType(), body.Bytes(), nil
}

func readMultipart(res *http.Response) ([]string, error) {
	var params map[string]string
	var err error
	if contentType := res.Header.Get("Content-Type"); contentType != "" {
		if _, params, err = mime.ParseMediaType(contentType); err != nil {
			return nil, err
		}
	}

	var buffer bytes.Buffer
	var output []string

	mr := multipart.NewReader(res.Body, params["boundary"])
	var bufferIndex int64
	for {
		var p *multipart.Part
		if p, err = mr.NextPart(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		var bytesRead int64
		if bytesRead, err = buffer.ReadFrom(p); err != nil {
			return nil, err
		}

		output = append(output, string(buffer.Bytes()[bufferIndex:bufferIndex+bytesRead]))
		bufferIndex += bytesRead
	}

	return output, nil
}
