// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr"
)

func TestSchemaRegistry(t *testing.T) {
	dummySchema := sr.SubjectSchema{
		Subject: "foo",
		Version: 1,
		ID:      1,
		Schema:  sr.Schema{Schema: `{"name":"foo", "type": "string"}`},
	}
	dummySchemaWithRef := sr.SubjectSchema{
		Subject: "bar",
		Version: 1,
		ID:      2,
		Schema: sr.Schema{
			Schema:     `{"name":"bar",  "type": "record", "fields":[{"name":"data", "type": "foo"}]}}`,
			References: []sr.SchemaReference{{Name: "foo", Subject: "foo", Version: 1}},
		},
	}
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.EscapedPath()
			var output any
			switch path {
			case "/mode":
				output = map[string]string{"mode": "READWRITE"}
			case "/subjects":
				output = []string{"foo", "bar"}
			case "/subjects/foo/versions", "/subjects/bar/versions":
				switch r.Method {
				case http.MethodGet:
					output = []int{1}
				case http.MethodPost:
					if path == "/subjects/foo/versions" {
						output = dummySchema
					} else {
						output = dummySchemaWithRef
					}
				default:
					http.Error(w, fmt.Sprintf("method not supported: %s", r.Method), http.StatusBadRequest)
					return
				}
			case "/subjects/foo/versions/1":
				output = dummySchema
			case "/subjects/bar/versions/1":
				output = dummySchemaWithRef
			case "/schemas/ids/1":
				output = dummySchema
			case "/schemas/ids/2":
				output = dummySchemaWithRef
			case "/schemas/ids/1/versions":
				output = []map[string]any{{"subject": "foo", "version": 1}}
			case "/schemas/ids/2/versions":
				output = []map[string]any{{"subject": "bar", "version": 1}}
			default:
				http.Error(w, fmt.Sprintf("path not found: %s", path), http.StatusNotFound)
				return
			}
			b, err := json.Marshal(output)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if len(b) == 0 {
				http.NotFound(w, r)
				return
			}
			_, err = w.Write(b)
			require.NoError(t, err)
		}),
	)
	t.Cleanup(ts.Close)

	mgr := service.MockResources()

	inputConf, err := schemaRegistryInputSpec().ParseYAML(fmt.Sprintf(`
url: %s
subject: foo
`, ts.URL), nil)
	require.NoError(t, err)

	reader, err := inputFromParsed(inputConf, mgr)
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), 1*time.Second)
	t.Cleanup(done)
	err = reader.Connect(ctx)
	require.NoError(t, err)

	var messages []*service.Message
	for {
		msg, _, err := reader.Read(ctx)
		if err == service.ErrEndOfInput {
			break
		}
		require.NoError(t, err)

		messages = append(messages, msg)
	}

	outputConf, err := schemaRegistryOutputSpec().ParseYAML(fmt.Sprintf(`
url: %s
subject: ${! @schema_registry_subject }
`, ts.URL), nil)
	require.NoError(t, err)

	writer, err := outputFromParsed(outputConf, mgr)
	require.NoError(t, err)

	err = writer.Connect(ctx)
	require.NoError(t, err)

	for _, msg := range messages {
		err := writer.Write(ctx, msg)
		require.NoError(t, err)
	}

	// Ensure that the written schemas are correctly returned.
	// TODO: Use a secondary test server for the writer so we can check that they're actually written.
	destID, err := writer.GetDestinationSchemaID(ctx, 1, "foo")
	require.NoError(t, err)
	assert.Equal(t, 1, destID)
	destID, err = writer.GetDestinationSchemaID(ctx, 2, "bar")
	require.NoError(t, err)
	assert.Equal(t, 2, destID)
}
