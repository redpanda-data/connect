// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package splunk

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestOutputEventFormat checks that a config without the optional event_* fields starts, and that the output sends
// each message in the shape the "Event format" section of the spec describes.
func TestOutputEventFormat(t *testing.T) {
	tests := []struct {
		name  string
		extra string
		msg   string
		want  string
	}{
		{
			name: "top-level event key is sent unchanged",
			msg:  `{"event":"e","host":"h","source":"s","sourcetype":"st","index":"i"}`,
			want: `{"event":"e","host":"h","source":"s","sourcetype":"st","index":"i"}`,
		},
		{
			name: "other message is wrapped under event",
			msg:  `{"index":"web","msg":"hi"}`,
			want: `{"event":{"index":"web","msg":"hi"}}`,
		},
		{
			name:  "event_index sets the index of a wrapped message",
			extra: "event_index: main\n",
			msg:   `{"index":"web","msg":"hi"}`,
			want:  `{"event":{"index":"web","msg":"hi"},"index":"main"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bodies := make(chan string, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				bodies <- string(b)
			}))
			t.Cleanup(srv.Close)

			pConf, err := outputSpec().ParseYAML("url: \""+srv.URL+"\"\ntoken: tok\n"+tt.extra, nil)
			require.NoError(t, err)

			o, err := outputFromParsed(pConf, service.MockResources().Logger())
			require.NoError(t, err)

			msg := service.NewMessage([]byte(tt.msg))
			require.NoError(t, o.WriteBatch(t.Context(), service.MessageBatch{msg}))

			assert.JSONEq(t, tt.want, <-bodies)
		})
	}
}
