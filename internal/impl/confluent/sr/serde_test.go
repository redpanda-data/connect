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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/sr"
)

func TestUpdateIDRoundtrip(t *testing.T) {
	dummyData := `{"foo": "bar"}`
	dummyID := 42

	tests := []struct {
		name       string
		msg        []byte
		id         int
		errUpdate  string
		errExtract string
	}{
		{
			name: "succeeds round trip",
			msg:  append(make([]byte, 5), []byte(dummyData)...),
			id:   dummyID,
		},
		{
			name:       "fails to update message if it's too small",
			msg:        make([]byte, 3),
			errUpdate:  "message is empty or too small",
			errExtract: "5 byte header for value is missing or does not have 0 magic byte",
		},
		{
			name:       "fails to extract ID from invalid message",
			msg:        []byte("foobar"),
			errUpdate:  "serialization format version number 102 not supported",
			errExtract: "5 byte header for value is missing or does not have 0 magic byte",
		},
	}

	var ch sr.ConfluentHeader
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := UpdateID(test.msg, test.id)
			if test.errUpdate == "" {
				assert.NoError(t, err)
			} else {
				assert.Contains(t, err.Error(), test.errUpdate)
			}

			extractedID, _, err := ch.DecodeID(test.msg)
			if test.errExtract == "" {
				assert.NoError(t, err)
				assert.Equal(t, dummyID, extractedID)
			} else {
				assert.Contains(t, err.Error(), test.errExtract)
			}
		})
	}
}
