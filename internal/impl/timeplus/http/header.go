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

package http

import (
	"encoding/base64"
	"net/http"
)

// NewHeader creates a standard Timeplus HTTP header.
func NewHeader(apikey, username, password string) http.Header {
	header := http.Header{}

	header.Add("Content-Type", "application/json")

	if len(username)+len(password) > 0 {
		auth := username + ":" + password
		header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(auth)))
	} else if len(apikey) > 0 {
		header.Add("X-Api-Key", apikey)
	}

	return header
}
