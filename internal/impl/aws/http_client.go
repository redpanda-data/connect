// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package aws

import (
	"net/http"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// httpClientFromConfig can be used to customize the HTTP client
// that will be used by the aws session. Currently used to set
// the tcp_user_timeout setting, which only applies to Linux systems.
func httpClientFromConfig(p *service.ParsedConfig) *http.Client {
	if tcpUserTimeout, err := p.FieldDuration("tcp_user_timeout"); err == nil && tcpUserTimeout > 0 {
		httpClient, err := newHTTPClientWithTCPUserTimeout(tcpUserTimeout)
		if err != nil {
			return nil
		}
		// On non linux systems the newHTTPClientWithTCPUSerTimeout returns nil,nil
		// so it will fall back to the default client.
		return httpClient
	}
	return nil
}
