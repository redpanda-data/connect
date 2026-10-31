// Copyright 2026 Redpanda Data, Inc.
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

package aerospike

import (
	"crypto/tls"
	"fmt"
	"strings"
)

const (
	protocolTLS12 = "TLSV1.2"
	protocolTLS13 = "TLSV1.3"
)

// applyTLSProtocols restricts cfg to the named versions. An empty list leaves
// the versions already selected by the tls field, which offers TLS 1.2 and
// TLS 1.3. Naming both does the same. crypto/tls cannot disable one TLS 1.3
// cipher suite, so this list selects protocol versions only.
func applyTLSProtocols(cfg *tls.Config, names []string) error {
	if len(names) == 0 {
		return nil
	}
	var want12, want13 bool
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			return fmt.Errorf("field '%s' contains an empty protocol name", aoFieldProtocols)
		}
		switch strings.ToUpper(name) {
		case protocolTLS12:
			want12 = true
		case protocolTLS13:
			want13 = true
		default:
			return fmt.Errorf("field '%s': unknown protocol %q; supported values are TLSv1.2 and TLSv1.3", aoFieldProtocols, name)
		}
	}
	switch {
	case want12 && !want13:
		cfg.MaxVersion = tls.VersionTLS12
	case want13 && !want12:
		cfg.MinVersion = tls.VersionTLS13
	}
	return nil
}
