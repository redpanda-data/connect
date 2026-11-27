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

// cipherSuiteIDs maps configured IANA names to the suites crypto/tls will
// offer. An empty list leaves the suites already selected by the tls field.
// Matching ignores case. The order of the result does not matter: Go ignores it.
func cipherSuiteIDs(names []string) ([]uint16, error) {
	if len(names) == 0 {
		return nil, nil
	}
	supported := append(tls.CipherSuites(), tls.InsecureCipherSuites()...)
	seen := make(map[uint16]struct{}, len(names))
	out := make([]uint16, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("field '%s' contains an empty cipher name", aoFieldCiphers)
		}
		suite := cipherSuiteByName(supported, name)
		if suite == nil {
			return nil, fmt.Errorf("field '%s': unknown cipher %q", aoFieldCiphers, name)
		}
		if tls13Only(suite) {
			return nil, fmt.Errorf("field '%s': %s is a TLS 1.3 cipher suite, which the Go client always offers and does not accept in this list", aoFieldCiphers, name)
		}
		if _, dup := seen[suite.ID]; dup {
			continue
		}
		seen[suite.ID] = struct{}{}
		out = append(out, suite.ID)
	}
	return out, nil
}

// tls13Only reports whether Go offers the suite for TLS 1.3 and no earlier
// version. crypto/tls rejects those IDs in Config.CipherSuites.
func tls13Only(suite *tls.CipherSuite) bool {
	if len(suite.SupportedVersions) == 0 {
		return false
	}
	for _, version := range suite.SupportedVersions {
		if version != tls.VersionTLS13 {
			return false
		}
	}
	return true
}

// insecureCipherNames returns the IANA names of ids that Go marks insecure.
func insecureCipherNames(ids []uint16) []string {
	if len(ids) == 0 {
		return nil
	}
	byID := make(map[uint16]string)
	for _, suite := range tls.InsecureCipherSuites() {
		byID[suite.ID] = suite.Name
	}
	var names []string
	for _, id := range ids {
		name, exists := byID[id]
		if !exists {
			continue
		}
		names = append(names, name)
	}
	return names
}

func cipherSuiteByName(supported []*tls.CipherSuite, name string) *tls.CipherSuite {
	for _, suite := range supported {
		if strings.EqualFold(suite.Name, name) {
			return suite
		}
	}
	return nil
}
