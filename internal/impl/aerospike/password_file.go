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
	"fmt"
	"os"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// applyPasswordFile loads the password from passwordFile when auth uses one.
// PKI ignores the file and reports that through ignored. Internal and external
// auth take either the inline password or the file. A username with neither,
// and a file whose first line is empty, are rejected. The file contents are
// not included in errors.
func applyPasswordFile(policy *as.ClientPolicy, passwordFile string) (ignored bool, err error) {
	if policy.AuthMode == as.AuthModePKI {
		return passwordFile != "", nil
	}
	if passwordFile != "" && policy.Password != "" {
		return false, fmt.Errorf("field '%s': set '%s' or '%s', not both", aoFieldCredentials, aoFieldCredentialsPassword, aoFieldCredentialsPasswordFile)
	}
	if passwordFile != "" {
		password, readErr := passwordFromFile(passwordFile)
		if readErr != nil {
			return false, readErr
		}
		policy.Password = password
		return false, nil
	}
	if policy.User != "" && policy.Password == "" {
		return false, fmt.Errorf("field '%s': set '%s' or '%s'", aoFieldCredentials, aoFieldCredentialsPassword, aoFieldCredentialsPasswordFile)
	}
	return false, nil
}

// passwordFromFile returns the first line. A trailing newline is dropped.
// Trailing spaces on that line are kept. Later lines are ignored.
func passwordFromFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("field '%s.%s': cannot read %s: %w", aoFieldCredentials, aoFieldCredentialsPasswordFile, path, err)
	}
	s := string(b)
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		s = s[:i]
	}
	if s == "" {
		return "", fmt.Errorf("field '%s.%s': %s is empty", aoFieldCredentials, aoFieldCredentialsPasswordFile, path)
	}
	return s, nil
}
