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
	"crypto/x509"
	"fmt"
	"math/big"
	"strings"
)

// applyRevokedCertificates rejects a handshake whose peer chain contains a
// listed serial. An empty list leaves the CA check unchanged. This is a
// serial deny-list, not a CRL or OCSP check.
func applyRevokedCertificates(cfg *tls.Config, names []string) error {
	serials, err := revokedSerials(names)
	if err != nil || len(serials) == 0 {
		return err
	}
	next := rejectRevokedSerials(serials)
	if prev := cfg.VerifyPeerCertificate; prev != nil {
		cfg.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
			if err := prev(raw, chains); err != nil {
				return err
			}
			return next(raw, chains)
		}
		return nil
	}
	cfg.VerifyPeerCertificate = next
	return nil
}

func revokedSerials(names []string) ([]*big.Int, error) {
	if len(names) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(names))
	out := make([]*big.Int, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("field '%s' contains an empty serial number", aoFieldRevokeCertificates)
		}
		n, err := parseRevokedSerial(name)
		if err != nil {
			return nil, err
		}
		key := n.Text(10)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, n)
	}
	return out, nil
}

// parseRevokedSerial accepts a decimal integer, or hex when the value has a
// 0x prefix. The digit check is what rejects a bad value. SetString cannot
// fail once that check has passed.
func parseRevokedSerial(name string) (*big.Int, error) {
	base := 10
	digits := name
	hex := strings.HasPrefix(name, "0x") || strings.HasPrefix(name, "0X")
	if hex {
		base = 16
		digits = name[2:]
	}
	if !serialDigits(digits, hex) {
		return nil, fmt.Errorf("field '%s': serial %q must be a decimal integer or a hex integer with a 0x prefix", aoFieldRevokeCertificates, name)
	}
	n, _ := new(big.Int).SetString(digits, base)
	return n, nil
}

func serialDigits(digits string, hex bool) bool {
	if digits == "" {
		return false
	}
	for _, r := range digits {
		switch {
		case r >= '0' && r <= '9':
		case hex && (r >= 'a' && r <= 'f' || r >= 'A' && r <= 'F'):
		default:
			return false
		}
	}
	return true
}

func rejectRevokedSerials(serials []*big.Int) func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		if len(verifiedChains) > 0 {
			for _, chain := range verifiedChains {
				for _, cert := range chain {
					if err := serialRevoked(serials, cert); err != nil {
						return err
					}
				}
			}
			return nil
		}
		for _, raw := range rawCerts {
			cert, err := x509.ParseCertificate(raw)
			if err != nil {
				return fmt.Errorf("tls: cannot parse peer certificate: %w", err)
			}
			if err := serialRevoked(serials, cert); err != nil {
				return err
			}
		}
		return nil
	}
}

func serialRevoked(serials []*big.Int, cert *x509.Certificate) error {
	for _, serial := range serials {
		if cert.SerialNumber != nil && cert.SerialNumber.Cmp(serial) == 0 {
			return fmt.Errorf("peer certificate serial %s is revoked", cert.SerialNumber.Text(10))
		}
	}
	return nil
}
