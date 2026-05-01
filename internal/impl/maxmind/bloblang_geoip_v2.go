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

package maxmind

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"

	"github.com/oschwald/geoip2-golang"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func registerMaxmindMethodSpecV2(name, entity string, fn func(*geoip2.Reader, net.IP) (any, error)) {
	bloblangv2.MustRegisterMethod(name,
		bloblangv2.NewPluginSpec().
			Category("GeoIP").
			Description(fmt.Sprintf("Looks up an IP address against a https://www.maxmind.com/en/home[MaxMind database file^] and, if found, returns an object describing the %v associated with it.", entity)).
			Param(bloblangv2.NewStringParam("path").Description("A path to an mmdb (maxmind) file.")),
		func(args *bloblangv2.ParsedParams) (bloblangv2.Method, error) {
			path, err := args.GetString("path")
			if err != nil {
				return nil, err
			}
			db, err := geoip2.Open(path)
			if err != nil {
				return nil, err
			}
			return func(v any) (any, error) {
				s, err := valueAsString(v)
				if err != nil {
					return nil, err
				}
				ip := net.ParseIP(s)
				if ip == nil {
					return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", s)
				}
				out, err := fn(db, ip)
				if err != nil {
					return nil, err
				}
				jBytes, err := json.Marshal(out)
				if err != nil {
					return nil, err
				}
				dec := json.NewDecoder(bytes.NewReader(jBytes))
				dec.UseNumber()
				var gV any
				err = dec.Decode(&gV)
				return gV, err
			}, nil
		})
}

func init() {
	registerMaxmindMethodSpecV2("geoip_city", "city", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.City(ip)
	})
	registerMaxmindMethodSpecV2("geoip_country", "country", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.Country(ip)
	})
	registerMaxmindMethodSpecV2("geoip_asn", "ASN", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.ASN(ip)
	})
	registerMaxmindMethodSpecV2("geoip_enterprise", "enterprise", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.Enterprise(ip)
	})
	registerMaxmindMethodSpecV2("geoip_anonymous_ip", "anonymous IP", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.AnonymousIP(ip)
	})
	registerMaxmindMethodSpecV2("geoip_connection_type", "connection type", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.ConnectionType(ip)
	})
	registerMaxmindMethodSpecV2("geoip_domain", "domain", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.Domain(ip)
	})
	registerMaxmindMethodSpecV2("geoip_isp", "ISP", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.ISP(ip)
	})
}

// valueAsString preserves V1's StringMethod-leniency for the maxmind plugin
// receivers. V2's StringMethod is strict and would reject byte-slice receivers
// — connect mappings are commonly authored as `meta("ip").geoip_city(...)`
// where `meta(...)` may yield a string or bytes depending on the caller.
func valueAsString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	}
	return "", fmt.Errorf("expected string or bytes, got %T", v)
}
