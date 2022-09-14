package maxmind

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"

	"github.com/oschwald/geoip2-golang"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func registerMaxmindMethodSpec(name, entity string, fn func(*geoip2.Reader, net.IP) (any, error)) {
	if err := bloblang.RegisterMethodV2(name,
		bloblang.NewPluginSpec().
			Experimental().
			Category(query.MethodCategoryGeoIP).
			Description(fmt.Sprintf("Looks up an IP address against a [MaxMind database file](https://www.maxmind.com/en/home) and, if found, returns an object describing the %v associated with it.", entity)).
			Param(bloblang.NewStringParam("path").Description("A path to an mmdb (maxmind) file.")),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			path, err := args.GetString("path")
			if err != nil {
				return nil, err
			}
			db, err := geoip2.Open(path)
			if err != nil {
				return nil, err
			}
			return bloblang.StringMethod(func(s string) (any, error) {
				ip := net.ParseIP(s)
				if ip == nil {
					return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", s)
				}
				v, err := fn(db, ip)
				if err != nil {
					return nil, err
				}
				jBytes, err := json.Marshal(v)
				if err != nil {
					return nil, err
				}
				dec := json.NewDecoder(bytes.NewReader(jBytes))
				dec.UseNumber()
				var gV any
				err = dec.Decode(&gV)
				return gV, err
			}), nil
		}); err != nil {
		panic(err)
	}
}

func init() {
	registerMaxmindMethodSpec("geoip_city", "city", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.City(ip)
	})

	registerMaxmindMethodSpec("geoip_country", "country", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.Country(ip)
	})

	registerMaxmindMethodSpec("geoip_asn", "ASN", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.ASN(ip)
	})

	registerMaxmindMethodSpec("geoip_enterprise", "enterprise", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.Enterprise(ip)
	})

	registerMaxmindMethodSpec("geoip_anonymous_ip", "anonymous IP", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.AnonymousIP(ip)
	})

	registerMaxmindMethodSpec("geoip_connection_type", "connection type", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.ConnectionType(ip)
	})

	registerMaxmindMethodSpec("geoip_domain", "domain", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.Domain(ip)
	})

	registerMaxmindMethodSpec("geoip_isp", "ISP", func(db *geoip2.Reader, ip net.IP) (any, error) {
		return db.ISP(ip)
	})
}
