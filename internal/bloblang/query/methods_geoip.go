package query

import (
	"net"

	"github.com/oschwald/geoip2-golang"
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"geoip_city", "",
	).Param(ParamString("path", "Path to city mmdb file")),
	func(args *ParsedParams) (simpleMethod, error) {
		// TODO: Ensure path is a static. Unsure how to accomplish this
		path, err := args.FieldString("path")
		if err != nil {
			return nil, err
		}
		db, err := geoip2.Open(path)
		if err != nil {
			return nil, err
		}

		// TODO db.Close(), finalizer?

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var ip net.IP

			// Cast to string type
			switch t := v.(type) {
			case []byte:
				ip = net.ParseIP(string(t))
			case string:
				ip = net.ParseIP(t)
			default:
				return nil, NewTypeError(v, ValueString)
			}

			// TODO: ip will be 'nil' if not a valid v4/v6 address. How to handle?
			// It currently winds up reporting this when invalid:
			//    "failed assignment (line 2): field `this.ip`: IP passed to Lookup cannot be nil"
			// should we add context? IE what the lookup value is?
			geoip_city, err := db.City(ip)
			if err != nil {
				return nil, err
			}
			return geoip_city, nil
		}, nil
	},
)

// TODO: geoip_country
// TODO: geoip_asn
// TODO: geoip_enterprise
// TODO: geoip_anonymous_ip
// TODO: geoip_connection_type
// TODO: geoip_domain
// TODO: geoip_isp