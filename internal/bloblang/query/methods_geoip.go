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
			switch t := v.(type) {
			case []byte:
				ip = net.ParseIP(string(t))
			case string:
				ip = net.ParseIP(t)
			default:
				return nil, NewTypeError(v, ValueString)
			}
			// TODO: ip will be 'nil' if invalid. Need to handle
			geoip_city, err := db.City(ip)
			if err != nil {
				return nil, err
			}
			return geoip_city, nil
		}, nil
	},
)