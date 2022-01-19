package query

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/oschwald/geoip2-golang"
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"geoip_city",
		"Takes an IP address as a string and returns a geoip2 City struct and/or an error. ",
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

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var ip net.IP

			// Cast to string type
			switch t := v.(type) {
			case []byte:
				ip = net.ParseIP(string(t))
			case string:
				ip = net.ParseIP(t)
			default:
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			if ip == nil {
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			geoipCity, err := db.City(ip)
			if err != nil {
				return nil, err
			}
			tmp, err := json.Marshal(geoipCity)
			if err != nil {
				return nil, err
			}
			var p map[string]interface{}
			err = json.Unmarshal(tmp, &p)
			if err != nil {
				return nil, err
			}
			return p, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"geoip_country",
		"Takes an IP address as a string and returns a geoip2 Country struct and/or an error. ",
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

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var ip net.IP

			// Cast to string type
			switch t := v.(type) {
			case []byte:
				ip = net.ParseIP(string(t))
			case string:
				ip = net.ParseIP(t)
			default:
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			if ip == nil {
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			geoipCountry, err := db.Country(ip)
			tmp, err := json.Marshal(geoipCountry)
			if err != nil {
				return nil, err
			}
			var p map[string]interface{}
			err = json.Unmarshal(tmp, &p)
			if err != nil {
				return nil, err
			}
			return p, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"geoip_asn",
		"Takes an IP address as a string and returns a geoip2 ASN struct and/or an error. ",
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

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var ip net.IP

			// Cast to string type
			switch t := v.(type) {
			case []byte:
				ip = net.ParseIP(string(t))
			case string:
				ip = net.ParseIP(t)
			default:
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			if ip == nil {
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			geoipASN, err := db.ASN(ip)
			tmp, err := json.Marshal(geoipASN)
			if err != nil {
				return nil, err
			}
			var p map[string]interface{}
			err = json.Unmarshal(tmp, &p)
			if err != nil {
				return nil, err
			}
			return p, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"geoip_enterprise",
		"Takes an IP address as a string and returns a geoip2 Enterprise struct and/or an error. ",
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

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var ip net.IP

			// Cast to string type
			switch t := v.(type) {
			case []byte:
				ip = net.ParseIP(string(t))
			case string:
				ip = net.ParseIP(t)
			default:
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			if ip == nil {
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			geoipEnterprise, err := db.Enterprise(ip)
			tmp, err := json.Marshal(geoipEnterprise)
			if err != nil {
				return nil, err
			}
			var p map[string]interface{}
			err = json.Unmarshal(tmp, &p)
			if err != nil {
				return nil, err
			}
			return p, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"geoip_anonymous_ip",
		"Takes an IP address as a string and returns a geoip2 AnonymousIP struct and/or an error. ",
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

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var ip net.IP

			// Cast to string type
			switch t := v.(type) {
			case []byte:
				ip = net.ParseIP(string(t))
			case string:
				ip = net.ParseIP(t)
			default:
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			if ip == nil {
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			geoipAnonymousIP, err := db.AnonymousIP(ip)
			tmp, err := json.Marshal(geoipAnonymousIP)
			if err != nil {
				return nil, err
			}
			var p map[string]interface{}
			err = json.Unmarshal(tmp, &p)
			if err != nil {
				return nil, err
			}
			return p, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"geoip_connection_type",
		"Takes an IP address as a string and returns a geoip2 ConnectionType struct and/or an error. ",
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

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var ip net.IP

			// Cast to string type
			switch t := v.(type) {
			case []byte:
				ip = net.ParseIP(string(t))
			case string:
				ip = net.ParseIP(t)
			default:
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			if ip == nil {
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			geoipConnectionType, err := db.ConnectionType(ip)
			tmp, err := json.Marshal(geoipConnectionType)
			if err != nil {
				return nil, err
			}
			var p map[string]interface{}
			err = json.Unmarshal(tmp, &p)
			if err != nil {
				return nil, err
			}
			return p, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"geoip_domain",
		"Takes an IP address as a string and returns a geoip2 Domain struct and/or an error. ",
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

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var ip net.IP

			// Cast to string type
			switch t := v.(type) {
			case []byte:
				ip = net.ParseIP(string(t))
			case string:
				ip = net.ParseIP(t)
			default:
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			if ip == nil {
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			geoipDomain, err := db.Domain(ip)
			tmp, err := json.Marshal(geoipDomain)
			if err != nil {
				return nil, err
			}
			var p map[string]interface{}
			err = json.Unmarshal(tmp, &p)
			if err != nil {
				return nil, err
			}
			return p, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"geoip_isp",
		"Takes an IP address as a string and returns a geoip2 ISP struct and/or an error. ",
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

		return func(v interface{}, ctx FunctionContext) (interface{}, error) {
			var ip net.IP

			// Cast to string type
			switch t := v.(type) {
			case []byte:
				ip = net.ParseIP(string(t))
			case string:
				ip = net.ParseIP(t)
			default:
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			if ip == nil {
				return nil, fmt.Errorf("value %v does not appear to be a valid v4 or v6 IP address", v)
			}

			geoipISP, err := db.ISP(ip)
			tmp, err := json.Marshal(geoipISP)
			if err != nil {
				return nil, err
			}
			var p map[string]interface{}
			err = json.Unmarshal(tmp, &p)
			if err != nil {
				return nil, err
			}
			return p, nil
		}, nil
	},
)
