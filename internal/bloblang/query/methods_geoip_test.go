package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGeoIPCity(t *testing.T) {
	testCases := []struct {
		name   string
		method string
		target interface{}
		args   []interface{}
		exp    string
	}{
		{
			name:   "geoip city",
			method: "geoip_city",
			target: "81.2.69.192",
			args:   []interface{}{"../../impl/maxmind/GeoIP2-City-Test.mmdb"},
			exp:    "London",
		},
		{
			name:   "geoip country",
			method: "geoip_country",
			target: "2001:220::80",
			args:   []interface{}{"../../impl/maxmind/GeoIP2-Country-Test.mmdb"},
			exp:    "Republik Korea",
		},
		{
			name:   "geoip ASN",
			method: "geoip_asn",
			target: "214.0.0.0",
			args:   []interface{}{"../../impl/maxmind/GeoLite2-ASN-Test.mmdb"},
			exp:    "DoD Network Information Center",
		},
		{
			name:   "geoip enterprise",
			method: "geoip_enterprise",
			target: "149.101.100.0",
			args:   []interface{}{"../../impl/maxmind/GeoIP2-Enterprise-Test.mmdb"},
			exp:    "Verizon Wireless",
		},
		{
			name:   "geoip anonymous IP",
			method: "geoip_anonymous_ip",
			target: "81.2.69.0",
			args:   []interface{}{"../../impl/maxmind/GeoIP2-Anonymous-IP-Test.mmdb"},
			exp:    "IsTorExitNode",
		},
		{
			name:   "geoip connection type",
			method: "geoip_connection_type",
			target: "207.179.48.0",
			args:   []interface{}{"../../impl/maxmind/GeoIP2-Connection-Type-Test.mmdb"},
			exp:    "Cellular",
		},
		{
			name:   "geoip domain",
			method: "geoip_domain",
			target: "89.95.192.0",
			args:   []interface{}{"../../impl/maxmind/GeoIP2-Domain-Test.mmdb"},
			exp:    "bbox.fr",
		},
		{
			name:   "geoip ISP",
			method: "geoip_isp",
			target: "12.87.120.0",
			args:   []interface{}{"../../impl/maxmind/GeoIP2-ISP-Test.mmdb"},
			exp:    "AT&T Services",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			targetClone := IClone(test.target)
			argsClone := IClone(test.args).([]interface{})

			fn, err := InitMethodHelper(test.method, NewLiteralFunction("", targetClone), argsClone...)
			require.NoError(t, err)

			res, err := fn.Exec(FunctionContext{
				Maps:     map[string]Function{},
				Index:    0,
				MsgBatch: nil,
			})
			require.NoError(t, err)
			assert.Contains(t, fmt.Sprintf("%v", res), test.exp)
			assert.Equal(t, test.target, targetClone)
			assert.Equal(t, test.args, argsClone)
		})
	}
}
