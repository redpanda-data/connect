package maxmind

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestGeoIPCity(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		exp   any
	}{
		{
			name:  "geoip city",
			input: `root = "81.2.69.192".geoip_city("./testdata/GeoIP2-City-Test.mmdb").City.Names.en`,
			exp:   "London",
		},
		{
			name:  "geoip country",
			input: `root = "2001:220::80".geoip_country("./testdata/GeoIP2-Country-Test.mmdb").Country.Names.en`,
			exp:   "South Korea",
		},
		{
			name:  "geoip ASN",
			input: `root = "214.0.0.0".geoip_asn("./testdata/GeoLite2-ASN-Test.mmdb").AutonomousSystemOrganization`,
			exp:   "DoD Network Information Center",
		},
		{
			name:  "geoip enterprise",
			input: `root = "149.101.100.0".geoip_enterprise("./testdata/GeoIP2-Enterprise-Test.mmdb").Traits.ISP`,
			exp:   "Verizon Wireless",
		},
		{
			name:  "geoip anonymous IP",
			input: `root = "81.2.69.0".geoip_anonymous_ip("./testdata/GeoIP2-Anonymous-IP-Test.mmdb").IsTorExitNode`,
			exp:   true,
		},
		{
			name:  "geoip connection type",
			input: `root = "207.179.48.0".geoip_connection_type("./testdata/GeoIP2-Connection-Type-Test.mmdb").ConnectionType`,
			exp:   "Cellular",
		},
		{
			name:  "geoip domain",
			input: `root = "89.95.192.0".geoip_domain("./testdata/GeoIP2-Domain-Test.mmdb").Domain`,
			exp:   "bbox.fr",
		},
		{
			name:  "geoip ISP",
			input: `root = "12.87.120.0".geoip_isp("./testdata/GeoIP2-ISP-Test.mmdb").ISP`,
			exp:   "AT&T Services",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			exec, err := bloblang.Parse(test.input)
			require.NoError(t, err)

			res, err := exec.Query(nil)
			require.NoError(t, err)

			assert.Equal(t, test.exp, res)
		})
	}
}
