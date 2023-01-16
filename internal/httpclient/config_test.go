package httpclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestNewStyleConfigs(t *testing.T) {
	fromDefault := func(fn func(o *OldConfig)) OldConfig {
		c := NewOldConfig()
		fn(&c)
		return c
	}

	tests := []struct {
		name         string
		verbOverride string
		forOutput    bool
		inputYAML    string
		outputConf   OldConfig
	}{
		{
			name: "basic fields",
			inputYAML: `
url: example.com/foo1
verb: PUT
headers:
  foo1: bar1
  foo2: bar2
`,
			outputConf: fromDefault(func(o *OldConfig) {
				o.URL = "example.com/foo1"
				o.Verb = "PUT"
				o.Headers = map[string]string{
					"foo1": "bar1",
					"foo2": "bar2",
				}
			}),
		},
		{
			name: "verb default",
			inputYAML: `
url: example.com/foo2
rate_limit: nah
`,
			verbOverride: "GET",
			outputConf: fromDefault(func(o *OldConfig) {
				o.URL = "example.com/foo2"
				o.Verb = "GET"
				o.RateLimit = "nah"
			}),
		},
		{
			name: "code overrides",
			inputYAML: `
url: example.com/foo3
successful_on: [ 1, 2, 3 ]
backoff_on: [ 4, 5, 6 ]
drop_on: [ 7, 8, 9 ]
`,
			outputConf: fromDefault(func(o *OldConfig) {
				o.URL = "example.com/foo3"
				o.SuccessfulOn = []int{1, 2, 3}
				o.BackoffOn = []int{4, 5, 6}
				o.DropOn = []int{7, 8, 9}
			}),
		},
		{
			name: "tls and auth overrides",
			inputYAML: `
url: example.com/foo4
tls:
  enabled: true
  root_cas: meow
oauth:
  consumer_key: woof
basic_auth:
  username: quack
jwt:
  headers:
    this: tweet
oauth2:
  client_key: moo
`,
			outputConf: fromDefault(func(o *OldConfig) {
				o.URL = "example.com/foo4"
				o.TLS.Enabled = true
				o.TLS.RootCAs = "meow"
				o.OAuth.ConsumerKey = "woof"
				o.BasicAuth.Username = "quack"
				o.JWT.Headers = map[string]any{
					"this": "tweet",
				}
				o.OAuth2.ClientKey = "moo"
			}),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			verb := "POST"
			if test.verbOverride != "" {
				verb = test.verbOverride
			}

			spec := service.NewConfigSpec().Field(ConfigField(verb, test.forOutput))
			parsed, err := spec.ParseYAML(test.inputYAML, nil)
			require.NoError(t, err)

			raw, err := parsed.FieldAny()
			require.NoError(t, err)

			conf, err := ConfigFromAny(raw)
			require.NoError(t, err)

			assert.Equal(t, test.outputConf, conf)
		})
	}

}
