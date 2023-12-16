package httpclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestNewStyleConfigs(t *testing.T) {
	tests := []struct {
		name         string
		verbOverride string
		forOutput    bool
		inputYAML    string
		validator    func(t *testing.T, c *OldConfig)
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
			validator: func(t *testing.T, o *OldConfig) {
				sURL, _ := o.URL.Static()
				assert.Equal(t, "example.com/foo1", sURL)
				assert.Equal(t, "PUT", o.Verb)

				sHeaders := map[string]string{}
				for k, v := range o.Headers {
					sHeaders[k], _ = v.Static()
				}
				assert.Equal(t, map[string]string{
					"foo1": "bar1",
					"foo2": "bar2",
				}, sHeaders)
			},
		},
		{
			name: "verb default",
			inputYAML: `
url: example.com/foo2
rate_limit: nah
`,
			verbOverride: "GET",
			validator: func(t *testing.T, o *OldConfig) {
				sURL, _ := o.URL.Static()
				assert.Equal(t, "example.com/foo2", sURL)
				assert.Equal(t, "GET", o.Verb)
				assert.Equal(t, "nah", o.RateLimit)
			},
		},
		{
			name: "code overrides",
			inputYAML: `
url: example.com/foo3
successful_on: [ 1, 2, 3 ]
backoff_on: [ 4, 5, 6 ]
drop_on: [ 7, 8, 9 ]
`,
			validator: func(t *testing.T, o *OldConfig) {
				sURL, _ := o.URL.Static()
				assert.Equal(t, "example.com/foo3", sURL)
				assert.Equal(t, []int{1, 2, 3}, o.SuccessfulOn)
				assert.Equal(t, []int{4, 5, 6}, o.BackoffOn)
				assert.Equal(t, []int{7, 8, 9}, o.DropOn)
			},
		},
		{
			name: "tls and auth overrides",
			inputYAML: `
url: example.com/foo4
tls:
  enabled: true
  skip_cert_verify: true
oauth:
  enabled: true
  consumer_key: woof
basic_auth:
  enabled: true
  username: quack
jwt:
  enabled: true
  headers:
    this: tweet
oauth2:
  enabled: true
  client_key: moo
`,
			validator: func(t *testing.T, o *OldConfig) {
				sURL, _ := o.URL.Static()
				assert.Equal(t, "example.com/foo4", sURL)
				assert.True(t, o.TLSEnabled)
				assert.True(t, o.TLSConf.InsecureSkipVerify)

				assert.True(t, o.Auth.BasicAuth.Enabled)
				assert.Equal(t, "quack", o.Auth.BasicAuth.Username)

				assert.True(t, o.Auth.OAuth.Enabled)
				assert.Equal(t, "woof", o.Auth.OAuth.ConsumerKey)

				assert.True(t, o.Auth.JWT.Enabled)
				assert.Equal(t, map[string]any{
					"this": "tweet",
				}, o.Auth.JWT.Headers)

				assert.True(t, o.OAuth2.Enabled)
				assert.Equal(t, "moo", o.OAuth2.ClientKey)
			},
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

			conf, err := ConfigFromParsed(parsed)
			require.NoError(t, err)

			test.validator(t, &conf)
		})
	}
}
