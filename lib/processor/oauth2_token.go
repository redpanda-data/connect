package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/coreos/go-oidc"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/oauth2/clientcredentials"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeOAuth2Token] = TypeSpec{
		constructor: NewOAuth2Token,
		Categories: []Category{
			CategoryIntegration,
		},
		Summary: `Retrieves an OAuth2 token using the ` + "`client credentials`" + ` token flow`,
		Description: `
This processor will return a JSON object containing an OAuth2 token or optionally within a marshalled JSON in a metadata key.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("endpoint", "The OAuth2 endpoint."),
			docs.FieldCommon("client_id", "ClientID is the application's ID."),
			docs.FieldCommon("client_secret", "ClientSecret is the application's secret."),
			docs.FieldCommon("scopes", "Scope specifies optional requested permissions."),
			docs.FieldAdvanced("metadata_key", "An optional metadata key to store the OAuth2Token token"),
			partsFieldSpec,
		},
		Examples: []docs.AnnotatedExample{
			{
				Title: "Retrieve an OAuth2 token using the ` + \"`client credentials`\" + ` token flow",
				Summary: `
We can retrieve the token, which is returned in a JSON object if no ` + "`metadata_key`" + ` is set,
using a branch processor and mapping the token in its ` + "`result_map`" + `. If the ` + "`metadata_key`" + ` is set, the token 
is returned in a marshalled JSON.

The resulting OAuth2 token object has the fields ` + "`access_token`" + `, ` + "`token_type`" + ` and ` + "`expiry`" + `.

In order to have a different endpoints, client IDs or secrets for each object you can use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.
`,
				Config: `
pipeline:
  processors:
  - branch:
      processors:
      - oauth2_token:
          endpoint: https://my-identiy-server.foo.com
          client_id: my-client-id
          client_secret: my-client-secret
          scopes:
          - my-scope
      result_map: |-
        token = this
  - bloblang: |
      root = this
      access_token = token.access_token
      token_type = token.token_type
      expiry = token.expiry
`,
			},
		},
		Footnotes: ``,
	}
}

//------------------------------------------------------------------------------

// OAuth2TokenConfig contains configuration fields for the OAuth2Token processor.
type OAuth2TokenConfig struct {
	Parts        []int    `json:"parts" yaml:"parts"`
	ClientID     string   `json:"client_id" yaml:"client_id"`
	CLientSecret string   `json:"client_secret" yaml:"client_secret"`
	MetadataKey  string   `json:"metadata_key" yaml:"metadata_key"`
	Scopes       []string `json:"scopes" yaml:"scopes"`
	Endpoint     string   `json:"endpoint" yaml:"endpoint"`
}

// NewOAuth2TokenConfig returns a OAuth2TokenConfig with default values.
func NewOAuth2TokenConfig() OAuth2TokenConfig {
	return OAuth2TokenConfig{
		Parts:        []int{},
		Endpoint:     "",
		ClientID:     "",
		CLientSecret: "",
		MetadataKey:  "",
		Scopes:       []string{},
	}
}

//------------------------------------------------------------------------------

// OAuth2Token is a processor retrieves an OAuth2Token token
type OAuth2Token struct {
	parts []int

	conf  OAuth2TokenConfig
	log   log.Modular
	stats metrics.Type

	endpoint     field.Expression
	clientID     field.Expression
	clientSecret field.Expression
	metadataKey  field.Expression
}

// NewOAuth2Token returns a OAuth2Token processor.
func NewOAuth2Token(
	conf Config, _ types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	a := &OAuth2Token{
		conf:  conf.OAuth2Token,
		log:   log,
		stats: stats,
	}
	var err error
	if a.endpoint, err = bloblang.NewField(conf.OAuth2Token.Endpoint); err != nil {
		return nil, fmt.Errorf("failed to parse endpoint expression: %v", err)
	}
	if a.clientID, err = bloblang.NewField(conf.OAuth2Token.ClientID); err != nil {
		return nil, fmt.Errorf("failed to parse client_id expression: %v", err)
	}
	if a.clientSecret, err = bloblang.NewField(conf.OAuth2Token.CLientSecret); err != nil {
		return nil, fmt.Errorf("failed to parse client_secret expression: %v", err)
	}

	return a, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (o *OAuth2Token) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		ctx := context.Background()
		provider, err := oidc.NewProvider(ctx, o.endpoint.String(index, msg))
		if err != nil {
			return err
		}
		config := clientcredentials.Config{
			ClientID:     o.clientID.String(index, msg),
			ClientSecret: o.clientSecret.String(index, msg),
			TokenURL:     provider.Endpoint().TokenURL,
			Scopes:       o.conf.Scopes,
		}
		token, err := config.Token(ctx)
		if err != nil {
			return err
		}
		c, _ := gabs.New().Set(token)
		if len(o.conf.MetadataKey) > 0 {
			tk, _ := c.MarshalJSON()
			part.Metadata().Set(o.conf.MetadataKey, string(tk))
		} else {
			part.Set(c.Bytes())
		}
		return nil
	}

	IteratePartsWithSpan(TypeCache, o.parts, newMsg, proc)

	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (o *OAuth2Token) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (o *OAuth2Token) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
