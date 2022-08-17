package aws

import (
	"net/http"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	baws "github.com/benthosdev/benthos/v4/internal/impl/aws"
	"github.com/benthosdev/benthos/v4/internal/impl/opensearch"
)

func init() {
	opensearch.AWSOptFn = func(roadtripper http.RoundTripper, conf output.OpenSearchConfig) (*opensearch.Transport, error) {
		if !conf.AWS.Enabled {
			return nil, nil
		}
		tsess, err := baws.GetSessionFromConf(conf.AWS.Config)
		if err != nil {
			return nil, err
		}
		signingClient := NewV4SigningClientWithHTTPClient(roadtripper, tsess.Config.Credentials, conf.AWS.Region)
		return signingClient, nil
	}
}

// NewV4SigningClientWithHTTPClient returns an *http.Client that will sign all requests with AWS V4 Signing.
func NewV4SigningClientWithHTTPClient(transport http.RoundTripper, creds *credentials.Credentials, region string) *opensearch.Transport {

	t := &opensearch.Transport{
		Transport: transport,
		Creds:     creds,
		Signer:    v4.NewSigner(creds),
		Region:    region,
	}
	return t
}
