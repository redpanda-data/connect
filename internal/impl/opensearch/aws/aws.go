package aws

import (
	"github.com/olivere/elastic/v7"
	aws "github.com/olivere/elastic/v7/aws/v4"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	baws "github.com/benthosdev/benthos/v4/internal/impl/aws"
	"github.com/benthosdev/benthos/v4/internal/impl/opensearch"
)

func init() {
	opensearch.AWSOptFn = func(conf output.OpenSearchConfig) ([]elastic.ClientOptionFunc, error) {
		if !conf.AWS.Enabled {
			return nil, nil
		}
		tsess, err := baws.GetSessionFromConf(conf.AWS.Config)
		if err != nil {
			return nil, err
		}
		signingClient := aws.NewV4SigningClient(tsess.Config.Credentials, conf.AWS.Region)
		return []elastic.ClientOptionFunc{elastic.SetHttpClient(signingClient)}, nil
	}
}
