package aws

import (
	"context"

	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v3/signer/awsv2"

	"github.com/redpanda-data/benthos/v4/public/service"

	baws "github.com/redpanda-data/connect/v4/internal/impl/aws"
	"github.com/redpanda-data/connect/v4/internal/impl/opensearch"
)

func init() {
	opensearch.AWSOptFn = func(conf *service.ParsedConfig, osconf *opensearchapi.Config) error {
		if enabled, _ := conf.FieldBool(opensearch.ESOFieldAWSEnabled); !enabled {
			return nil
		}

		tsess, err := baws.GetSession(context.TODO(), conf)
		if err != nil {
			return err
		}

		signer, err := awsv2.NewSigner(tsess)
		if err != nil {
			return err
		}

		osconf.Client.Signer = signer
		return nil
	}
}
