package aws

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/impl/kafka"
	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/twmb/franz-go/pkg/sasl"
	kaws "github.com/twmb/franz-go/pkg/sasl/aws"

	sess "github.com/benthosdev/benthos/v4/internal/impl/aws"
)

func init() {
	kafka.AWSSASLFromConfigFn = func(c *service.ParsedConfig) (sasl.Mechanism, error) {
		awsSession, err := sess.GetSession(c.Namespace("aws"))
		if err != nil {
			return nil, err
		}

		creds := awsSession.Config.Credentials
		return kaws.ManagedStreamingIAM(func(ctx context.Context) (kaws.Auth, error) {
			val, err := creds.GetWithContext(ctx)
			if err != nil {
				return kaws.Auth{}, err
			}
			return kaws.Auth{
				AccessKey:    val.AccessKeyID,
				SecretKey:    val.SecretAccessKey,
				SessionToken: val.SessionToken,
			}, nil
		}), nil
	}
}
