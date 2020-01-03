package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeS3] = TypeSpec{
		constructor: NewAmazonS3,
		Description: `
Sends message parts as objects to an Amazon S3 bucket. Each object is uploaded
with the path specified with the ` + "`path`" + ` field.

In order to have a different path for each object you should use function
interpolations described [here](../config_interpolation.md#functions), which are
calculated per message of a batch.

The fields ` + "`content_type`, `content_encoding` and `storage_class`" + ` can
also be set dynamically using function interpolation.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).`,
		Async: true,
	}
}

//------------------------------------------------------------------------------

// NewAmazonS3 creates a new AmazonS3 output type.
func NewAmazonS3(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	sthree, err := writer.NewAmazonS3(conf.S3, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.S3.MaxInFlight == 1 {
		return NewWriter(
			TypeS3, sthree, log, stats,
		)
	}
	return NewAsyncWriter(
		TypeS3, conf.S3.MaxInFlight, sthree, log, stats,
	)
}

//------------------------------------------------------------------------------
