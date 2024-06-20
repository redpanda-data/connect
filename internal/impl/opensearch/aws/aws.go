// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
