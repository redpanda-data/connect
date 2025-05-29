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

package azure

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/azure/cosmosdb"
)

const (
	cdboFieldBatching = "batching"
)

func cosmosDBOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Azure").
		Version("v4.25.0").
		Summary("Creates or updates messages as JSON documents in https://learn.microsoft.com/en-us/azure/cosmos-db/introduction[Azure CosmosDB^].").
		Description(`
When creating documents, each message must have the `+"`id`"+` property (case-sensitive) set (or use `+"`auto_id: true`"+`). It is the unique name that identifies the document, that is, no two documents share the same `+"`id`"+` within a logical partition. The `+"`id`"+` field must not exceed 255 characters. https://learn.microsoft.com/en-us/rest/api/cosmos-db/documents[See details^].

The `+"`partition_keys`"+` field must resolve to the same value(s) across the entire message batch.
`+cosmosdb.CredentialsDocs+cosmosdb.BatchingDocs+service.OutputPerformanceDocs(true, true)).
		Footnotes(cosmosdb.EmulatorDocs).
		Fields(cosmosdb.ContainerClientConfigFields()...).
		Field(cosmosdb.PartitionKeysField(false)).
		Fields(cosmosdb.CRUDFields(false)...).
		Field(service.NewBatchPolicyField(cdboFieldBatching)).
		Field(service.NewOutputMaxInFlightField()).
		LintRule("root = []"+cosmosdb.CommonLintRules+cosmosdb.CRUDLintRules).
		Example("Create documents", "Create new documents in the `blobfish` container with partition key `/habitat`.", `
output:
  azure_cosmosdb:
    endpoint: http://localhost:8080
    account_key: C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
    database: blobbase
    container: blobfish
    partition_keys_map: root = json("habitat")
    operation: Create
`).
		Example("Patch documents", "Execute the Patch operation on documents from the `blobfish` container.", `
output:
  azure_cosmosdb:
    endpoint: http://localhost:8080
    account_key: C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
    database: testdb
    container: blobfish
    partition_keys_map: root = json("habitat")
    item_id: ${! json("id") }
    operation: Patch
    patch_operations:
      # Add a new /diet field
      - operation: Add
        path: /diet
        value_map: root = json("diet")
      # Remove the first location from the /locations array field
      - operation: Remove
        path: /locations/0
      # Add new location at the end of the /locations array field
      - operation: Add
        path: /locations/-
        value_map: root = "Challenger Deep"
`)
}

func init() {
	service.MustRegisterBatchOutput("azure_cosmosdb", cosmosDBOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(cdboFieldBatching); err != nil {
				return
			}
			output, err = newCosmosDBWriterFromParsed(conf, mgr.Logger())
			return
		})
}

//------------------------------------------------------------------------------

type cosmosDBWriter struct {
	logger *service.Logger

	// Config
	cosmosdb.CRUDConfig

	// State
	containerClient *azcosmos.ContainerClient
}

func newCosmosDBWriterFromParsed(conf *service.ParsedConfig, logger *service.Logger) (*cosmosDBWriter, error) {
	containerClient, err := cosmosdb.ContainerClientFromParsed(conf)
	if err != nil {
		return nil, err
	}

	crudConfig, err := cosmosdb.CRUDConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	return &cosmosDBWriter{
		CRUDConfig:      crudConfig,
		containerClient: containerClient,
		logger:          logger,
	}, nil
}

//------------------------------------------------------------------------------

func (*cosmosDBWriter) Connect(context.Context) error { return nil }

func (c *cosmosDBWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	resp, err := cosmosdb.ExecMessageBatch(ctx, batch, c.containerClient, c.CRUDConfig, false)
	if err != nil {
		return fmt.Errorf("failed to execute transactional batch: %s", err)
	}

	c.logger.Debugf("Transactional batch executed successfully. ActivityID %s consumed %f RU", resp.ActivityID, resp.RequestCharge)

	if !resp.Success {
		for idx, opRes := range resp.OperationResults {
			c.logger.Errorf("Rejected batch element %d with status: %d", idx, opRes.StatusCode)
		}

		return errors.New("failed to write message batch")
	}

	return nil
}

func (*cosmosDBWriter) Close(context.Context) error { return nil }
