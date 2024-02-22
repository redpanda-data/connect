package azure

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/impl/azure/cosmosdb"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	cdboFieldBatching = "batching"
)

func cosmosDBOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Azure").
		Version("v4.25.0").
		Summary("Creates or updates messages as JSON documents in [Azure CosmosDB](https://learn.microsoft.com/en-us/azure/cosmos-db/introduction).").
		Description(output.Description(true, true, `
When creating documents, each message must have the `+"`id`"+` property (case-sensitive) set (or use `+"`auto_id: true`"+`). It is the unique name that identifies the document, that is, no two documents share the same `+"`id`"+` within a logical partition. The `+"`id`"+` field must not exceed 255 characters. More details can be found [here](https://learn.microsoft.com/en-us/rest/api/cosmos-db/documents).

The `+"`partition_keys`"+` field must resolve to the same value(s) across the entire message batch.
`+cosmosdb.CredentialsDocs+cosmosdb.BatchingDocs)).
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
	err := service.RegisterBatchOutput("azure_cosmosdb", cosmosDBOutputConfig(),
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
	if err != nil {
		panic(err)
	}
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

func (c *cosmosDBWriter) Connect(ctx context.Context) error { return nil }

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

func (c *cosmosDBWriter) Close(ctx context.Context) error { return nil }
