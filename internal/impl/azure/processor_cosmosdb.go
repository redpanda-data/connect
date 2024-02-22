package azure

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/benthosdev/benthos/v4/internal/impl/azure/cosmosdb"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	cdbpFieldEnableContentResponseOnWrite = "enable_content_response_on_write"
)

func cosmosDBProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Azure").
		Version("v4.25.0").
		Summary("Creates or updates messages as JSON documents in [Azure CosmosDB](https://learn.microsoft.com/en-us/azure/cosmos-db/introduction).").
		Description(`
When creating documents, each message must have the `+"`id`"+` property (case-sensitive) set (or use `+"`auto_id: true`"+`). It is the unique name that identifies the document, that is, no two documents share the same `+"`id`"+` within a logical partition. The `+"`id`"+` field must not exceed 255 characters. More details can be found [here](https://learn.microsoft.com/en-us/rest/api/cosmos-db/documents).

The `+"`partition_keys`"+` field must resolve to the same value(s) across the entire message batch.
`+cosmosdb.CredentialsDocs+cosmosdb.MetadataDocs+cosmosdb.BatchingDocs).
		Footnotes(cosmosdb.EmulatorDocs).
		Fields(cosmosdb.ContainerClientConfigFields()...).
		Field(cosmosdb.PartitionKeysField(false)).
		Fields(cosmosdb.CRUDFields(true)...).
		Field(service.NewBoolField(cdbpFieldEnableContentResponseOnWrite).Description("Enable content response on write operations. To save some bandwidth, set this to false if you don't need to receive the updated message(s) from the server, in which case the processor will not modify the content of the messages which are fed into it. Applies to every operation except Read.").Default(true).Advanced()).
		LintRule("root = []"+cosmosdb.CommonLintRules+cosmosdb.CRUDLintRules).
		Example("Patch documents", "Query documents from a container and patch them.", `
input:
  azure_cosmosdb:
    endpoint: http://localhost:8080
    account_key: C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
    database: blobbase
    container: blobfish
    partition_keys_map: root = "AbyssalPlain"
    query: SELECT * FROM blobfish

  processors:
    - mapping: |
        root = ""
        meta habitat = json("habitat")
        meta id = this.id
    - azure_cosmosdb:
        endpoint: http://localhost:8080
        account_key: C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
        database: testdb
        container: blobfish
        partition_keys_map: root = json("habitat")
        item_id: ${! meta("id") }
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
        # Return the updated document
        enable_content_response_on_write: true
`)
}

func init() {
	err := service.RegisterBatchProcessor(
		"azure_cosmosdb", cosmosDBProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newCosmosDBProcessorFromParsed(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type cosmosDBProcessor struct {
	logger *service.Logger

	// Config
	cosmosdb.CRUDConfig
	enableContentResponseOnWrite bool

	// State
	containerClient *azcosmos.ContainerClient
}

func newCosmosDBProcessorFromParsed(conf *service.ParsedConfig, logger *service.Logger) (*cosmosDBProcessor, error) {
	containerClient, err := cosmosdb.ContainerClientFromParsed(conf)
	if err != nil {
		return nil, err
	}

	crudConfig, err := cosmosdb.CRUDConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	c := cosmosDBProcessor{
		CRUDConfig:      crudConfig,
		containerClient: containerClient,
		logger:          logger,
	}

	if c.enableContentResponseOnWrite, err = conf.FieldBool(cdbpFieldEnableContentResponseOnWrite); err != nil {
		return nil, err
	}

	return &c, nil
}

//------------------------------------------------------------------------------

func (c *cosmosDBProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	resp, err := cosmosdb.ExecMessageBatch(ctx, batch, c.containerClient, c.CRUDConfig, c.enableContentResponseOnWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to execute transactional batch: %s", err)
	}

	c.logger.Debugf("Transactional batch executed successfully. ActivityID %s consumed %f RU", resp.ActivityID, resp.RequestCharge)

	batch = batch.Copy()
	for idx, opRes := range resp.OperationResults {
		p := batch[idx]
		if resp.Success {
			if c.Operation == cosmosdb.OperationRead || c.enableContentResponseOnWrite {
				p.SetBytes(opRes.ResourceBody)
			}
		} else {
			p.SetError(fmt.Errorf("rejected batch element %d with status: %d", idx, opRes.StatusCode))
		}

		p.MetaSetMut("activity_id", resp.ActivityID)
		p.MetaSetMut("request_charge", opRes.RequestCharge)
	}

	return []service.MessageBatch{batch}, nil
}

func (c *cosmosDBProcessor) Close(ctx context.Context) error { return nil }
