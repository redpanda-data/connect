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
	"math"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/go-viper/mapstructure/v2"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/azure/cosmosdb"
)

const (
	cdbiFieldQuery       = "query"
	cdbiFieldArgsMapping = "args_mapping"
	cdbiFieldBatchCount  = "batch_count"
)

func cosmosDBInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Beta().
		Categories("Azure").
		Version("v4.25.0").
		Summary(`Executes a SQL query against https://learn.microsoft.com/en-us/azure/cosmos-db/introduction[Azure CosmosDB^] and creates a batch of messages from each page of items.`).
		Description(`
== Cross-partition queries

Cross-partition queries are currently not supported by the underlying driver. For every query, the PartitionKey values must be known in advance and specified in the config. https://github.com/Azure/azure-sdk-for-go/issues/18578#issuecomment-1222510989[See details^].
`+cosmosdb.CredentialsDocs+cosmosdb.MetadataDocs).
		Footnotes(cosmosdb.EmulatorDocs).
		Fields(cosmosdb.ContainerClientConfigFields()...).
		Field(cosmosdb.PartitionKeysField(true)).
		Field(service.NewStringField(cdbiFieldQuery).Description("The query to execute").Example(`SELECT c.foo FROM testcontainer AS c WHERE c.bar = "baz" AND c.timestamp < @timestamp`)).
		Field(service.NewBloblangField(cdbiFieldArgsMapping).
			Description("A xref:guides:bloblang/about.adoc[Bloblang mapping] that, for each message, creates a list of arguments to use with the query.").Optional().Example(`root = [
  { "Name": "@name", "Value": "benthos" },
]`)).
		Field(service.NewIntField(cdbiFieldBatchCount).
			Description(`The maximum number of messages that should be accumulated into each batch. Use '-1' specify dynamic page size.`).
			Default(-1).
			Advanced().LintRule(`root = if this < -1 || this == 0 || this > `+strconv.Itoa(math.MaxInt32)+` { [ "`+cdbiFieldBatchCount+` must be must be > 0 and smaller than `+strconv.Itoa(math.MaxInt32)+` or -1." ] }`)).
		Field(service.NewAutoRetryNacksToggleField()).
		LintRule("root = []"+cosmosdb.CommonLintRules).
		Example("Query container", "Execute a parametrized SQL query to select documents from a container.", `
input:
  azure_cosmosdb:
    endpoint: http://localhost:8080
    account_key: C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
    database: blobbase
    container: blobfish
    partition_keys_map: root = "AbyssalPlain"
    query: SELECT * FROM blobfish AS b WHERE b.species = @species
    args_mapping: |
      root = [
          { "Name": "@species", "Value": "smooth-head" },
      ]
`)
}

func init() {
	service.MustRegisterBatchInput("azure_cosmosdb", cosmosDBInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		r, err := newCosmosDBReaderFromParsed(conf, mgr)
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacksBatchedToggled(conf, r)
	})
}

//------------------------------------------------------------------------------

type cosmosDBReader struct {
	// State
	pager *runtime.Pager[azcosmos.QueryItemsResponse]
}

func newCosmosDBReaderFromParsed(conf *service.ParsedConfig, _ *service.Resources) (*cosmosDBReader, error) {
	containerClient, err := cosmosdb.ContainerClientFromParsed(conf)
	if err != nil {
		return nil, err
	}

	partitionKeysMapping, err := conf.FieldBloblang(cosmosdb.FieldPartitionKeysMap)
	if err != nil {
		return nil, err
	}

	pkQueryResult, err := partitionKeysMapping.Query(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate partition keys values: %s", err)
	}

	// TODO: Enable support for hierarchical / empty Partition Keys this when the following issues are addressed:
	// - https://github.com/Azure/azure-sdk-for-go/issues/18578
	// - https://github.com/Azure/azure-sdk-for-go/issues/21063
	if pkValuesList, ok := pkQueryResult.([]any); ok {
		if len(pkValuesList) != 1 {
			return nil, errors.New("only one partition key is supported")
		}
		pkQueryResult = pkValuesList[0]
	}

	pkValue, err := cosmosdb.GetTypedPartitionKeyValue(pkQueryResult)
	if err != nil {
		return nil, err
	}

	query, err := conf.FieldString(cdbiFieldQuery)
	if err != nil {
		return nil, err
	}

	var args []azcosmos.QueryParameter
	if conf.Contains(cdbiFieldArgsMapping) {
		argsMapping, err := conf.FieldBloblang(cdbiFieldArgsMapping)
		if err != nil {
			return nil, err
		}

		argsConf, err := argsMapping.Query(nil)
		if err != nil {
			return nil, fmt.Errorf("error evaluating %s: %s", cdbiFieldArgsMapping, err)
		}

		if err := mapstructure.Decode(argsConf, &args); err != nil {
			return nil, fmt.Errorf("error converting %s to CosmosDB parameters: %s", cdbiFieldArgsMapping, err)
		}
	}

	batchCount, err := conf.FieldInt(cdbiFieldBatchCount)
	if err != nil {
		return nil, err
	}
	if batchCount < -1 || batchCount == 0 || batchCount > math.MaxInt32 {
		return nil, fmt.Errorf("%s must be > 0 and smaller than %d or -1, got %d", cdbiFieldBatchCount, math.MaxInt32, batchCount)
	}

	return &cosmosDBReader{
		pager: containerClient.NewQueryItemsPager(query, pkValue, &azcosmos.QueryOptions{
			PageSizeHint:    int32(batchCount),
			QueryParameters: args,
		}),
	}, nil
}

func (*cosmosDBReader) Connect(context.Context) error { return nil }

func (c *cosmosDBReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if !c.pager.More() {
		return nil, nil, service.ErrEndOfInput
	}

	queryResponse, err := c.pager.NextPage(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get next page of query response: %s", err)
	}

	resBatch := make(service.MessageBatch, 0, len(queryResponse.Items))
	for _, item := range queryResponse.Items {
		m := service.NewMessage(item)
		m.MetaSetMut("activity_id", queryResponse.ActivityID)
		m.MetaSetMut("request_charge", queryResponse.RequestCharge)

		resBatch = append(resBatch, m)
	}

	return resBatch, func(context.Context, error) error { return nil }, nil
}

func (*cosmosDBReader) Close(context.Context) error { return nil }
