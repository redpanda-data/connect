package cosmosdb

import "github.com/benthosdev/benthos/v4/public/service"

// PartitionKeyType represents the type of the partition_key field.
type PartitionKeyType string

const (
	// PartitionKeyString partition_key contains a string.
	PartitionKeyString PartitionKeyType = "string"
	// PartitionKeyBool partition_key contains a boolean.
	PartitionKeyBool PartitionKeyType = "bool"
	// PartitionKeyNumber partition_key contains a number.
	PartitionKeyNumber PartitionKeyType = "number"
)

// OperationType represents the CosmosDB operation type.
type OperationType string

const (
	// OperationCreate CosmosDB Create operation.
	OperationCreate OperationType = "Create"
	// OperationDelete CosmosDB Delete operation.
	OperationDelete OperationType = "Delete"
	// OperationReplace CosmosDB Replace operation.
	OperationReplace OperationType = "Replace"
	// OperationUpsert CosmosDB Upsert operation.
	OperationUpsert OperationType = "Upsert"
)

// NewConfigSpec constructs a new CosmosDB ConfigSpec with common config fields
func NewConfigSpec(description string) *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Azure").
		Description(description + `
### CosmosDB Emulator

If you wish to run the CosmosDB emulator that is referenced in the documentation [here](https://learn.microsoft.com/en-us/azure/cosmos-db/linux-emulator),
the following Docker command should do the trick:

` + "```shell" + `
> docker run --rm -it -p 8081:8081 --name=cosmosdb -e AZURE_COSMOS_EMULATOR_PARTITION_COUNT=2 -e AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator
` + "```" + `

Note: ` + "`AZURE_COSMOS_EMULATOR_PARTITION_COUNT`" + ` controls how many database, container and partition combinations are
supported. The bigger the value, the longer it takes for the container to start up.

Additionally, instead of installing the container self-signed certificate which is exposed via
` + "`https://localhost:8081/_explorer/emulator.pem`" + `, you can run [mitmproxy](https://mitmproxy.org/) like so:

` + "```shell" + `
> mitmproxy -k --mode "reverse:https://localhost:8081"
` + "```" + `

Then you can access the CosmosDB UI via ` + "`http://localhost:8080/_explorer/index.html`" + ` and use
` + "`http://localhost:8080`" + ` as the CosmosDB endpoint.
`).
		Field(service.NewStringField("endpoint").Description("CosmosDB endpoint.").Example("https://localhost:8081")).
		Field(service.NewStringField("account_key").Description("Account key.").Example("C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")).
		Field(service.NewStringField("database_id").Description("Database ID.").Example("TestDB")).
		Field(service.NewStringField("container_id").Description("Container ID.").Example("TestContainer")).
		Field(service.NewInterpolatedStringField("partition_key").Description("Partition key.").Example(`${! json("foobar") }`)).
		Field(service.NewStringAnnotatedEnumField("partition_key_type", map[string]string{
			string(PartitionKeyString): "partition_key contains a string.",
			string(PartitionKeyBool):   "partition_key contains a boolean.",
			string(PartitionKeyNumber): "partition_key contains a number.",
		}).Description("Partition key type.").Default(string(PartitionKeyString)))
}
