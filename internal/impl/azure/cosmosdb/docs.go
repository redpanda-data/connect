package cosmosdb

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	fieldEndpoint         = "endpoint"
	fieldAccountKey       = "account_key"
	fieldConnectionString = "connection_string"
	fieldDatabase         = "database"
	fieldContainer        = "container"
	FieldPartitionKeys    = "partition_keys_map"
	fieldOperation        = "operation"
	fieldPatchOperations  = "patch_operations"
	fieldPatchCondition   = "patch_condition"
	fieldPatchOperation   = "operation"
	fieldPatchPath        = "path"
	fieldPatchValue       = "value_map"
	fieldAutoID           = "auto_id"
	fieldItemID           = "item_id"
)

// OperationType operation type
type OperationType string

const (
	// OperationCreate Create operation
	OperationCreate OperationType = "Create"
	// OperationDelete Delete operation
	OperationDelete OperationType = "Delete"
	// OperationReplace Replace operation
	OperationReplace OperationType = "Replace"
	// OperationUpsert Upsert operation
	OperationUpsert OperationType = "Upsert"
	// OperationRead Read operation
	OperationRead OperationType = "Read"
	// OperationPatch Patch operation
	OperationPatch OperationType = "Patch"
)

type patchOperationType string

const (
	patchOperationAdd       patchOperationType = "Add"
	patchOperationIncrement patchOperationType = "Increment"
	patchOperationRemove    patchOperationType = "Remove"
	patchOperationReplace   patchOperationType = "Replace"
	patchOperationSet       patchOperationType = "Set"
)

type patchOperation struct {
	Operation patchOperationType
	Path      *service.InterpolatedString
	Value     *bloblang.Executor
}

// CRUDConfig contains the configuration fields required for CRUD operations
type CRUDConfig struct {
	PartitionKeys   *bloblang.Executor
	Operation       OperationType
	AutoID          bool
	ItemID          *service.InterpolatedString
	PatchCondition  *service.InterpolatedString
	PatchOperations []patchOperation
}

// CredentialsDocs credentials docs
var CredentialsDocs = `

## Credentials

You can use one of the following authentication mechanisms:

- Set the ` + "`endpoint`" + ` field and the ` + "`account_key`" + ` field
- Set only the ` + "`endpoint`" + ` field to use [DefaultAzureCredential](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#DefaultAzureCredential)
- Set the ` + "`connection_string`" + ` field
`

// MetadataDocs metadata docs
var MetadataDocs = `

## Metadata

This component adds the following metadata fields to each message:
` + "```" + `
- activity_id
- request_charge
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).
`

// BatchingDocs batching docs
var BatchingDocs = `

## Batching

CosmosDB limits the maximum batch size to 100 messages and the payload must not exceed 2MB (details [here](https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits#per-request-limits)).
`

// EmulatorDocs emulator docs
var EmulatorDocs = `

## CosmosDB Emulator

If you wish to run the CosmosDB emulator that is referenced in the documentation [here](https://learn.microsoft.com/en-us/azure/cosmos-db/linux-emulator), the following Docker command should do the trick:

` + "```shell" + `
> docker run --rm -it -p 8081:8081 --name=cosmosdb -e AZURE_COSMOS_EMULATOR_PARTITION_COUNT=10 -e AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator
` + "```" + `

Note: ` + "`AZURE_COSMOS_EMULATOR_PARTITION_COUNT`" + ` controls the number of partitions that will be supported by the emulator. The bigger the value, the longer it takes for the container to start up.

Additionally, instead of installing the container self-signed certificate which is exposed via ` + "`https://localhost:8081/_explorer/emulator.pem`" + `, you can run [mitmproxy](https://mitmproxy.org/) like so:

` + "```shell" + `
> mitmproxy -k --mode "reverse:https://localhost:8081"
` + "```" + `

Then you can access the CosmosDB UI via ` + "`http://localhost:8080/_explorer/index.html`" + ` and use ` + "`http://localhost:8080`" + ` as the CosmosDB endpoint.
`

// CommonLintRules contains the lint rules for common fields
var CommonLintRules = `
let hasEndpoint = this.endpoint.or("") != ""
let hasConnectionString = this.connection_string.or("") != ""

root."-" = if !$hasEndpoint && !$hasConnectionString {
  "Either ` + "`endpoint`" + ` or ` + "`connection_string`" + ` must be set."
}
`

// CRUDLintRules contains the lint rules for CRUD fields
var CRUDLintRules = `
let hasItemID = this.item_id.or("") != ""
let hasPatchOperations = this.patch_operations.length().or(0) > 0
let hasPatchCondition = this.patch_condition.or("") != ""

root."-" = if !$hasItemID && (this.operation == "Replace" || this.operation == "Delete" || this.operation == "Read" || this.operation == "Patch") {
  "The ` + "`item_id`" + ` field must be set for Replace, Delete, Read and Patch operations."
}

root."-" = if this.operation == "Patch" && !$hasPatchOperations {
  "At least one ` + "`patch_operations`" + ` must be set when ` + "`operation: Patch`" + `."
}

root."-" = if $hasPatchCondition && (!$hasPatchOperations || this.operation != "Patch") {
  "The ` + "`patch_condition` " + ` field only applies to ` + "`Patch`" + ` operations and it requires one or more ` + "`patch_operations`" + `."
}

root."-" = if this.operation == "Patch" && this.patch_operations.any(o -> o.operation != "Remove" && o.value_map.or("") == "") {
  "The ` + "`patch_operations` " + "`value_map`" + ` field must be set when ` + "`operation`" + ` is not ` + "`Remove`" + `."
}

root."-" = if this.operation == "Patch" && this.patch_operations.any(o -> o.operation == "Remove" && o.value_map.or("") != "") {
  "The ` + "`patch_operations` " + "`value_map`" + ` field must not be set when ` + "`operation`" + ` is ` + "`Remove`" + `."
}
`

//------------------------------------------------------------------------------

// ContainerClientConfigFields returns the container client config fields
func ContainerClientConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(fieldEndpoint).Description("CosmosDB endpoint.").Optional().Example("https://localhost:8081"),
		service.NewStringField(fieldAccountKey).Description("Account key.").Secret().Optional().Example("C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="),
		service.NewStringField(fieldConnectionString).Description("Connection string.").Secret().Optional().Example("AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;"),
		service.NewStringField(fieldDatabase).Description("Database.").Example("testdb"),
		service.NewStringField(fieldContainer).Description("Container.").Example("testcontainer"),
	}
}

// PartitionKeysField returns the partition keys field definition
func PartitionKeysField(isInputField bool) *service.ConfigField {
	// TODO: Add examples for hierarchical / empty Partition Keys this when the following issues are addressed:
	// - https://github.com/Azure/azure-sdk-for-go/issues/18578
	// - https://github.com/Azure/azure-sdk-for-go/issues/21063
	field := service.NewBloblangField(FieldPartitionKeys).Description("A [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to a single partition key value or an array of partition key values of type string, integer or boolean. Currently, hierarchical partition keys are not supported so only one value may be provided.").Example(`root = "blobfish"`).Example(`root = 41`).Example(`root = true`).Example(`root = null`)

	// Add dynamic examples
	if !isInputField {
		return field.Example(`root = json("blobfish").depth`)
	}
	return field.Example(`root = now().ts_format("2006-01-02")`)
}

// CRUDFields returns the CRUD field definitions
func CRUDFields(hasReadOperation bool) []*service.ConfigField {
	operations := map[string]string{
		string(OperationCreate):  "Create operation.",
		string(OperationDelete):  "Delete operation.",
		string(OperationReplace): "Replace operation.",
		string(OperationUpsert):  "Upsert operation.",
		string(OperationPatch):   "Patch operation.",
	}
	if hasReadOperation {
		operations[string(OperationRead)] = "Read operation."
	}

	return []*service.ConfigField{
		service.NewStringAnnotatedEnumField(fieldOperation, operations).Description("Operation.").Default(string(OperationCreate)),
		service.NewObjectListField(fieldPatchOperations, []*service.ConfigField{
			service.NewStringAnnotatedEnumField(fieldPatchOperation, map[string]string{
				string(patchOperationAdd):       "Add patch operation.",
				string(patchOperationIncrement): "Increment patch operation.",
				string(patchOperationRemove):    "Remove patch operation.",
				string(patchOperationReplace):   "Replace patch operation.",
				string(patchOperationSet):       "Set patch operation.",
			}).Description("Operation.").Default(string(patchOperationAdd)),
			service.NewStringField(fieldPatchPath).Description("Path.").Example("/foo/bar/baz"),
			service.NewBloblangField(fieldPatchValue).Description("A [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to a value of any type that is supported by CosmosDB.").Example(`root = "blobfish"`).Example(`root = 41`).Example(`root = true`).Example(`root = json("blobfish").depth`).Example(`root = [1, 2, 3]`).Optional(),
		}...).Description("Patch operations to be performed when `" + fieldOperation + ": " + string(OperationPatch) + "` .").Optional().Advanced(),
		service.NewInterpolatedStringField(fieldPatchCondition).Description("Patch operation condition.").Optional().Advanced().Example(`from c where not is_defined(c.blobfish)`),
		service.NewBoolField(fieldAutoID).Description("Automatically set the item `id` field to a random UUID v4. If the `id` field is already set, then it will not be overwritten. Setting this to `false` can improve performance, since the messages will not have to be parsed.").Default(true).Advanced(),
		service.NewInterpolatedStringField(fieldItemID).Description("ID of item to replace or delete. Only used by the Replace and Delete operations").Example(`${! json("id") }`).Optional(),
	}
}

// ContainerClientFromParsed creates the container client from a parsed config
func ContainerClientFromParsed(conf *service.ParsedConfig) (*azcosmos.ContainerClient, error) {
	var endpoint string
	var err error
	if conf.Contains(fieldEndpoint) {
		if endpoint, err = conf.FieldString(fieldEndpoint); err != nil {
			return nil, err
		}
	}

	var accountKey string
	var keyCredential azcosmos.KeyCredential
	if conf.Contains(fieldAccountKey) {
		if accountKey, err = conf.FieldString(fieldAccountKey); err != nil {
			return nil, err
		}

		keyCredential, err = azcosmos.NewKeyCredential(accountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialise %s: %s", fieldAccountKey, err)
		}
	}

	var connectionString string
	if conf.Contains(fieldConnectionString) {
		if connectionString, err = conf.FieldString(fieldConnectionString); err != nil {
			return nil, err
		}
	}

	var client *azcosmos.Client
	if endpoint != "" {
		if accountKey != "" {
			client, err = azcosmos.NewClientWithKey(endpoint, keyCredential, nil)
		} else {
			var cred *azidentity.DefaultAzureCredential
			cred, err = azidentity.NewDefaultAzureCredential(nil)
			if err != nil {
				return nil, fmt.Errorf("error getting default Azure credentials: %s", err)
			}

			client, err = azcosmos.NewClient(endpoint, cred, nil)
		}
	} else if connectionString != "" {
		client, err = azcosmos.NewClientFromConnectionString(connectionString, nil)
	} else {
		return nil, fmt.Errorf("either %s or %s must be set", fieldEndpoint, fieldConnectionString)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %s", err)
	}

	database, err := conf.FieldString(fieldDatabase)
	if err != nil {
		return nil, err
	}

	container, err := conf.FieldString(fieldContainer)
	if err != nil {
		return nil, err
	}

	containerClient, err := client.NewContainer(database, container)
	if err != nil {
		return nil, fmt.Errorf("failed to create container client: %s", err)
	}

	return containerClient, nil
}

// CRUDConfigFromParsed extracts the CRUD config from the parsed config
func CRUDConfigFromParsed(conf *service.ParsedConfig) (CRUDConfig, error) {
	var c CRUDConfig
	var err error

	if c.PartitionKeys, err = conf.FieldBloblang(FieldPartitionKeys); err != nil {
		return CRUDConfig{}, err
	}

	if c.AutoID, err = conf.FieldBool(fieldAutoID); err != nil {
		return CRUDConfig{}, err
	}

	if conf.Contains(fieldItemID) {
		if c.ItemID, err = conf.FieldInterpolatedString(fieldItemID); err != nil {
			return CRUDConfig{}, err
		}
	}

	operation, err := conf.FieldString(fieldOperation)
	if err != nil {
		return CRUDConfig{}, err
	}
	switch o := OperationType(operation); o {
	case OperationCreate, OperationDelete, OperationReplace, OperationUpsert, OperationRead, OperationPatch:
		c.Operation = o
	default:
		return CRUDConfig{}, fmt.Errorf("unrecognised %s: %s", fieldOperation, operation)
	}

	if c.Operation == OperationPatch {
		if conf.Contains(fieldPatchCondition) {
			if c.PatchCondition, err = conf.FieldInterpolatedString(fieldPatchCondition); err != nil {
				return CRUDConfig{}, err
			}
		}

		patchOperationsConfs, err := conf.FieldObjectList(fieldPatchOperations)
		if err != nil {
			return CRUDConfig{}, err
		}

		for _, poConf := range patchOperationsConfs {
			var po patchOperation

			var operation string
			if operation, err = poConf.FieldString(fieldPatchOperation); err != nil {
				return CRUDConfig{}, err
			}
			switch o := patchOperationType(operation); o {
			case patchOperationAdd, patchOperationIncrement, patchOperationRemove, patchOperationReplace, patchOperationSet:
				po.Operation = o
			default:
				return CRUDConfig{}, fmt.Errorf("unrecognised %s: %s", fieldPatchOperation, operation)
			}

			if po.Path, err = poConf.FieldInterpolatedString(fieldPatchPath); err != nil {
				return CRUDConfig{}, err
			}

			if poConf.Contains(fieldPatchValue) {
				if po.Value, err = poConf.FieldBloblang(fieldPatchValue); err != nil {
					return CRUDConfig{}, err
				}
			}
			if po.Value == nil && po.Operation != patchOperationRemove {
				return CRUDConfig{}, fmt.Errorf("the %s field must be set when the patch operation is not %s", fieldPatchValue, patchOperationRemove)
			}

			c.PatchOperations = append(c.PatchOperations, po)
		}
	}

	return c, nil
}
