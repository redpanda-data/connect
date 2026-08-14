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

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// JSONMarshalMode represents the way in which BSON should be marshalled to JSON.
type JSONMarshalMode string

const (
	// JSONMarshalModeCanonical Canonical BSON to JSON marshal mode.
	JSONMarshalModeCanonical JSONMarshalMode = "canonical"
	// JSONMarshalModeRelaxed Relaxed BSON to JSON marshal mode.
	JSONMarshalModeRelaxed JSONMarshalMode = "relaxed"
)

//------------------------------------------------------------------------------

const (
	// Common Client Fields
	commonFieldClientURL      = "url"
	commonFieldClientDatabase = "database"
	commonFieldClientUsername = "username"
	commonFieldClientPassword = "password"
	commonFieldClientAppName  = "app_name"
)

const (
	// FieldAWSIAMAuth is the name of the AWS IAM authentication object field.
	FieldAWSIAMAuth = "aws"
	// FieldAWSIAMAuthEnabled is the name of the field enabling AWS IAM authentication.
	FieldAWSIAMAuthEnabled = "enabled"
	// FieldAWSIAMAuthRegion is the name of the AWS region field.
	FieldAWSIAMAuthRegion = "region"
	// FieldAWSIAMAuthSessionDuration is the name of the STS session duration field.
	FieldAWSIAMAuthSessionDuration = "session_duration"
	// FieldAWSIAMAuthID is the name of the static access key ID field.
	FieldAWSIAMAuthID = "id"
	// FieldAWSIAMAuthSecret is the name of the static secret key field.
	FieldAWSIAMAuthSecret = "secret"
	// FieldAWSIAMAuthToken is the name of the static session token field.
	FieldAWSIAMAuthToken = "token"
	// FieldAWSIAMAuthRole is the name of the role ARN field, also used for entries of the roles list.
	FieldAWSIAMAuthRole = "role"
	// FieldAWSIAMAuthRoleExternalID is the name of the role external ID field, also used for entries of the roles list.
	FieldAWSIAMAuthRoleExternalID = "role_external_id"
	// FieldAWSIAMAuthRoles is the name of the role-chaining list field.
	FieldAWSIAMAuthRoles = "roles"
)

// clientConstructTimeout bounds client construction for components without a
// connect lifecycle (processor, cache). Role assumption is rejected for these
// components, so no STS calls happen under this context — it only bounds the
// driver handshake work, which is further limited by the client's own
// connect/server-selection timeouts.
const clientConstructTimeout = time.Minute

// CredentialBuilder resolves a MONGODB-AWS credential at connection time. It
// returns nil when there is no credential to apply.
type CredentialBuilder func(ctx context.Context) (*options.Credential, error)

func notImportedAWSOptFn(awsConf *service.ParsedConfig, _ *service.Logger) (CredentialBuilder, error) {
	if enabled, _ := awsConf.FieldBool(FieldAWSIAMAuthEnabled); !enabled {
		return nil, nil
	}
	return nil, errors.New("unable to configure AWS authentication as this binary does not import components/aws")
}

// AWSOptFn is populated by the child `aws` package when imported. It parses and
// validates the `aws` config block eagerly, so that misconfiguration fails at
// startup, and returns a builder which resolves credentials at connection time.
// A nil builder is returned when IAM authentication is disabled.
var AWSOptFn = notImportedAWSOptFn

// AWSIAMAuthField returns the spec of the `aws` IAM authentication block
// shared by all MongoDB components.
func AWSIAMAuthField() *service.ConfigField {
	return service.NewObjectField(FieldAWSIAMAuth,
		service.NewBoolField(FieldAWSIAMAuthEnabled).
			Description("Enable AWS IAM authentication using the driver-native `MONGODB-AWS` mechanism. The MongoDB Atlas database user must be created with the AWS IAM authentication type, and connections require TLS. When no static credentials or roles are configured, the ambient AWS credential chain (environment variables, EC2 instance profile, EKS pod role) is used and expiring credentials are refreshed automatically.").
			ShortDescription("Enable AWS IAM authentication using the MONGODB-AWS mechanism.").
			Default(false),
		service.NewStringField(FieldAWSIAMAuthRegion).
			Description("The AWS region used when assuming roles (for STS calls). Only used when `role` or `roles` are configured; the ambient and static-key paths ignore it. If no region is specified then the environment default is used.").
			ShortDescription("The AWS region used for STS calls when assuming roles. Defaults to the environment region.").
			Optional(),
		service.NewDurationField(FieldAWSIAMAuthSessionDuration).
			Description("The duration of the STS session requested when assuming roles. AWS requires at least 15 minutes and caps sessions created through role chaining at one hour. Only used when `role` or `roles` are configured. When using `mongodb_cdc` with role assumption, credentials are freshly resolved after the initial snapshot completes, so the streaming phase starts with a full session. The snapshot itself must still complete within a single session duration: snapshot progress is not checkpointed, so a credential expiry mid-snapshot restarts the snapshot from scratch after reconnecting. Once the snapshot completes and is fully acknowledged, its position is checkpointed, so later restarts resume the stream without re-running the snapshot. For very large snapshots prefer the ambient credential chain.").
			ShortDescription("STS session duration when assuming roles. AWS requires at least 15m and caps role chaining at 1h.").
			Default("1h").
			Advanced(),
		service.NewStringField(FieldAWSIAMAuthID).
			Description("The ID of credentials to use.").
			Optional().Advanced(),
		service.NewStringField(FieldAWSIAMAuthSecret).
			Description("The secret for the credentials being used.").
			Optional().Advanced().Secret(),
		service.NewStringField(FieldAWSIAMAuthToken).
			Description("The token for the credentials being used, required when using short term credentials.").
			Optional().Advanced(),
		service.NewStringField(FieldAWSIAMAuthRole).
			Description("Optional AWS IAM role ARN to assume for authentication. Cannot be combined with `roles`; use the `roles` array instead when chaining multiple roles.").
			ShortDescription("Optional AWS IAM role ARN to assume for authentication. Cannot be combined with roles.").
			Optional(),
		service.NewStringField(FieldAWSIAMAuthRoleExternalID).
			Description("Optional external ID for the role assumption. Only used with the `role` field, which cannot be combined with `roles`.").
			ShortDescription("Optional external ID for the role assumption. Only used alongside the role field.").
			Optional(),
		service.NewObjectListField(FieldAWSIAMAuthRoles,
			service.NewStringField(FieldAWSIAMAuthRole).
				Default("").
				Description("AWS IAM role ARN to assume."),
			service.NewStringField(FieldAWSIAMAuthRoleExternalID).
				Description("Optional external ID for the role assumption.").
				Default("").
				Optional(),
		).
			Description("Optional array of AWS IAM roles to assume for authentication. Roles can be assumed in sequence, enabling chaining for purposes such as cross-account access. Each role can optionally specify an external ID. Cannot be combined with `role`.").
			ShortDescription("AWS IAM roles to assume for authentication. Assumed in sequence to allow role chaining.").
			Optional(),
	).
		Description("AWS IAM authentication using the `MONGODB-AWS` mechanism, for example against MongoDB Atlas. When enabled, IAM credentials are used instead of a static username and password. Role-derived session credentials are resolved when the component connects and are re-resolved whenever it reconnects. The `mongodb` processor and cache establish their client once at creation and cannot refresh expiring session credentials, so `role` and `roles` are rejected for those components; use the ambient credential chain or static keys with them. For long-running pipelines, prefer the ambient credential chain (leave keys and roles unset), which the driver refreshes automatically.").
		ShortDescription("AWS IAM authentication configuration (MONGODB-AWS).").
		Advanced().
		Optional().
		Version("4.106.0")
}

// ClientConfig holds parsed MongoDB connection settings so that a client can be
// built - and rebuilt with freshly resolved credentials - at connect time.
type ClientConfig struct {
	url         string
	appName     string
	database    string
	username    string
	password    string
	credBuilder CredentialBuilder // nil unless aws.enabled
	assumesRole bool
}

// AssumesRole reports whether the aws block is configured with role
// assumption, which yields expiring session credentials that can only be
// refreshed by rebuilding the client.
func (c *ClientConfig) AssumesRole() bool {
	return c.assumesRole
}

// ClientConfigFromParsed parses and validates the connection fields shared by
// all MongoDB components (url, app_name, database, username, password and the
// aws IAM block). Any AWS configuration is validated here, at construction
// time, while the credentials themselves are resolved on each connect.
func ClientConfigFromParsed(conf *service.ParsedConfig, logger *service.Logger) (*ClientConfig, error) {
	c := &ClientConfig{}

	var err error
	if c.url, err = conf.FieldString(commonFieldClientURL); err != nil {
		return nil, err
	}
	if c.appName, err = conf.FieldString(commonFieldClientAppName); err != nil {
		return nil, err
	}
	if c.database, err = conf.FieldString(commonFieldClientDatabase); err != nil {
		return nil, err
	}
	if c.username, err = conf.FieldString(commonFieldClientUsername); err != nil {
		return nil, err
	}
	if c.password, err = conf.FieldString(commonFieldClientPassword); err != nil {
		return nil, err
	}

	// Probe the URL once so that malformed connection strings are rejected at
	// startup, and so we can tell whether it already carries credentials.
	probe := options.Client().ApplyURI(c.url)
	if err := probe.Validate(); err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}

	awsConf := conf.Namespace(FieldAWSIAMAuth)
	if enabled, _ := awsConf.FieldBool(FieldAWSIAMAuthEnabled); enabled {
		if c.username != "" || c.password != "" {
			return nil, errors.New("username and password cannot be set when aws.enabled is true, the MONGODB-AWS mechanism authenticates with IAM credentials instead")
		}
		// Only userinfo in the URL is a conflict: URLs which merely name the
		// mechanism (`?authSource=$external&authMechanism=MONGODB-AWS`, as Atlas
		// suggests for IAM users) also populate probe.Auth and are fine.
		if probe.Auth != nil && (probe.Auth.Username != "" || probe.Auth.PasswordSet) {
			return nil, errors.New("credentials embedded in the url cannot be combined with aws.enabled; the MONGODB-AWS mechanism authenticates with IAM credentials instead")
		}
		role, _ := awsConf.FieldString(FieldAWSIAMAuthRole)
		roles, _ := awsConf.FieldObjectList(FieldAWSIAMAuthRoles)
		c.assumesRole = role != "" || len(roles) > 0
		if c.credBuilder, err = AWSOptFn(awsConf, logger); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Connect builds a MongoDB client using freshly resolved credentials. The
// optFns allow callers to adjust the client options (e.g. BSON settings) before
// connecting.
func (c *ClientConfig) Connect(ctx context.Context, optFns ...func(*options.ClientOptions)) (*mongo.Client, *mongo.Database, error) {
	opt := options.Client().
		SetConnectTimeout(10 * time.Second).
		SetTimeout(30 * time.Second).
		SetServerSelectionTimeout(30 * time.Second).
		ApplyURI(c.url).
		SetAppName(c.appName)
	for _, fn := range optFns {
		fn(opt)
	}

	if c.credBuilder != nil {
		cred, err := c.credBuilder(ctx)
		if err != nil {
			return nil, nil, err
		}
		if cred != nil {
			opt.SetAuth(*cred)
		}
	} else if c.username != "" && c.password != "" {
		opt.SetAuth(options.Credential{
			Username: c.username,
			Password: c.password,
		})
	}

	client, err := mongo.Connect(opt)
	if err != nil {
		return nil, nil, err
	}
	return client, client.Database(c.database), nil
}

// isConnPoolError reports whether err indicates the client's connection pool
// is unusable — a handshake (including MONGODB-AWS auth) failure, a cleared
// pool, or the server being unselectable — as opposed to an ordinary
// operation error. Components with a connect lifecycle should treat these by
// rebuilding the client, which also re-resolves IAM credentials.
//
// The pool-cleared error type has no Unwrap, so text matching is the only
// available detection for some shapes.
func isConnPoolError(err error) bool {
	if err == nil {
		return false
	}
	if mongo.IsNetworkError(err) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "error occurred during connection handshake") ||
		(strings.Contains(msg, "connection pool for ") && strings.Contains(msg, " was cleared")) ||
		strings.Contains(msg, "server selection error")
}

func clientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewURLField(commonFieldClientURL).
			Description("The URL of the target MongoDB server.").
			Example("mongodb://localhost:27017"),
		service.NewStringField(commonFieldClientDatabase).
			Description("The name of the target MongoDB database."),
		service.NewStringField(commonFieldClientUsername).
			Description("The username to connect to the database.").
			Default(""),
		service.NewStringField(commonFieldClientPassword).
			Description("The password to connect to the database.").
			Default("").
			Secret(),
		service.NewURLField(commonFieldClientAppName).
			Description("The client application name.").
			Default("benthos").
			Advanced(),
		AWSIAMAuthField(),
	}
}

//------------------------------------------------------------------------------

// Operation represents the operation that will be performed by MongoDB.
type Operation string

const (
	// OperationInsertOne Insert One operation.
	OperationInsertOne Operation = "insert-one"
	// OperationDeleteOne Delete One operation.
	OperationDeleteOne Operation = "delete-one"
	// OperationDeleteMany Delete many operation.
	OperationDeleteMany Operation = "delete-many"
	// OperationReplaceOne Replace one operation.
	OperationReplaceOne Operation = "replace-one"
	// OperationUpdateOne Update one operation.
	OperationUpdateOne Operation = "update-one"
	// OperationFindOne Find one operation.
	OperationFindOne Operation = "find-one"
	// OperationAggregate Execute Aggregation Pipeline operation.
	OperationAggregate Operation = "aggregate"
	// OperationInvalid Invalid operation.
	OperationInvalid Operation = "invalid"
)

func (op Operation) isDocumentAllowed() bool {
	switch op {
	case OperationInsertOne,
		OperationReplaceOne,
		OperationUpdateOne,
		OperationAggregate:
		return true
	default:
		return false
	}
}

func (op Operation) isFilterAllowed() bool {
	switch op {
	case OperationDeleteOne,
		OperationDeleteMany,
		OperationReplaceOne,
		OperationUpdateOne,
		OperationFindOne:
		return true
	default:
		return false
	}
}

func (op Operation) isHintAllowed() bool {
	switch op {
	case OperationDeleteOne,
		OperationDeleteMany,
		OperationReplaceOne,
		OperationUpdateOne,
		OperationFindOne:
		return true
	default:
		return false
	}
}

func (op Operation) isUpsertAllowed() bool {
	switch op {
	case OperationReplaceOne,
		OperationUpdateOne:
		return true
	default:
		return false
	}
}

// NewOperation converts a string operation to a strongly-typed Operation.
func NewOperation(op string) Operation {
	switch op {
	case "insert-one":
		return OperationInsertOne
	case "delete-one":
		return OperationDeleteOne
	case "delete-many":
		return OperationDeleteMany
	case "replace-one":
		return OperationReplaceOne
	case "update-one":
		return OperationUpdateOne
	case "find-one":
		return OperationFindOne
	case "aggregate":
		return OperationAggregate
	default:
		return OperationInvalid
	}
}

const (
	// Common Operation Fields
	commonFieldOperation = "operation"
)

func processorOperationDocs(defaultOperation Operation) *service.ConfigField {
	return service.NewStringEnumField("operation",
		string(OperationInsertOne),
		string(OperationDeleteOne),
		string(OperationDeleteMany),
		string(OperationReplaceOne),
		string(OperationUpdateOne),
		string(OperationFindOne),
		string(OperationAggregate),
	).Description("The mongodb operation to perform.").
		Default(string(defaultOperation))
}

func outputOperationDocs(defaultOperation Operation) *service.ConfigField {
	return service.NewStringEnumField("operation",
		string(OperationInsertOne),
		string(OperationDeleteOne),
		string(OperationDeleteMany),
		string(OperationReplaceOne),
		string(OperationUpdateOne),
	).Description("The mongodb operation to perform.").
		Default(string(defaultOperation))
}

func operationFromParsed(pConf *service.ParsedConfig) (operation Operation, err error) {
	var operationStr string
	if operationStr, err = pConf.FieldString(commonFieldOperation); err != nil {
		return
	}

	if operation = NewOperation(operationStr); operation == OperationInvalid {
		err = fmt.Errorf("mongodb operation %q unknown: must be insert-one, delete-one, delete-many, replace-one, update-one or aggregate", operationStr)
	}
	return
}

//------------------------------------------------------------------------------

const (
	// Common Write Concern Fields
	commonFieldWriteConcern         = "write_concern"
	commonFieldWriteConcernW        = "w"
	commonFieldWriteConcernJ        = "j"
	commonFieldWriteConcernWTimeout = "w_timeout"
)

func writeConcernDocs() *service.ConfigField {
	return service.NewObjectField(commonFieldWriteConcern,
		service.NewStringField(commonFieldWriteConcernW).
			Description(`W requests acknowledgement that write operations propagate to the specified number of mongodb instances. Can be the string "majority" to wait for a calculated majority of nodes to acknowledge the write operation, or an integer value specifying an minimum number of nodes to acknowledge the operation, or a string specifying the name of a custom write concern configured in the cluster.`).
			ShortDescription("How many MongoDB instances must acknowledge a write. Can be majority to wait for a calculated majority.").
			Default("majority"),
		service.NewBoolField(commonFieldWriteConcernJ).
			Description("J requests acknowledgement from MongoDB that write operations are written to the journal.").
			Default(false),
		service.NewStringField(commonFieldWriteConcernWTimeout).
			Description("The write concern timeout.").
			Default(""),
	).Description("The write concern settings for the mongo connection.")
}

func writeConcernSpecFromParsed(pConf *service.ParsedConfig) (spec *writeConcernSpec, err error) {
	pConf = pConf.Namespace(commonFieldWriteConcern)

	var w string
	if w, err = pConf.FieldString(commonFieldWriteConcernW); err != nil {
		return
	}

	var j bool
	if j, err = pConf.FieldBool(commonFieldWriteConcernJ); err != nil {
		return
	}

	var wTimeout time.Duration
	if dStr, _ := pConf.FieldString(commonFieldWriteConcernWTimeout); dStr != "" {
		if wTimeout, err = pConf.FieldDuration(commonFieldWriteConcernWTimeout); err != nil {
			return
		}
	}

	writeConcern := &writeconcern.WriteConcern{
		Journal: &j,
	}
	if wInt, err := strconv.Atoi(w); err != nil {
		writeConcern.W = w
	} else {
		writeConcern.W = wInt
	}

	return &writeConcernSpec{
		options:  options.Collection().SetWriteConcern(writeConcern),
		wTimeout: wTimeout,
	}, nil
}

type writeConcernSpec struct {
	options  *options.CollectionOptionsBuilder
	wTimeout time.Duration
}

//------------------------------------------------------------------------------

const (
	// Common Write Map Fields
	commonFieldDocumentMap = "document_map"
	commonFieldFilterMap   = "filter_map"
	commonFieldHintMap     = "hint_map"
	commonFieldUpsert      = "upsert"
)

func writeMapsFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewBloblangField(commonFieldDocumentMap).
			Description("A bloblang map representing a document to store within MongoDB, expressed as https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/[extended JSON in canonical form^]. The document map is required for the operations " +
				"insert-one, replace-one, update-one and aggregate.").
			ShortDescription("A Bloblang map producing the document to store in MongoDB, as extended JSON in canonical form.").
			Examples(mapExamples()...).
			Default(""),
		service.NewBloblangField(commonFieldFilterMap).
			Description("A bloblang map representing a filter for a MongoDB command, expressed as https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/[extended JSON in canonical form^]. The filter map is required for all operations except " +
				"insert-one. It is used to find the document(s) for the operation. For example in a delete-one case, the filter map should " +
				"have the fields required to locate the document to delete.").
			ShortDescription("A Bloblang map producing a MongoDB filter, as extended JSON in canonical form.").
			Examples(mapExamples()...).
			Default(""),
		service.NewBloblangField(commonFieldHintMap).
			Description("A bloblang map representing the hint for the MongoDB command, expressed as https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/[extended JSON in canonical form^]. This map is optional and is used with all operations " +
				"except insert-one. It is used to improve performance of finding the documents in the mongodb.").
			ShortDescription("An optional Bloblang map producing the MongoDB command hint, as extended JSON in canonical form.").
			Examples(mapExamples()...).
			Default(""),
		service.NewBoolField(commonFieldUpsert).
			Description("The upsert setting is optional and only applies for update-one and replace-one operations. If the filter specified in filter_map matches, the document is updated or replaced accordingly, otherwise it is created.").
			ShortDescription("Insert the document when the filter matches nothing. Applies only to update-one and replace-one.").
			Version("3.60.0").
			Default(false),
	}
}

type writeMaps struct {
	filterMap   *bloblang.Executor
	documentMap *bloblang.Executor
	hintMap     *bloblang.Executor
	upsert      bool
}

func writeMapsFromParsed(conf *service.ParsedConfig, operation Operation) (maps writeMaps, err error) {
	if probeStr, _ := conf.FieldString(commonFieldFilterMap); probeStr != "" {
		if maps.filterMap, err = conf.FieldBloblang(commonFieldFilterMap); err != nil {
			return
		}
	}
	if probeStr, _ := conf.FieldString(commonFieldDocumentMap); probeStr != "" {
		if maps.documentMap, err = conf.FieldBloblang(commonFieldDocumentMap); err != nil {
			return
		}
	}
	if probeStr, _ := conf.FieldString(commonFieldHintMap); probeStr != "" {
		if maps.hintMap, err = conf.FieldBloblang(commonFieldHintMap); err != nil {
			return
		}
	}
	if maps.upsert, err = conf.FieldBool(commonFieldUpsert); err != nil {
		return
	}

	if operation.isFilterAllowed() {
		if maps.filterMap == nil {
			err = errors.New("mongodb filter_map must be specified")
			return
		}
	} else if maps.filterMap != nil {
		err = fmt.Errorf("mongodb filter_map not allowed for '%s' operation", operation)
		return
	}

	if operation.isDocumentAllowed() {
		if maps.documentMap == nil {
			err = errors.New("mongodb document_map must be specified")
			return
		}
	} else if maps.documentMap != nil {
		err = fmt.Errorf("mongodb document_map not allowed for '%s' operation", operation)
		return
	}

	if !operation.isHintAllowed() && maps.hintMap != nil {
		err = fmt.Errorf("mongodb hint_map not allowed for '%s' operation", operation)
		return
	}

	if !operation.isUpsertAllowed() && maps.upsert {
		err = fmt.Errorf("mongodb upsert not allowed for '%s' operation", operation)
		return
	}

	return
}

type writeMapsExec struct {
	filterMap   *service.MessageBatchBloblangExecutor
	documentMap *service.MessageBatchBloblangExecutor
	hintMap     *service.MessageBatchBloblangExecutor
	upsert      bool
}

func (w writeMaps) exec(b service.MessageBatch) (e writeMapsExec) {
	if w.filterMap != nil {
		e.filterMap = b.BloblangExecutor(w.filterMap)
	}
	if w.documentMap != nil {
		e.documentMap = b.BloblangExecutor(w.documentMap)
	}
	if w.hintMap != nil {
		e.hintMap = b.BloblangExecutor(w.hintMap)
	}
	e.upsert = w.upsert
	return
}

func extJSONFromMap(i int, m *service.MessageBatchBloblangExecutor) (any, error) {
	msg, err := m.Query(i)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}

	valBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	var ejsonVal any
	if err := bson.UnmarshalExtJSON(valBytes, true, &ejsonVal); err != nil {
		return nil, err
	}
	return ejsonVal, nil
}

func (w writeMapsExec) extractFromMessage(operation Operation, i int) (
	docJSON, filterJSON, hintJSON any, err error,
) {
	filterValWanted := operation.isFilterAllowed()
	documentValWanted := operation.isDocumentAllowed()

	if filterValWanted && w.filterMap != nil {
		if filterJSON, err = extJSONFromMap(i, w.filterMap); err != nil {
			err = fmt.Errorf("executing filter_map: %v", err)
			return
		}
	}

	if documentValWanted && w.documentMap != nil {
		if docJSON, err = extJSONFromMap(i, w.documentMap); err != nil {
			err = fmt.Errorf("executing document_map: %v", err)
			return
		}
	}

	if w.hintMap != nil {
		if hintJSON, err = extJSONFromMap(i, w.hintMap); err != nil {
			return
		}
	}
	return
}

func mapExamples() []any {
	examples := []any{"root.a = this.foo\nroot.b = this.bar"}
	return examples
}
