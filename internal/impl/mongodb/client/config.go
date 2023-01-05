package client

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

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
	// OperationInvalid Invalid operation.
	OperationInvalid Operation = "invalid"
)

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
	default:
		return OperationInvalid
	}
}

// JSONMarshalMode represents the way in which BSON should be marshalled to JSON.
type JSONMarshalMode string

const (
	// JSONMarshalModeCanonical Canonical BSON to JSON marshal mode.
	JSONMarshalModeCanonical JSONMarshalMode = "canonical"
	// JSONMarshalModeRelaxed Relaxed BSON to JSON marshal mode.
	JSONMarshalModeRelaxed JSONMarshalMode = "relaxed"
)

// Config is a config struct for a mongo connection.
type Config struct {
	URL        string `json:"url" yaml:"url"`
	Database   string `json:"database" yaml:"database"`
	Collection string `json:"collection" yaml:"collection"`
	Username   string `json:"username" yaml:"username"`
	Password   string `json:"password" yaml:"password"`
}

// WriteConcern describes a write concern for MongoDB.
type WriteConcern struct {
	W        string `json:"w" yaml:"w"`
	J        bool   `json:"j" yaml:"j"`
	WTimeout string `json:"w_timeout" yaml:"w_timeout"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		URL:        "",
		Database:   "",
		Collection: "",
		Username:   "",
		Password:   "",
	}
}

// Client returns a new mongodb client based on the configuration parameters.
func (m Config) Client() (*mongo.Client, error) {
	opt := options.Client().
		SetConnectTimeout(10 * time.Second).
		SetSocketTimeout(30 * time.Second).
		SetServerSelectionTimeout(30 * time.Second).
		ApplyURI(m.URL)

	if m.Username != "" && m.Password != "" {
		creds := options.Credential{
			Username: m.Username,
			Password: m.Password,
		}
		opt.SetAuth(creds)
	}

	client, err := mongo.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to create mongodb client: %v", err)
	}

	return client, nil
}

// ConfigDocs returns a documentation field spec for fields within a Config.
func ConfigDocs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString(
			"url", "The URL of the target MongoDB DB.",
			"mongodb://localhost:27017",
		),
		docs.FieldString("database", "The name of the target MongoDB DB."),
		docs.FieldString("username", "The username to connect to the database."),
		docs.FieldString("password", "The password to connect to the database."),
	}
}
