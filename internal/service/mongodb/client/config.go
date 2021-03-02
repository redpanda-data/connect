package client

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Config is a config struct for a mongo connection
type Config struct {
	URL        string `json:"url" yaml:"url"`
	Database   string `json:"database" yaml:"database"`
	Collection string `json:"collection" yaml:"collection"`
	Username   string `json:"username" yaml:"username"`
	Password   string `json:"password" yaml:"password"`
}

type WriteConcern struct {
	W        string `json:"w" yaml:"w"`
	J        bool   `json:"j" yaml:"j"`
	WTimeout string `json:"w_timeout" yaml:"w_timeout"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		URL:        "mongodb://localhost:27017",
		Database:   "",
		Collection: "",
		Username:   "",
		Password:   "",
	}
}

// Client returns a new mongodb client based on the configuration parameters
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
		docs.FieldCommon(
			"url", "The URL of the target MongoDB DB.",
			"mongodb://localhost:27017",
		),
		docs.FieldCommon("database", "The name of the target MongoDB DB."),
		docs.FieldCommon("collection", "The name of the target collection in the MongoDB DB."),
		docs.FieldCommon("username", "The username to connect to the database."),
		docs.FieldCommon("password", "The password to connect to the database."),
	}
}
