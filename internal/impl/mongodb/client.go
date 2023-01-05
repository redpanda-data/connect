package mongodb

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/benthosdev/benthos/v4/public/service"
)

func clientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewURLField("url").
			Description("The URL of the target MongoDB server.").
			Example("mongodb://localhost:27017"),
		service.NewStringField("username").Description("The username to connect to the database.").Default(""),
		service.NewStringField("password").Description("The password to connect to the database.").Default("").Secret(),
	}
}

func getClient(parsedConf *service.ParsedConfig) (*mongo.Client, error) {
	url, err := parsedConf.FieldString("url")
	if err != nil {
		return nil, err
	}

	username, err := parsedConf.FieldString("username")
	if err != nil {
		return nil, err
	}

	password, err := parsedConf.FieldString("password")
	if err != nil {
		return nil, err
	}

	opt := options.Client().
		SetConnectTimeout(10 * time.Second).
		SetSocketTimeout(30 * time.Second).
		SetServerSelectionTimeout(30 * time.Second).
		ApplyURI(url)

	if username != "" && password != "" {
		creds := options.Credential{
			Username: username,
			Password: password,
		}
		opt.SetAuth(creds)
	}

	client, err := mongo.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to create mongodb client: %v", err)
	}
	return client, nil
}
