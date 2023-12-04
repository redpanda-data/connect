package gcp

import (
	"encoding/json"

	"google.golang.org/api/option"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	credentialsPathField = "credentials_path"
	credentialsField     = "credentials"

	typeField             = "type"
	projectIDField        = "project_id"
	privateKeyIDField     = "private_key_id"
	privateKeyField       = "private_key"
	clientEmailField      = "client_email"
	clientIDField         = "client_id"
	authURIField          = "auth_uri"
	tokenURIField         = "token_uri"
	authProviderX509Field = "auth_provider_x509_cert_url"
	clientX509Field       = "client_x509_cert_url"
)

func CredentialsFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(credentialsPathField).
			Description("The path for custom credentials of Google Cloud.").
			Optional().
			Advanced().
			Secret(),
		service.NewObjectField(credentialsField,
			service.NewStringField(typeField).
				Description("The type of the account (always \"service_account\" for a service account).").
				Advanced().
				Default("service_account"),
			service.NewStringField(projectIDField).
				Description("The ID of your Google Cloud project.").
				Advanced(),
			service.NewStringField(privateKeyIDField).
				Description("The ID of your private key.").
				Advanced().
				Secret(),
			service.NewStringField(privateKeyField).
				Description("Your private key.").
				Advanced().
				Secret(),
			service.NewStringField(clientEmailField).
				Description("The email address of your service account.").
				Advanced().
				Secret(),
			service.NewStringField(clientIDField).
				Description("The ID of your service account.").
				Advanced().
				Secret(),
			service.NewStringField(authURIField).
				Description("The URI for OAuth2 authorization.").
				Advanced().
				Default("https://accounts.google.com/o/oauth2/auth"),
			service.NewStringField(tokenURIField).
				Description("The URI that provides the OAuth2 tokens.").
				Advanced().
				Default("https://oauth2.googleapis.com/token"),
			service.NewStringField(authProviderX509Field).
				Description("The URL of the authentication provider's X.509 public key certificate.").
				Advanced().
				Default("https://www.googleapis.com/oauth2/v1/certs"),
			service.NewStringField(clientX509Field).
				Description("The URL of the service account's X.509 public key certificate. Typically: https://www.googleapis.com/robot/v1/metadata/x509/your-service-account-email%40your-project-id.iam.gserviceaccount.com").
				Advanced().
				Secret()).
			Description("Optional manual configuration of Google Cloud credentials to use. More information can be found [in this document](/docs/guides/cloud/gcp).").
			Optional().
			Advanced().
			Secret(),
	}
}

func GetGoogleCloudCredentials(parsedConfig *service.ParsedConfig) ([]option.ClientOption, error) {
	if parsedConfig.Contains(credentialsPathField) {
		credentialsPath, err := parsedConfig.FieldString(credentialsPathField)
		if err != nil {
			return nil, err
		}

		return []option.ClientOption{option.WithCredentialsFile(credentialsPath)}, nil
	}

	if parsedConfig.Contains(credentialsField) {
		credentialsMap, err := parsedConfig.FieldStringMap(credentialsField)
		if err != nil {
			return nil, err
		}

		credentialsJSON, err := json.Marshal(credentialsMap)
		if err != nil {
			return nil, err
		}

		return []option.ClientOption{option.WithCredentialsJSON(credentialsJSON)}, nil
	}

	return nil, nil
}
