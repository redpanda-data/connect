/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package google

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/drivelabels/v2"
	"google.golang.org/api/option"
)

const (
	baseFieldCredentialsJSON = "credentials_json"

	baseAuthDescription = `== Authentication
By default, this connector will use Google Application Default Credentials (ADC) to authenticate with Google APIs.

To use this mechanism locally, the following gcloud commands can be used:

	# Login for the application default credentials and add scopes for readonly drive access
	gcloud auth application-default login --scopes='openid,https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/cloud-platform'
	# When logging in with a user account, you may need to set the quota project for the application default credentials
	gcloud auth application-default set-quota-project <project-id>

Otherwise if using a service account, you can create a JSON key for the service account and set it in the ` + "`" + baseFieldCredentialsJSON + "`" + ` field.
In order for a service account to access files in Google Drive either files need to be explicitly shared with the service account email, otherwise https://support.google.com/a/answer/162106[^domain wide delegation] can be used to share all files within a Google Workspace.
`
)

func commonFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(baseFieldCredentialsJSON).
			Description("A service account credentials JSON file. If left unset then the application default credentials are used.").
			Optional().
			Secret(),
	}
}

type baseProcessor[Service any] struct {
	credentialsJSON string

	mu      sync.RWMutex
	service *Service // guarded by mu
	ctor    func(context.Context, ...option.ClientOption) (*Service, error)
}

func newBaseLabelProcessor(conf *service.ParsedConfig) (*baseProcessor[drivelabels.Service], error) {
	creds := ""
	if conf.Contains(baseFieldCredentialsJSON) {
		var err error
		creds, err = conf.FieldString(baseFieldCredentialsJSON)
		if err != nil {
			return nil, err
		}
	}
	return &baseProcessor[drivelabels.Service]{credentialsJSON: creds, ctor: drivelabels.NewService}, nil
}

func newBaseDriveProcessor(conf *service.ParsedConfig) (*baseProcessor[drive.Service], error) {
	creds := ""
	if conf.Contains(baseFieldCredentialsJSON) {
		var err error
		creds, err = conf.FieldString(baseFieldCredentialsJSON)
		if err != nil {
			return nil, err
		}
	}
	return &baseProcessor[drive.Service]{credentialsJSON: creds, ctor: drive.NewService}, nil
}

func (g *baseProcessor[Service]) getDriveService(ctx context.Context) (*Service, error) {
	g.mu.RLock()
	service := g.service
	g.mu.RUnlock()
	if service != nil {
		return service, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.service != nil {
		return g.service, nil
	}
	options, err := googleClientOptions(ctx, g.credentialsJSON)
	if err != nil {
		return nil, err
	}
	service, err = g.ctor(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Drive service: %v", err)
	}
	g.service = service
	return g.service, nil
}

func (g *baseProcessor[Service]) Close(ctx context.Context) error {
	return nil
}

func googleClientOptions(ctx context.Context, credentialsJSON string) (options []option.ClientOption, err error) {
	if credentialsJSON == "" {
		creds, err := google.FindDefaultCredentials(ctx, drive.DriveReadonlyScope)
		if err != nil {
			return nil, fmt.Errorf("failed to create default google client: %v", err)
		}
		options = append(options, option.WithTokenSource(creds.TokenSource))
		if len(creds.JSON) > 0 {
			var quotaProjectConfig struct {
				ID string `json:"quota_project_id"`
			}
			_ = json.Unmarshal(creds.JSON, &quotaProjectConfig)
			if quotaProjectConfig.ID != "" {
				options = append(options, option.WithQuotaProject(quotaProjectConfig.ID))
			}
		}
	} else {
		jwtConfig, err := google.JWTConfigFromJSON([]byte(credentialsJSON), drive.DriveReadonlyScope)
		if err != nil {
			return nil, fmt.Errorf("failed to parse credentials: %v", err)
		}
		client := jwtConfig.Client(ctx)
		options = append(options, option.WithHTTPClient(client))
	}
	return
}
