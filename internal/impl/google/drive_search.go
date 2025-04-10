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
	"errors"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

const (
	driveSearchFieldQuery      = "query"
	driveSearchFieldProjection = "projection"
	driveSearchFieldMaxResults = "max_results"
)

func init() {
	err := service.RegisterProcessor(
		"google_drive_search",
		driveSearchProcessorConfig(),
		newGoogleDriveSearchProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func driveSearchProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Unstructured").
		Summary("Searches Google Drive for files matching the provided query.").
		Description(`
This processor searches for files in Google Drive using the provided query.

Search results are emitted as message batch.

Authentication is expected to be configured through Google application credentials.
`).
		Fields(commonFields()...).
		Fields(
			service.NewInterpolatedStringField(driveSearchFieldQuery).
				Description("The search query to use for finding files in Google Drive. Supports the same query format as the Google Drive UI."),
			service.NewStringListField(driveSearchFieldProjection).
				Description("The partial fields to include in the result.").
				Default([]any{"id", "name", "mimeType", "parents", "size"}),
			service.NewIntField(driveSearchFieldMaxResults).
				Description("The maximum number of results to return.").
				Default(64),
		).
		Example("Search & download files from Google Drive", "This examples downloads all the files from Google Drive that are returned in the query", `
input:
  stdin: {}
pipeline:
  processors:
    - google_drive_search:
        query: "${!content().string()}"
        projection: ["id", "name", "mimeType", "parents", "size"]
    - mutation: 'meta path = this.name'
    - google_drive_download:
        file_id: "${!this.id}"
        mime_type: "${!this.mimeType}"
output:
  file:
    path: "${!@path}"
    codec: all-bytes
`)
}

type googleDriveSearchProcessor struct {
	*baseProcessor
	query      *service.InterpolatedString
	fields     []string
	maxResults int
}

// newGoogleDriveSearchProcessor creates a new instance of googleDriveSearchProcessor.
func newGoogleDriveSearchProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	base, err := newBaseProcessor(conf)
	if err != nil {
		return nil, err
	}
	query, err := conf.FieldInterpolatedString(driveSearchFieldQuery)
	if err != nil {
		return nil, err
	}

	fields, err := conf.FieldStringList(driveSearchFieldProjection)
	if err != nil {
		return nil, err
	}

	maxResults, err := conf.FieldInt(driveSearchFieldMaxResults)
	if err != nil {
		return nil, err
	}

	return &googleDriveSearchProcessor{
		baseProcessor: base,
		query:         query,
		fields:        fields,
		maxResults:    maxResults,
	}, nil
}

var errStopIteration = errors.New("stop iteration")

func (g *googleDriveSearchProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	client, err := g.getDriveService(ctx)
	if err != nil {
		return nil, err
	}
	q, err := g.query.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate query: %v", err)
	}
	call := client.Files.List().
		Context(ctx).
		Q(q).
		PageSize(min(int64(g.maxResults), 100)).
		Fields("nextPageToken", googleapi.Field("files("+strings.Join(g.fields, ",")+")"))
	var files []*drive.File
	err = call.Pages(ctx, func(page *drive.FileList) error {
		files = append(files, page.Files...)
		if len(files) >= g.maxResults {
			return errStopIteration
		}
		return nil
	})
	if errors.Is(err, errStopIteration) {
		err = nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query files in google drive: %v", err)
	}
	batch := service.MessageBatch{}
	for _, file := range files {
		b, err := file.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal file to JSON: %v", err)
		}
		msg := service.NewMessage(b)
		batch = append(batch, msg)
	}
	return batch, nil
}
