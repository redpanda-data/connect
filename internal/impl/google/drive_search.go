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

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	driveSearchFieldQuery      = "query"
	driveSearchFieldProjection = "projection"
	driveSearchFieldLabels     = "include_label_ids"
	driveSearchFieldMaxResults = "max_results"
)

func init() {
	service.MustRegisterProcessor(
		"google_drive_search",
		driveSearchProcessorConfig(),
		newGoogleDriveSearchProcessor,
	)
}

func driveSearchProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Unstructured").
		Summary("Searches Google Drive for files matching the provided query.").
		Description(`
This processor searches for files in Google Drive using the provided query.

Search results are emitted as message batch, where each message is a https://developers.google.com/workspace/drive/api/reference/rest/v3/files#File[^Google Drive File]

`+authDescription("https://www.googleapis.com/auth/drive.readonly")).
		Fields(commonFields()...).
		Fields(
			service.NewInterpolatedStringField(driveSearchFieldQuery).
				Description("The search query to use for finding files in Google Drive. Supports the same query format as the Google Drive UI."),
			service.NewStringListField(driveSearchFieldProjection).
				Description("The partial fields to include in the result.").
				Default([]any{"id", "name", "mimeType", "size", "labelInfo"}),
			service.NewInterpolatedStringField(driveSearchFieldLabels).
				Description("A comma delimited list of label IDs to include in the result").
				Default(""),
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
	*baseProcessor[drive.Service]
	query      *service.InterpolatedString
	labels     *service.InterpolatedString
	fields     []string
	maxResults int
}

// newGoogleDriveSearchProcessor creates a new instance of googleDriveSearchProcessor.
func newGoogleDriveSearchProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}
	base, err := newBaseDriveProcessor(conf)
	if err != nil {
		return nil, err
	}
	query, err := conf.FieldInterpolatedString(driveSearchFieldQuery)
	if err != nil {
		return nil, err
	}
	labels, err := conf.FieldInterpolatedString(driveSearchFieldLabels)
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
		labels:        labels,
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
		return nil, fmt.Errorf("failed to interpolate %s: %v", driveSearchFieldQuery, err)
	}
	l, err := g.labels.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate %s: %v", driveSearchFieldLabels, err)
	}
	call := client.Files.List().
		Context(ctx).
		Q(q).
		PageSize(min(int64(g.maxResults), 100)).
		Fields("nextPageToken", googleapi.Field("files("+strings.Join(g.fields, ",")+")"))
	if l != "" {
		call = call.IncludeLabels(l)
	}
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
		cpy := msg.Copy()
		cpy.SetBytes(b)
		batch = append(batch, cpy)
	}
	return batch, nil
}
