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
	"fmt"
	"io"
	"slices"

	"google.golang.org/api/drive/v3"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	driveDownloadFieldFileID          = "file_id"
	driveDownloadFieldMimeType        = "mime_type"
	driveDownloadFieldExportMimeTypes = "export_mime_types"
)

func init() {
	service.MustRegisterProcessor(
		"google_drive_download",
		driveDownloadProcessorConfig(),
		newGoogleDriveDownloadProcessor,
	)
}

func driveDownloadProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Unstructured").
		Summary("Downloads files from Google Drive").
		Description(`
Can download a file from Google Drive based on a file ID.
`+authDescription("https://www.googleapis.com/auth/drive.readonly")).
		Fields(commonFields()...).
		Fields(
			service.NewInterpolatedStringField(driveDownloadFieldFileID).
				Description("The file ID of the file to download."),
			service.NewInterpolatedStringField(driveDownloadFieldMimeType).
				Description("The mime type of the file in drive."),
			service.NewStringMapField(driveDownloadFieldExportMimeTypes).
				Default(map[string]string{
					// Bias towards textual formats for exports because they are easier to work with in Connect.
					"application/vnd.google-apps.document":     "text/markdown",
					"application/vnd.google-apps.spreadsheet":  "text/csv",
					"application/vnd.google-apps.presentation": "application/pdf",
					"application/vnd.google-apps.drawing":      "image/png",
					"application/vnd.google-apps.script":       "application/vnd.google-apps.script+json",
				}).
				Description("A map of Google Drive MIME types to their export formats. The key is the MIME type, and the value is the export format. See https://developers.google.com/workspace/drive/api/guides/ref-export-formats[^Google Drive API Documentation] for a list of supported export types").
				Example(map[string]string{
					"application/vnd.google-apps.document":     "application/pdf",
					"application/vnd.google-apps.spreadsheet":  "application/pdf",
					"application/vnd.google-apps.presentation": "application/pdf",
					"application/vnd.google-apps.drawing":      "application/pdf",
				}).
				Example(map[string]string{
					"application/vnd.google-apps.document":     "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
					"application/vnd.google-apps.spreadsheet":  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
					"application/vnd.google-apps.presentation": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
					"application/vnd.google-apps.drawing":      "image/svg+xml",
				}).
				Advanced(),
		).
		Example("Download files from Google Drive", "This examples downloads all the files from Google Drive", `
pipeline:
  processors:
    - google_drive_search:
        query: "name = 'Test Doc'"
    - google_drive_download:
        file_id: "${!this.id}"
        mime_type: "${!this.mimeType}"
`)
}

type googleDriveDownloadProcessor struct {
	*baseProcessor[drive.Service]
	fileID          *service.InterpolatedString
	mimeType        *service.InterpolatedString
	exportMimeTypes map[string]string
}

func newGoogleDriveDownloadProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}
	base, err := newBaseDriveProcessor(conf)
	if err != nil {
		return nil, err
	}
	fileID, err := conf.FieldInterpolatedString(driveDownloadFieldFileID)
	if err != nil {
		return nil, err
	}
	mimeType, err := conf.FieldInterpolatedString(driveDownloadFieldMimeType)
	if err != nil {
		return nil, err
	}

	mimeTypes, err := conf.FieldStringMap(driveDownloadFieldExportMimeTypes)
	if err != nil {
		return nil, err
	}

	for mimeType, exportMimeType := range mimeTypes {
		formats, ok := googleMimeToFormat[mimeType]
		if !ok {
			return nil, fmt.Errorf("export is only valid for Google App file types, got: %v", mimeType)
		}
		ok = slices.ContainsFunc(formats.ExportTypes, func(et exportType) bool {
			return et.MimeType == exportMimeType
		})
		if !ok {
			return nil, fmt.Errorf("export mime type %v is not supported for mime type %v", exportMimeType, mimeType)
		}
	}

	return &googleDriveDownloadProcessor{
		baseProcessor:   base,
		fileID:          fileID,
		mimeType:        mimeType,
		exportMimeTypes: mimeTypes,
	}, nil
}

func (g *googleDriveDownloadProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	client, err := g.getDriveService(ctx)
	if err != nil {
		return nil, err
	}
	id, err := g.fileID.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate file_id: %v", err)
	}
	mimeType, err := g.mimeType.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate mime_type: %v", err)
	}
	exportMimeType, ok := g.exportMimeTypes[mimeType]
	var b []byte
	if ok {
		b, err = exportFile(ctx, client, id, exportMimeType)
	} else {
		b, err = downloadFile(ctx, client, id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to download file %v: %v", id, err)
	}
	msg = msg.Copy()
	msg.SetBytes(b)
	return service.MessageBatch{msg}, nil
}

func downloadFile(ctx context.Context, srv *drive.Service, fileID string) ([]byte, error) {
	resp, err := srv.Files.Get(fileID).Context(ctx).Download()
	if err != nil {
		return nil, fmt.Errorf("unable to download file: %v", err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func exportFile(ctx context.Context, srv *drive.Service, fileID, mimeType string) ([]byte, error) {
	resp, err := srv.Files.Export(fileID, mimeType).Context(ctx).Download()
	if err != nil {
		return nil, fmt.Errorf("unable to download file: %v", err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
