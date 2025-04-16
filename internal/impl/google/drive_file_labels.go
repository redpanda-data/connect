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

	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/api/drivelabels/v2"
)

func init() {
	err := service.RegisterProcessor(
		"google_drive_list_labels",
		driveLabelsProcessorConfig(),
		newGoogleDriveLabelsProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func driveLabelsProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Unstructured").
		Summary("Lists labels for a file in Google Drive").
		Description(`
Can list labels for a file from Google Drive based on a file ID.
`+baseAuthDescription).
		Fields(commonFields()...).
		Example("List files from Google Drive with labels", "This example lists all files with a specific name from Google Drive and their labels.", `
pipeline:
  processors:
    - branch:
        result_map: 'root.labels = this'
        processors:
          - google_drive_get_labels:
    - google_drive_search:
        query: "name contains 'Foo'"
`)
}

type googleDriveLabelsProcessor struct {
	*baseProcessor[drivelabels.Service]
}

func newGoogleDriveLabelsProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	base, err := newBaseLabelProcessor(conf)
	if err != nil {
		return nil, err
	}
	return &googleDriveLabelsProcessor{
		baseProcessor: base,
	}, nil
}

func (g *googleDriveLabelsProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	client, err := g.getDriveService(ctx)
	if err != nil {
		return nil, err
	}
	allLabels := []json.RawMessage{}
	err = client.Labels.List().
		Context(ctx).
		PublishedOnly(true).
		View("LABEL_VIEW_FULL").
		Pages(ctx, func(labels *drivelabels.GoogleAppsDriveLabelsV2ListLabelsResponse) error {
			for _, label := range labels.Labels {
				b, err := label.MarshalJSON()
				if err != nil {
					return fmt.Errorf("unable to marshal label: %w", err)
				}
				allLabels = append(allLabels, b)
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("unable to list labels: %w", err)
	}
	labels, err := json.Marshal(allLabels)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal labels: %w", err)
	}
	msg = msg.Copy()
	msg.SetBytes(labels)
	return service.MessageBatch{msg}, nil
}
