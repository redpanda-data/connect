package jira

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	if err := service.RegisterProcessor(
		"jira", jiraProcessorConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newJiraProcessor(conf, mgr)
		},
	); err != nil {
		panic(err)
	}
}

func (j *jiraProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	inputMsg, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	j.debug("Fetching from Jira.. Input: %s", string(inputMsg))

	inputQuery, err := j.extractQueryFromMessage(msg)
	if err != nil {
		return nil, err
	}

	resource, customFields, params, err := j.prepareJiraQuery(ctx, inputQuery)
	if err != nil {
		return nil, err
	}

	return SearchResource(ctx, j, resource, inputQuery, customFields, params)
}

func (*jiraProc) Close(context.Context) error { return nil }
