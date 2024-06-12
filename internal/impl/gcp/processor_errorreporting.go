package gcp

import (
	"context"
	"errors"

	"cloud.google.com/go/errorreporting"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var (
	_ service.Processor = (*errorReportingProcessor)(nil)
)

func init() {
	err := service.RegisterProcessor(
		"gcp_errorreporting", newErrorReportingProcessorConfig(),
		newErrorReportingProcessor,
	)
	if err != nil {
		panic(err)
	}
}

type errorReportingProcessor struct {
	c       *errorreporting.Client
	project string
}

func newErrorReportingProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Summary("Report an individual error event and record the event to a log.").
		Field(service.NewStringField("project").
			Description("GCP project where the query job will execute.")).
		Field(service.NewObjectField("service_context",
			service.NewStringField("name").
				Description("Name identifies the running program and is included in the error reports"),
			service.NewStringField("version").
				Description("Version identifies the version of the running program and is included in the error reports.").Optional(),
		).
			Description("The service context in which this error has occurred.").
			Optional(),
		).
		Field(service.NewStringField("message").
			Description("The error message."))
}

func newErrorReportingProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {

	project, err := conf.FieldString("project")
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	serviceName, err := conf.FieldString("service_context.name")
	if err != nil {
		panic(err)
	}

	serviceVersion, err := conf.FieldString("service_context.version")
	if err != nil {
		panic(err)
	}

	cfg := errorreporting.Config{
		ServiceName:    serviceName,
		ServiceVersion: serviceVersion,
	}

	c, err := errorreporting.NewClient(ctx, project, cfg)
	if err != nil {
		return nil, err
	}
	return &errorReportingProcessor{
		c:       c,
		project: project,
	}, nil
}

func (p *errorReportingProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {

	s, err := msg.AsStructured()
	if err != nil {
		return nil, err
	}

	m, _ := s.(map[string]any)
	if m == nil {
		return nil, errors.New("not provided expected struct to ErrorReporting")
	}

	errAny := m["message"]
	if errAny == "" {
		return nil, errors.New("not provided expected error message")
	}

	errMsg, _ := errAny.(string)
	if errMsg == "" {
		return nil, errors.New("error message in wrong type")
	}

	userAny := m["user"]
	userString, _ := userAny.(string)

	p.c.Report(errorreporting.Entry{
		Error: errors.New(errMsg),
		User:  userString,
	})

	return service.MessageBatch{msg}, nil
}

func (p *errorReportingProcessor) Close(context.Context) error {
	return p.c.Close()
}
