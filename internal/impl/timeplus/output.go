package timeplus

import (
	"context"
	"errors"
	"sort"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/timeplus/http"
)

var outputConfigSpec *service.ConfigSpec

func init() {
	// TODO: add Version
	outputConfigSpec = service.NewConfigSpec().
		Categories("Services").
		Summary("Sends message to a Timeplus Enterprise stream via ingest endpoint").
		Description(`
This output can send message to Timeplus Enterprise Cloud, Timeplus Enterprise (self-hosted) or directly to timeplusd.

This output accepts structured message only. It also expects all message contains the same keys and matches the schema of stream. If the upstream source returns
unstructured message such as string, please include a processors to contruct the strcuture message. Here is a short sample:

` + "```yml" + `
output:
  timeplus:
    workspace: my_workspace_id
    stream: mystream
    apikey: <Api key genereated on web console>

	processors:
	  - mapping: |
        root = {}
        root.raw = content().string() # Make sure your stream contains a string type raw field
` + "```" + `

A sample config to send the data to Timeplus Enterprise Cloud
` + "```yml" + `
output:
  timeplus:
    workspace: my_workspace_id
    stream: mystream
    apikey: <Api key genereated on web console>
` + "```" + `

A sample config to send the data to Timeplus Enterprise (self-hosted)
` + "```yml" + `
output:
  timeplus:
    url: http://localhost:8000
    workspace: my_workspace_id
    stream: mystream
    username: username
    password: pw
` + "```" + `

A sample config to send the data to timeplusd
` + "```yml" + `
output:
  timeplus:
    url: http://localhost:3218
    stream: mystream
    username: username
    password: pw
` + "```" + `
`)

	outputConfigSpec.
		Field(service.NewStringEnumField("target", http.TargetTimeplus, http.TargetTimeplusd).Default(http.TargetTimeplus).Description("The destination type, either Timeplus Enterprise or timeplusd")).
		Field(service.NewURLField("url").Description("The url should always include schema and host.").Default("https://us-west-2.timeplus.cloud").Examples("http://localhost:8000", "http://127.0.0.1:3218")).
		Field(service.NewStringField("workspace").Optional().Description("ID of the workspace. Required if target is `timeplus`.")).
		Field(service.NewStringField("stream").Description("The name of the stream. Make sure the schema of the stream matches the input")).
		Field(service.NewStringField("apikey").Secret().Optional().Description("The API key. Required if you are sending message to Timeplus Enterprise Cloud")).
		Field(service.NewStringField("username").Optional().Description("The username. Required if you are sending message to Timeplus Enterprise (self-hosted) or timeplusd")).
		Field(service.NewStringField("password").Secret().Optional().Description("The password. Required if you are sending message to Timeplus Enterprise (self-hosted) or timeplusd")).
		Field(service.NewOutputMaxInFlightField()).
		Field(service.NewBatchPolicyField("batching"))

	if err := service.RegisterBatchOutput("timeplus", outputConfigSpec, newTimeplusOutput); err != nil {
		panic(err)
	}
}

type timeplus struct {
	logger *service.Logger
	client Writer
}

// Close implements service.Output
func (t *timeplus) Close(context.Context) error {
	// TODO: shall we wait for ongoing writes?
	t.client = nil
	return nil
}

// Connect implements service.Output
func (t *timeplus) Connect(context.Context) error {
	if t.client == nil {
		return errors.New("client not initialized")
	}

	return nil
}

func (t *timeplus) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	if len(b) == 0 {
		return nil
	}

	cols := []string{}
	rows := [][]any{}

	// Here we assume all messages have the same structure, same keys
	// Currently we will just skip the message if it is invalid. Probably we should add a feature flag to not skip them by propagating the error
	for _, msg := range b {
		keys := []string{}
		data := []any{}

		msgStructure, err := msg.AsStructured()
		if err != nil {
			t.logger.Errorf("failed to get structured message %w, skipping this message", err)
			continue
		}

		msgJSON, OK := msgStructure.(map[string]any)
		if !OK {
			t.logger.Errorf("expect map[string]any, got %T, skipping this message", msgJSON)
			continue
		}

		for key := range msgJSON {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			data = append(data, msgJSON[key])
		}

		rows = append(rows, data)
		cols = keys
	}

	return t.client.Write(ctx, cols, rows)
}

func newTimeplusOutput(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
	logger := mgr.Logger()

	baseURL, err := conf.FieldURL("url")
	if err != nil {
		return
	}

	target, err := conf.FieldString("target")
	if err != nil {
		return
	}

	stream, err := conf.FieldString("stream")
	if err != nil {
		return
	}

	var (
		apikey   string
		username string
		password string
	)
	if conf.Contains("apikey") {
		apikey, err = conf.FieldString("apikey")
		if err != nil {
			return
		}
	}
	if conf.Contains("username") {
		username, err = conf.FieldString("username")
		if err != nil {
			return
		}
	}
	if conf.Contains("password") {
		password, err = conf.FieldString("password")
		if err != nil {
			return
		}
	}

	var workspace string

	if target == http.TargetTimeplus {
		workspace, err = conf.FieldString("workspace")
		if err != nil {
			return
		}
		if len(workspace) == 0 {
			err = errors.New("workspace is required for `timeplus` target")
			return
		}
	}

	if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
		return
	}
	if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
		return
	}

	out = &timeplus{
		logger: logger,
		client: http.NewClient(logger, maxInFlight, target, baseURL, workspace, stream, apikey, username, password),
	}

	return
}
