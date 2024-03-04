package slack

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/slack-go/slack"
)

// ConfigSpec returns the configuration specification for the Slack webhook output.
func ConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services", "Social").
		Summary("Post messages to Slack via webhook.").
		Description(`
		This output POSTs messages to a Slack channel via webhook.
		The format of a message should be a JSON object should match the [Golang Slack WebhookMessage struct](https://github.com/slack-go/slack/blob/v0.12.5/webhooks.go#L13) type`).
		Fields(
			service.NewStringField("webhook").
				Description("Slack webhook URL to post messages"),
		)
}

func init() {
	err := service.RegisterOutput(
		"slack_webhook", ConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			w, err := newWriter(conf, mgr)
			return w, 1, err
		},
	)
	if err != nil {
		panic(err)
	}
}

type writer struct {
	log *service.Logger

	// Config
	webhook string

	httpClient *http.Client
}

// newWriter creates a new instance of the Slack webhook output.
func newWriter(conf *service.ParsedConfig, mgr *service.Resources) (*writer, error) {
	w := &writer{
		log: mgr.Logger(),
	}
	var err error
	if w.webhook, err = conf.FieldString("webhook"); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *writer) Connect(ctx context.Context) error {
	w.httpClient = &http.Client{
		Timeout: 5 * time.Second,
	}
	w.log.Infof("Writing Slack messages with webhook: %s", w.webhook)
	return nil
}

func (w *writer) Write(ctx context.Context, msg *service.Message) error {
	rawContent, err := msg.AsBytes()
	if err != nil {
		return err
	}

	var slackMsg slack.WebhookMessage
	if err := json.Unmarshal(rawContent, &slackMsg); err != nil {
		w.log.Errorf("Failed to parse the object for Slack schema '%v': %v", string(rawContent), err)
		return err
	}

	// Post the message to the Slack channel using the webhook URL
	err = slack.PostWebhookCustomHTTPContext(ctx, w.webhook, w.httpClient, &slackMsg)
	if err != nil {
		w.log.Errorf("Failed to post message to Slack: %v", err)
		return err
	}
	w.log.Debugf("Message sent to Slack: %v", string(rawContent))
	return nil
}

func (w *writer) Close(ctx context.Context) error {
	// No close logic required for webhook output.
	return nil
}
