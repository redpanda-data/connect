// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"context"
	"fmt"
	"strings"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/httpclient"
	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	sfiFieldObject      = "object"
	sfiFieldColumns     = "columns"
	sfiFieldWhere       = "where"
	sfiFieldArgsMapping = "args_mapping"
	sfiFieldPrefix      = "prefix"
	sfiFieldSuffix      = "suffix"
)

func init() {
	service.MustRegisterInput("salesforce", salesforceInputConfigSpec(), newSalesforceInput)
}

func salesforceInputConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Categories("Services").
		Summary("Executes a Salesforce SOQL query once and emits a message for each record.").
		Description(`Runs a SOQL query against the Salesforce REST API, paginates through all result pages, and emits one message per record. When results are exhausted the input shuts down, letting the pipeline terminate gracefully (or the next input in a xref:components:inputs/sequence.adoc[sequence] to take over).

== When to use this input

Use ` + "`salesforce`" + ` for:

- One-shot extracts (e.g. dump all Accounts into a warehouse).
- Periodic full-table refreshes via a scheduled pipeline or xref:components:inputs/sequence.adoc[sequence].
- Backfills and ad-hoc queries.
- Warming up a downstream pipeline before switching to CDC.

Use a different Salesforce input instead if:

- You need continuous change events — use xref:components:inputs/salesforce_cdc.adoc[` + "`salesforce_cdc`" + `].
- You need a GraphQL query (cross-object in one request) — use xref:components:inputs/salesforce_graphql.adoc[` + "`salesforce_graphql`" + `].

== Metadata

This input adds the following metadata fields to each message:

- ` + "`sobject`" + `: The sObject API name (e.g. "Account").

== Authentication

Uses the Salesforce OAuth 2.0 Client Credentials flow. Create a Connected App in Salesforce, enable OAuth settings and the Client Credentials Flow, then supply the Consumer Key as ` + "`client_id`" + ` and Consumer Secret as ` + "`client_secret`" + `.
`)

	spec = spec.Fields(authFieldSpecs()...).
		Field(service.NewStringField(sfiFieldObject).
			Description("The sObject API name to SELECT from. Case-sensitive; uses the API name, not the display label. Standard objects use the noun (`Account`, `Opportunity`); custom objects end with `__c`; Big Objects end with `__b`; External Objects end with `__x`. Confirm the exact API name in Setup → Object Manager.").
			Example("Account").
			Example("Contact").
			Example("MyCustom__c")).
		Field(service.NewStringListField(sfiFieldColumns).
			Description("Ordered list of field API names to retrieve. SOQL does not accept `*` — every field must be listed explicitly. Standard fields use their documented names; custom fields end with `__c`. Relationship fields traverse parents via dot notation (`Account.Name`, `Owner.Manager.Email`) up to 5 levels deep. Requesting a non-existent or non-queryable field fails at Connect time with a SOQL compile error.").
			Example([]string{"Id", "Name", "LastModifiedDate"}).
			Example([]string{"Id", "Account.Name", "Owner.Email"}).
			Example([]string{"Id", "MyCustom__c"})).
		Field(service.NewStringField(sfiFieldWhere).
			Description("Optional SOQL WHERE body, without the `WHERE` keyword. `?` placeholders are substituted client-side from `args_mapping` with SOQL literal escaping (quoted strings, ISO-8601 datetimes). Supports the full WHERE grammar: `AND`/`OR`/`NOT`, `LIKE`, `IN`, date literals (`TODAY`, `LAST_N_DAYS:7`), subqueries. Date/datetime comparisons require ISO-8601 with explicit timezone.").
			Example("LastModifiedDate > ?").
			Example("Status__c = ? AND CreatedDate > ?").
			Example("OwnerId IN (?, ?)").
			Optional()).
		Field(service.NewBloblangField(sfiFieldArgsMapping).
			Description("Optional xref:guides:bloblang/about.adoc[Bloblang mapping] whose result must be an array of values matching the count of `?` placeholders in `where`. Values are SOQL-escaped: strings become quoted literals, timestamps become ISO-8601, booleans and numbers pass through. The mapping is evaluated once at startup with no message context — use `now()`, `env()`, or `cache()`.").
			Example(`root = [ (now() - "1h").ts_format("2006-01-02T15:04:05Z") ]`).
			Example(`root = [ "Active", (now() - "24h").ts_format("2006-01-02T15:04:05Z") ]`).
			Optional()).
		Field(service.NewStringField(sfiFieldPrefix).
			Description("Optional SOQL fragment inserted before the SELECT keyword. Rarely needed — provided for forward compatibility with future SOQL extensions or Bulk API framing.").
			Optional().
			Advanced()).
		Field(service.NewStringField(sfiFieldSuffix).
			Description("Optional SOQL fragment appended after the WHERE clause. Typical uses: `ORDER BY` for deterministic pagination, `LIMIT` to cap result size, `FOR REFERENCE` / `FOR VIEW` to mark records for Chatter tracking.").
			Example("ORDER BY LastModifiedDate DESC").
			Example("ORDER BY Id LIMIT 1000").
			Example("ORDER BY CreatedDate DESC LIMIT 10000").
			Optional().
			Advanced()).
		Field(service.NewAutoRetryNacksToggleField()).
		Field(httpFieldSpec())

	spec = spec.Example("Recent Account Changes",
		"Query Accounts modified in the last hour and emit one message per record:",
		`
input:
  salesforce:
    org_url: https://acme.my.salesforce.com
    client_id: ${SALESFORCE_CLIENT_ID}
    client_secret: ${SALESFORCE_CLIENT_SECRET}
    object: Account
    columns: [ Id, Name, LastModifiedDate ]
    where: LastModifiedDate > ?
    args_mapping: |
      root = [ (now() - "1h").ts_format("2006-01-02T15:04:05Z") ]
`,
	)

	return spec
}

// InputConfig holds the parsed configuration for the salesforce input.
type InputConfig struct {
	Auth AuthConfig
	HTTP httpclient.Config

	Object      string
	Columns     []string
	Where       string
	ArgsMapping *bloblang.Executor
	Prefix      string
	Suffix      string
}

// NewInputConfigFromParsed parses a InputConfig from a benthos parsed config.
func NewInputConfigFromParsed(pConf *service.ParsedConfig) (InputConfig, error) {
	var (
		conf InputConfig
		err  error
	)

	if conf.Auth, err = NewAuthConfigFromParsed(pConf); err != nil {
		return conf, err
	}
	if conf.HTTP, err = newHTTPConfigFromParsed(conf.Auth.OrgURL, pConf); err != nil {
		return conf, err
	}
	if conf.Object, err = pConf.FieldString(sfiFieldObject); err != nil {
		return conf, err
	}
	if conf.Object = strings.TrimSpace(conf.Object); conf.Object == "" {
		return conf, fmt.Errorf("%s must not be empty", sfiFieldObject)
	}
	if conf.Columns, err = pConf.FieldStringList(sfiFieldColumns); err != nil {
		return conf, err
	}
	if len(conf.Columns) == 0 {
		return conf, fmt.Errorf("%s must contain at least one field", sfiFieldColumns)
	}
	if pConf.Contains(sfiFieldWhere) {
		if conf.Where, err = pConf.FieldString(sfiFieldWhere); err != nil {
			return conf, err
		}
	}
	if pConf.Contains(sfiFieldArgsMapping) {
		if conf.ArgsMapping, err = pConf.FieldBloblang(sfiFieldArgsMapping); err != nil {
			return conf, err
		}
	}
	if pConf.Contains(sfiFieldPrefix) {
		if conf.Prefix, err = pConf.FieldString(sfiFieldPrefix); err != nil {
			return conf, err
		}
	}
	if pConf.Contains(sfiFieldSuffix) {
		if conf.Suffix, err = pConf.FieldString(sfiFieldSuffix); err != nil {
			return conf, err
		}
	}
	return conf, nil
}

// readResult carries one record (or a terminal error) from the pager goroutine
// to Read. Exactly one of msg/err is non-nil; channel close signals
// end-of-input.
type readResult struct {
	msg *service.Message
	err error
}

// salesforceInput is the benthos input for the salesforce component.
type salesforceInput struct {
	conf   InputConfig
	mgr    *service.Resources
	logger *service.Logger

	resCh   chan readResult
	stopSig *shutdown.Signaller
}

func newSalesforceInput(pConf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	conf, err := NewInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}

	in := &salesforceInput{
		conf:    conf,
		mgr:     mgr,
		logger:  mgr.Logger(),
		stopSig: shutdown.NewSignaller(),
	}
	in.stopSig.TriggerHasStopped()

	return service.AutoRetryNacksToggled(pConf, in)
}

// Connect builds the HTTP client, authenticates, materialises the SOQL query
// from args_mapping, and launches the background pager goroutine that
// paginates RestQueryPage and feeds resCh. resCh is closed when no more pages
// exist; a terminal error is delivered as a final value before close.
func (s *salesforceInput) Connect(ctx context.Context) error {
	httpDoer, err := httpclient.NewClient(s.conf.HTTP, s.mgr)
	if err != nil {
		return fmt.Errorf("build http client: %w", err)
	}
	client, err := salesforcehttp.NewClient(salesforcehttp.ClientConfig{
		OrgURL:       s.conf.Auth.OrgURL,
		ClientID:     s.conf.Auth.ClientID,
		ClientSecret: s.conf.Auth.ClientSecret,
		APIVersion:   s.conf.Auth.APIVersion,
		HTTPClient:   httpDoer,
		Logger:       s.logger,
	})
	if err != nil {
		return fmt.Errorf("build salesforce http client: %w", err)
	}
	if err := client.RefreshToken(ctx); err != nil {
		return fmt.Errorf("salesforce auth: %w", err)
	}

	args, err := evalArgsMapping(s.conf.ArgsMapping)
	if err != nil {
		return fmt.Errorf("evaluate %s: %w", sfiFieldArgsMapping, err)
	}
	soql, err := buildSOQL(s.conf.Object, s.conf.Columns, s.conf.Where, s.conf.Prefix, s.conf.Suffix, args)
	if err != nil {
		return fmt.Errorf("build SOQL: %w", err)
	}
	s.logger.Debugf("salesforce SOQL: %s", soql)

	s.resCh = make(chan readResult)
	s.stopSig = shutdown.NewSignaller()

	go func() {
		defer s.stopSig.TriggerHasStopped()
		defer close(s.resCh)

		runCtx, cancel := s.stopSig.SoftStopCtx(context.Background())
		defer cancel()

		nextURL := ""
		for {
			batch, next, err := client.RestQueryPage(runCtx, soql, nextURL)
			if err != nil {
				if runCtx.Err() == nil {
					select {
					case s.resCh <- readResult{err: err}:
					case <-runCtx.Done():
					}
				}
				return
			}
			for _, msg := range batch {
				msg.MetaSet("sobject", s.conf.Object)
				select {
				case s.resCh <- readResult{msg: msg}:
				case <-runCtx.Done():
					return
				}
			}
			if next == "" {
				return
			}
			nextURL = next
		}
	}()
	return nil
}

// evalArgsMapping runs an args_mapping Bloblang against an empty message and
// returns the resulting []any. A nil executor produces nil args.
func evalArgsMapping(mapping *bloblang.Executor) ([]any, error) {
	if mapping == nil {
		return nil, nil
	}
	raw, err := mapping.Query(nil)
	if err != nil {
		return nil, err
	}
	args, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("must produce an array, got %T", raw)
	}
	return args, nil
}

// Read returns the next record, or ErrEndOfInput once the pager has drained
// all pages.
func (s *salesforceInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case res, ok := <-s.resCh:
		if !ok {
			return nil, nil, service.ErrEndOfInput
		}
		if res.err != nil {
			return nil, nil, res.err
		}
		return res.msg, func(context.Context, error) error { return nil }, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// Close stops the pager goroutine and waits for it to drain.
func (s *salesforceInput) Close(ctx context.Context) error {
	s.stopSig.TriggerSoftStop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopSig.HasStoppedChan():
		return nil
	}
}
