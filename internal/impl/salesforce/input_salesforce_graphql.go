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
	sfgqlFieldQuery     = "query"
	sfgqlFieldVariables = "variables"
)

func init() {
	service.MustRegisterInput("salesforce_graphql", salesforceGraphQLInputConfigSpec(), newSalesforceGraphQLInput)
}

func salesforceGraphQLInputConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Categories("Services").
		Summary("Executes a Salesforce GraphQL (UIAPI) query once and emits a message for each record.").
		Description(`Executes a GraphQL query against the Salesforce UIAPI (` + "`POST /services/data/{api_version}/graphql`" + `), walks the response tree, and emits one message per record. When records are exhausted the input shuts down, letting the pipeline terminate gracefully (or the next input in a xref:components:inputs/sequence.adoc[sequence] to take over).

== When to use this input

Use ` + "`salesforce_graphql`" + ` for:

- Cross-object queries in a single request (parent + children + grandchildren).
- Response shapes that already match your downstream schema.
- Queries benefiting from GraphQL's field-level selection and nesting.
- Adopting Salesforce's UIAPI / future-forward query surface.

Use a different Salesforce input instead if:

- You only need single-object SELECTs — use xref:components:inputs/salesforce.adoc[` + "`salesforce`" + `] (simpler, no GraphQL schema knowledge needed).
- You need continuous change events — use xref:components:inputs/salesforce_cdc.adoc[` + "`salesforce_cdc`" + `].

== Pagination

When the query selects an ` + "`edges`" + `/` + "`pageInfo`" + ` connection, the input transparently paginates by injecting ` + "`after: \"<cursor>\"`" + ` into the query string between requests. The query must include ` + "`pageInfo { hasNextPage endCursor }`" + ` for pagination to terminate cleanly. Responses without an ` + "`edges`" + ` array are emitted as a single message and the input completes.

== Metadata

This input adds no Salesforce-specific metadata. GraphQL response shapes vary by query, so record identity and context travel in the message body.

== Authentication

Uses the Salesforce OAuth 2.0 Client Credentials flow. Create a Connected App in Salesforce, enable OAuth settings and the Client Credentials Flow, grant the ` + "`api`" + ` OAuth scope, then supply the Consumer Key as ` + "`client_id`" + ` and Consumer Secret as ` + "`client_secret`" + `. The Connected App must have access to the target UIAPI objects.
`)

	spec = spec.Fields(authFieldSpecs()...).
		Field(service.NewStringField(sfgqlFieldQuery).
			Description("The GraphQL query document as a single string. Must target the Salesforce UIAPI schema (`uiapi.query.*` or `uiapi.mutation.*`). Typically follows the UIAPI convention of nested `edges { node { field { value } } }`. Variables are referenced with `$name` and supplied via the `variables` field. For automatic pagination, include `pageInfo { hasNextPage endCursor }` in the relevant connection.").
			Example(`query Accounts { uiapi { query { Account { edges { node { Id { value } Name { value } } } pageInfo { hasNextPage endCursor } } } } }`).
			Example(`query Accounts($first: Int) { uiapi { query { Account(first: $first) { edges { node { Id { value } Name { value } } } pageInfo { hasNextPage endCursor } } } } }`)).
		Field(service.NewBloblangField(sfgqlFieldVariables).
			Description("Optional xref:guides:bloblang/about.adoc[Bloblang mapping] whose result must be an object whose keys are GraphQL variable names referenced by `query`. Values pass through as JSON in the request body's `variables` field. The mapping is evaluated once at startup with no message context — use `env()`, `now()`, `cache()`, or static literals.").
			Example(`root = {"first": 100}`).
			Example(`root = {"since": now().ts_format("2006-01-02T15:04:05Z"), "limit": 500}`).
			Optional()).
		Field(service.NewAutoRetryNacksToggleField()).
		Field(httpFieldSpec())

	spec = spec.Example("Account snapshot via GraphQL",
		"Fetch the first 500 Accounts with a nested UIAPI query:",
		`
input:
  salesforce_graphql:
    org_url: https://acme.my.salesforce.com
    client_id: ${SALESFORCE_CLIENT_ID}
    client_secret: ${SALESFORCE_CLIENT_SECRET}
    query: |
      query Accounts($limit: Int) {
        uiapi {
          query {
            Account(first: $limit) {
              edges {
                node {
                  Id { value }
                  Name { value }
                  LastModifiedDate { value }
                }
              }
              pageInfo { hasNextPage endCursor }
            }
          }
        }
      }
    variables: |
      root = {"limit": 500}
`,
	)

	return spec
}

// GraphQLInputConfig holds the parsed configuration for the salesforce_graphql input.
type GraphQLInputConfig struct {
	Auth AuthConfig
	HTTP httpclient.Config

	Query     string
	Variables *bloblang.Executor
}

// NewGraphQLInputConfigFromParsed parses a GraphQLInputConfig from a benthos parsed config.
func NewGraphQLInputConfigFromParsed(pConf *service.ParsedConfig) (GraphQLInputConfig, error) {
	var (
		conf GraphQLInputConfig
		err  error
	)

	if conf.Auth, err = NewAuthConfigFromParsed(pConf); err != nil {
		return conf, err
	}
	if conf.HTTP, err = newHTTPConfigFromParsed(conf.Auth.OrgURL, pConf); err != nil {
		return conf, err
	}
	if conf.Query, err = pConf.FieldString(sfgqlFieldQuery); err != nil {
		return conf, err
	}
	if conf.Query = strings.TrimSpace(conf.Query); conf.Query == "" {
		return conf, fmt.Errorf("%s must not be empty", sfgqlFieldQuery)
	}
	if pConf.Contains(sfgqlFieldVariables) {
		if conf.Variables, err = pConf.FieldBloblang(sfgqlFieldVariables); err != nil {
			return conf, err
		}
	}
	return conf, nil
}

// salesforceGraphQLInput is the benthos input for the salesforce_graphql component.
type salesforceGraphQLInput struct {
	conf   GraphQLInputConfig
	mgr    *service.Resources
	logger *service.Logger

	resCh   chan readResult
	stopSig *shutdown.Signaller
}

func newSalesforceGraphQLInput(pConf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	conf, err := NewGraphQLInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}

	in := &salesforceGraphQLInput{
		conf:    conf,
		mgr:     mgr,
		logger:  mgr.Logger(),
		stopSig: shutdown.NewSignaller(),
	}
	in.stopSig.TriggerHasStopped()

	return service.AutoRetryNacksToggled(pConf, in)
}

// Connect builds the HTTP client, authenticates, evaluates variables, and
// launches the background pager goroutine that paginates
// GraphQLQueryPageWithVariables and feeds resCh. resCh is closed when no more
// pages exist; a terminal error is delivered as a final value before close.
func (s *salesforceGraphQLInput) Connect(ctx context.Context) error {
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

	variables, err := evalGraphQLVariables(s.conf.Variables)
	if err != nil {
		return fmt.Errorf("evaluate %s: %w", sfgqlFieldVariables, err)
	}

	s.resCh = make(chan readResult)
	s.stopSig = shutdown.NewSignaller()

	go func() {
		defer s.stopSig.TriggerHasStopped()
		defer close(s.resCh)

		runCtx, cancel := s.stopSig.SoftStopCtx(context.Background())
		defer cancel()

		cursor := ""
		for {
			batch, next, err := client.GraphQLQueryPageWithVariables(runCtx, s.conf.Query, variables, cursor)
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
				select {
				case s.resCh <- readResult{msg: msg}:
				case <-runCtx.Done():
					return
				}
			}
			if next == "" {
				return
			}
			cursor = next
		}
	}()
	return nil
}

// evalGraphQLVariables runs a variables Bloblang against an empty message and
// returns the result as a map. nil mapping or an empty map produces nil.
func evalGraphQLVariables(mapping *bloblang.Executor) (map[string]any, error) {
	if mapping == nil {
		return nil, nil
	}
	raw, err := mapping.Query(nil)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("must produce an object, got %T", raw)
	}
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}

// Read returns the next record, or ErrEndOfInput once the pager has drained
// all pages.
func (s *salesforceGraphQLInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
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
func (s *salesforceGraphQLInput) Close(ctx context.Context) error {
	s.stopSig.TriggerSoftStop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopSig.HasStoppedChan():
		return nil
	}
}
