// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cohere

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	cohere "github.com/cohere-ai/cohere-go/v2"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	ccpFieldUserPrompt       = "prompt"
	ccpFieldSystemPrompt     = "system_prompt"
	ccpFieldMaxTokens        = "max_tokens"
	ccpFieldTemp             = "temperature"
	ccpFieldTopP             = "top_p"
	ccpFieldSeed             = "seed"
	ccpFieldStop             = "stop"
	ccpFieldPresencePenalty  = "presence_penalty"
	ccpFieldFrequencyPenalty = "frequency_penalty"
	ccpFieldResponseFormat   = "response_format"
	// JSON schema fields
	ccpFieldJSONSchema = "json_schema"
	// Schema registry fields
	ccpFieldSchemaRegistry                = "schema_registry"
	ccpFieldSchemaRegistrySubject         = "subject"
	ccpFieldSchemaRegistryRefreshInterval = "refresh_interval"
	ccpFieldSchemaRegistryURL             = "url"
	ccpFieldSchemaRegistryTLS             = "tls"
)

func init() {
	err := service.RegisterProcessor(
		"cohere_chat",
		chatProcessorConfig(),
		makeChatProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func chatProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Generates responses to messages in a chat conversation, using the Cohere API.").
		Description(`
This processor sends the contents of user prompts to the Cohere API, which generates responses. By default, the processor submits the entire payload of each message as a string, unless you use the `+"`"+ccpFieldUserPrompt+"`"+` configuration field to customize it.

To learn more about chat completion, see the https://docs.cohere.com/docs/chat-api[Cohere API documentation^].`).
		Version("4.37.0").
		Fields(
			baseConfigFieldsWithModels(
				"command-r-plus",
				"command-r",
				"command",
				"command-light",
			)...,
		).
		Fields(
			service.NewInterpolatedStringField(ccpFieldUserPrompt).
				Description("The user prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
				Optional(),
			service.NewInterpolatedStringField(ccpFieldSystemPrompt).
				Description("The system prompt to submit along with the user prompt.").
				Optional(),
			service.NewIntField(ccpFieldMaxTokens).
				Optional().
				Description("The maximum number of tokens that can be generated in the chat completion."),
			service.NewFloatField(ccpFieldTemp).
				Optional().
				Description(`What sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic.

We generally recommend altering this or top_p but not both.`).
				LintRule(`root = if this > 2 || this < 0 { [ "field must be between 0 and 2" ] }`),
			service.NewStringEnumField(ccpFieldResponseFormat, "text", "json", "json_schema").
				Default("text").
				Description("Specify the model's output format. If `json_schema` is specified, then additionally a `json_schema` or `schema_registry` must be configured."),
			service.NewStringField(ccpFieldJSONSchema).
				Optional().
				Description("The JSON schema to use when responding in `json_schema` format. To learn more about what JSON schema is supported see the https://docs.cohere.com/docs/structured-outputs-json[Cohere documentation^]."),
			service.NewObjectField(
				ccpFieldSchemaRegistry,
				slices.Concat(
					[]*service.ConfigField{
						service.NewURLField(ccpFieldSchemaRegistryURL).Description("The base URL of the schema registry service."),
						service.NewStringField(ccpFieldSchemaRegistrySubject).
							Description("The subject name to fetch the schema for."),
						service.NewDurationField(ccpFieldSchemaRegistryRefreshInterval).
							Optional().
							Description("The refresh rate for getting the latest schema. If not specified the schema does not refresh."),
						service.NewTLSField(ccpFieldSchemaRegistryTLS),
					},
					service.NewHTTPRequestAuthSignerFields(),
				)...,
			).
				Description("The schema registry to dynamically load schemas from when responding in `json_schema` format. Schemas themselves must be in JSON format. To learn more about what JSON schema is supported see the https://docs.cohere.com/docs/structured-outputs-json[Cohere documentation^].").
				Optional().
				Advanced(),
			service.NewFloatField(ccpFieldTopP).
				Optional().
				Advanced().
				Description(`An alternative to sampling with temperature, called nucleus sampling, where the model considers the results of the tokens with top_p probability mass. So 0.1 means only the tokens comprising the top 10% probability mass are considered.

We generally recommend altering this or temperature but not both.`).
				LintRule(`root = if this > 1 || this < 0 { [ "field must be between 0 and 1" ] }`),
			service.NewFloatField(ccpFieldFrequencyPenalty).
				Optional().
				Advanced().
				Description("Number between -2.0 and 2.0. Positive values penalize new tokens based on their existing frequency in the text so far, decreasing the model's likelihood to repeat the same line verbatim.").
				LintRule(`root = if this > 2 || this < -2 { [ "field must be less than 2 and greater than -2" ] }`),
			service.NewFloatField(ccpFieldPresencePenalty).
				Optional().
				Advanced().
				Description("Number between -2.0 and 2.0. Positive values penalize new tokens based on whether they appear in the text so far, increasing the model's likelihood to talk about new topics.").
				LintRule(`root = if this > 2 || this < -2 { [ "field must be less than 2 and greater than -2" ] }`),
			service.NewIntField(ccpFieldSeed).
				Advanced().
				Optional().
				Description("If specified, our system will make a best effort to sample deterministically, such that repeated requests with the same seed and parameters should return the same result. Determinism is not guaranteed."),
			service.NewStringListField(ccpFieldStop).
				Optional().
				Advanced().
				Description("Up to 4 sequences where the API will stop generating further tokens."),
		).LintRule(`
      root = match {
        this.exists("` + ccpFieldJSONSchema + `") && this.exists("` + ccpFieldSchemaRegistry + `") => ["cannot set both ` + "`" + ccpFieldJSONSchema + "`" + ` and ` + "`" + ccpFieldSchemaRegistry + "`" + `"]
        this.response_format == "json_schema" && !this.exists("` + ccpFieldJSONSchema + `") && !this.exists("` + ccpFieldSchemaRegistry + `") => ["schema must be specified using either ` + "`" + ccpFieldJSONSchema + "`" + ` or ` + "`" + ccpFieldSchemaRegistry + "`" + `"]
      }
    `)
}

func makeChatProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	b, err := newBaseProcessor(conf)
	if err != nil {
		return nil, err
	}
	var up *service.InterpolatedString
	if conf.Contains(ccpFieldUserPrompt) {
		up, err = conf.FieldInterpolatedString(ccpFieldUserPrompt)
		if err != nil {
			return nil, err
		}
	}
	var sp *service.InterpolatedString
	if conf.Contains(ccpFieldSystemPrompt) {
		sp, err = conf.FieldInterpolatedString(ccpFieldSystemPrompt)
		if err != nil {
			return nil, err
		}
	}
	var maxTokens *int
	if conf.Contains(ccpFieldMaxTokens) {
		mt, err := conf.FieldInt(ccpFieldMaxTokens)
		if err != nil {
			return nil, err
		}
		maxTokens = &mt
	}
	var temp *float64
	if conf.Contains(ccpFieldTemp) {
		ft, err := conf.FieldFloat(ccpFieldTemp)
		if err != nil {
			return nil, err
		}
		temp = &ft
	}
	var topP *float64
	if conf.Contains(ccpFieldTopP) {
		v, err := conf.FieldFloat(ccpFieldTopP)
		if err != nil {
			return nil, err
		}
		topP = &v
	}
	var frequencyPenalty *float64
	if conf.Contains(ccpFieldFrequencyPenalty) {
		v, err := conf.FieldFloat(ccpFieldFrequencyPenalty)
		if err != nil {
			return nil, err
		}
		frequencyPenalty = &v
	}
	var presencePenalty *float64
	if conf.Contains(ccpFieldPresencePenalty) {
		v, err := conf.FieldFloat(ccpFieldPresencePenalty)
		if err != nil {
			return nil, err
		}
		presencePenalty = &v
	}
	var seed *int
	if conf.Contains(ccpFieldSeed) {
		intSeed, err := conf.FieldInt(ccpFieldSeed)
		if err != nil {
			return nil, err
		}
		seed = &intSeed
	}
	var stop []string
	if conf.Contains(ccpFieldStop) {
		stop, err = conf.FieldStringList(ccpFieldStop)
		if err != nil {
			return nil, err
		}
	}
	v, err := conf.FieldString(ccpFieldResponseFormat)
	if err != nil {
		return nil, err
	}
	var responseFormat cohere.ResponseFormat
	var schemaProvider jsonSchemaProvider
	switch v {
	case "json":
		fallthrough
	case "json_object":
		responseFormat.Type = "json_object"
		responseFormat.JsonObject = &cohere.JsonResponseFormat{}
	case "json_schema":
		responseFormat.Type = "json_object"
		responseFormat.JsonObject = &cohere.JsonResponseFormat{}
		if conf.Contains(ccpFieldJSONSchema) {
			schemaProvider, err = newFixedSchemaProvider(conf)
			if err != nil {
				return nil, err
			}
		} else if conf.Contains(ccpFieldSchemaRegistry) {
			schemaProvider, err = newDynamicSchemaProvider(conf.Namespace(ccpFieldSchemaRegistry), mgr)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("using %s %q, but did not specify %s or %s", ccpFieldResponseFormat, v, ccpFieldJSONSchema, ccpFieldSchemaRegistry)
		}
	case "text":
		responseFormat.Type = "text"
		responseFormat.Text = &cohere.TextResponseFormat{}
	default:
		return nil, fmt.Errorf("unknown %s: %q", ccpFieldResponseFormat, v)
	}
	return &chatProcessor{b, up, sp, maxTokens, temp, topP, frequencyPenalty, presencePenalty, seed, stop, responseFormat, schemaProvider}, nil
}

func newFixedSchemaProvider(conf *service.ParsedConfig) (jsonSchemaProvider, error) {
	schema, err := conf.FieldString(ccpFieldJSONSchema)
	if err != nil {
		return nil, err
	}
	return newFixedSchema(schema)
}

func newDynamicSchemaProvider(conf *service.ParsedConfig, mgr *service.Resources) (jsonSchemaProvider, error) {
	url, err := conf.FieldString(ccpFieldSchemaRegistryURL)
	if err != nil {
		return nil, err
	}
	reqSigner, err := conf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, err
	}
	tlsConfig, err := conf.FieldTLS(ccpFieldSchemaRegistryTLS)
	if err != nil {
		return nil, err
	}
	client, err := sr.NewClient(url, reqSigner, tlsConfig, mgr)
	if err != nil {
		return nil, fmt.Errorf("unable to create schema registry client: %w", err)
	}
	subject, err := conf.FieldString(ccpFieldSchemaRegistrySubject)
	if err != nil {
		return nil, err
	}
	var refreshInterval time.Duration = math.MaxInt64
	if conf.Contains(ccpFieldSchemaRegistryRefreshInterval) {
		refreshInterval, err = conf.FieldDuration(ccpFieldSchemaRegistryRefreshInterval)
		if err != nil {
			return nil, err
		}
	}
	return newDynamicSchema(client, subject, refreshInterval), nil
}

type chatProcessor struct {
	*baseProcessor

	userPrompt       *service.InterpolatedString
	systemPrompt     *service.InterpolatedString
	maxTokens        *int
	temperature      *float64
	topP             *float64
	frequencyPenalty *float64
	presencePenalty  *float64
	seed             *int
	stop             []string
	responseFormat   cohere.ResponseFormat
	schemaProvider   jsonSchemaProvider
}

func (p *chatProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var body cohere.ChatRequest
	body.Model = &p.model
	body.MaxTokens = p.maxTokens
	body.Temperature = p.temperature
	body.P = p.topP
	body.Seed = p.seed
	body.FrequencyPenalty = p.frequencyPenalty
	body.PresencePenalty = p.presencePenalty
	body.ResponseFormat = &p.responseFormat
	if p.schemaProvider != nil {
		s, err := p.schemaProvider.GetJSONSchema(ctx)
		if err != nil {
			return nil, err
		}
		body.ResponseFormat.JsonObject.Schema = s
	}
	body.StopSequences = p.stop
	if p.systemPrompt != nil {
		s, err := p.systemPrompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", ccpFieldSystemPrompt, err)
		}
		body.Preamble = &s
	}
	if p.userPrompt != nil {
		s, err := p.userPrompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", ccpFieldUserPrompt, err)
		}
		body.Message = s
	} else {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		body.Message = string(b)
	}
	resp, err := p.client.Chat(ctx, &body)
	if err != nil {
		return nil, err
	}
	msg = msg.Copy()
	msg.SetBytes([]byte(resp.Text))
	return service.MessageBatch{msg}, nil
}
