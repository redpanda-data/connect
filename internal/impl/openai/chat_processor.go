// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package openai

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	oai "github.com/sashabaranov/go-openai"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

const (
	ocpFieldUserPrompt       = "prompt"
	ocpFieldSystemPrompt     = "system_prompt"
	ocpFieldMaxTokens        = "max_tokens"
	ocpFieldTemp             = "temperature"
	ocpFieldUser             = "user"
	ocpFieldTopP             = "top_p"
	ocpFieldSeed             = "seed"
	ocpFieldStop             = "stop"
	ocpFieldPresencePenalty  = "presence_penalty"
	ocpFieldFrequencyPenalty = "frequency_penalty"
	ocpFieldResponseFormat   = "response_format"
	// JSON schema fields
	ocpFieldJSONSchema       = "json_schema"
	ocpFieldJSONSchemaName   = "name"
	ocpFieldJSONSchemaDesc   = "description"
	ocpFieldJSONSchemaSchema = "schema"
	// Schema registry fields
	ocpFieldSchemaRegistry                = "schema_registry"
	ocpFieldSchemaRegistrySubject         = "subject"
	ocpFieldSchemaRegistryRefreshInterval = "refresh_interval"
	ocpFieldSchemaRegistryNamePrefix      = "name_prefix"
	ocpFieldSchemaRegistryURL             = "url"
	ocpFieldSchemaRegistryTLS             = "tls"
)

func init() {
	err := service.RegisterProcessor(
		"openai_chat_completion",
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
		Summary("Generates responses to messages in a chat conversation, using the OpenAI API.").
		Description(`
This processor sends the contents of user prompts to the OpenAI API, which generates responses. By default, the processor submits the entire payload of each message as a string, unless you use the `+"`"+ocpFieldUserPrompt+"`"+` configuration field to customize it.

To learn more about chat completion, see the https://platform.openai.com/docs/guides/chat-completions[OpenAI API documentation^].`).
		Version("4.32.0").
		Fields(
			baseConfigFieldsWithModels(
				"gpt-4o",
				"gpt-4o-mini",
				"gpt-4",
				"gpt4-turbo",
			)...,
		).
		Fields(
			service.NewInterpolatedStringField(ocpFieldUserPrompt).
				Description("The user prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
				Optional(),
			service.NewInterpolatedStringField(ocpFieldSystemPrompt).
				Description("The system prompt to submit along with the user prompt.").
				Optional(),
			service.NewIntField(ocpFieldMaxTokens).
				Optional().
				Description("The maximum number of tokens that can be generated in the chat completion."),
			service.NewFloatField(ocpFieldTemp).
				Optional().
				Description(`What sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic.

We generally recommend altering this or top_p but not both.`).
				LintRule(`root = if this > 2 || this < 0 { [ "field must be between 0 and 2" ] }`),
			service.NewInterpolatedStringField(ocpFieldUser).
				Optional().
				Description("A unique identifier representing your end-user, which can help OpenAI to monitor and detect abuse."),
			service.NewStringEnumField(ocpFieldResponseFormat, "text", "json", "json_schema").
				Default("text").
				Description("Specify the model's output format. If `json_schema` is specified, then additionally a `json_schema` or `schema_registry` must be configured."),
			service.NewObjectField(ocpFieldJSONSchema,
				service.NewStringField(ocpFieldJSONSchemaName).Description("The name of the schema."),
				service.NewStringField(ocpFieldJSONSchemaDesc).Optional().Advanced().Description("Additional description of the schema for the LLM."),
				service.NewStringField(ocpFieldJSONSchemaSchema).Description("The JSON schema for the LLM to use when generating the output."),
			).
				Optional().
				Description("The JSON schema to use when responding in `json_schema` format. To learn more about what JSON schema is supported see the https://platform.openai.com/docs/guides/structured-outputs/supported-schemas[OpenAI documentation^]."),
			service.NewObjectField(
				ocpFieldSchemaRegistry,
				slices.Concat(
					[]*service.ConfigField{
						service.NewURLField(ocpFieldSchemaRegistryURL).Description("The base URL of the schema registry service."),
						service.NewStringField(ocpFieldSchemaRegistryNamePrefix).
							Default("schema_registry_id_").
							Description("The prefix of the name for this schema, the schema ID is used as a suffix."),
						service.NewStringField(ocpFieldSchemaRegistrySubject).
							Description("The subject name to fetch the schema for."),
						service.NewDurationField(ocpFieldSchemaRegistryRefreshInterval).
							Optional().
							Description("The refresh rate for getting the latest schema. If not specified the schema does not refresh."),
						service.NewTLSField(ocpFieldSchemaRegistryTLS),
					},
					service.NewHTTPRequestAuthSignerFields(),
				)...,
			).
				Description("The schema registry to dynamically load schemas from when responding in `json_schema` format. Schemas themselves must be in JSON format. To learn more about what JSON schema is supported see the https://platform.openai.com/docs/guides/structured-outputs/supported-schemas[OpenAI documentation^].").
				Optional().
				Advanced(),
			service.NewFloatField(ocpFieldTopP).
				Optional().
				Advanced().
				Description(`An alternative to sampling with temperature, called nucleus sampling, where the model considers the results of the tokens with top_p probability mass. So 0.1 means only the tokens comprising the top 10% probability mass are considered.

We generally recommend altering this or temperature but not both.`).
				LintRule(`root = if this > 1 || this < 0 { [ "field must be between 0 and 1" ] }`),
			service.NewFloatField(ocpFieldFrequencyPenalty).
				Optional().
				Advanced().
				Description("Number between -2.0 and 2.0. Positive values penalize new tokens based on their existing frequency in the text so far, decreasing the model's likelihood to repeat the same line verbatim.").
				LintRule(`root = if this > 2 || this < -2 { [ "field must be less than 2 and greater than -2" ] }`),
			service.NewFloatField(ocpFieldPresencePenalty).
				Optional().
				Advanced().
				Description("Number between -2.0 and 2.0. Positive values penalize new tokens based on whether they appear in the text so far, increasing the model's likelihood to talk about new topics.").
				LintRule(`root = if this > 2 || this < -2 { [ "field must be less than 2 and greater than -2" ] }`),
			service.NewIntField(ocpFieldSeed).
				Advanced().
				Optional().
				Description("If specified, our system will make a best effort to sample deterministically, such that repeated requests with the same seed and parameters should return the same result. Determinism is not guaranteed."),
			service.NewStringListField(ocpFieldStop).
				Optional().
				Advanced().
				Description("Up to 4 sequences where the API will stop generating further tokens."),
		).LintRule(`
      root = match {
        this.exists("` + ocpFieldJSONSchema + `") && this.exists("` + ocpFieldSchemaRegistry + `") => ["cannot set both ` + "`" + ocpFieldJSONSchema + "`" + ` and ` + "`" + ocpFieldSchemaRegistry + "`" + `"]
        this.response_format == "json_schema" && !this.exists("` + ocpFieldJSONSchema + `") && !this.exists("` + ocpFieldSchemaRegistry + `") => ["schema must be specified using either ` + "`" + ocpFieldJSONSchema + "`" + ` or ` + "`" + ocpFieldSchemaRegistry + "`" + `"]
      }
    `)
}

func makeChatProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	b, err := newBaseProcessor(conf)
	if err != nil {
		return nil, err
	}
	var up *service.InterpolatedString
	if conf.Contains(ocpFieldUserPrompt) {
		up, err = conf.FieldInterpolatedString(ocpFieldUserPrompt)
		if err != nil {
			return nil, err
		}
	}
	var sp *service.InterpolatedString
	if conf.Contains(ocpFieldSystemPrompt) {
		sp, err = conf.FieldInterpolatedString(ocpFieldSystemPrompt)
		if err != nil {
			return nil, err
		}
	}
	var maxTokens *int
	if conf.Contains(ocpFieldMaxTokens) {
		mt, err := conf.FieldInt(ocpFieldMaxTokens)
		if err != nil {
			return nil, err
		}
		maxTokens = &mt
	}
	var temp *float32
	if conf.Contains(ocpFieldTemp) {
		ft, err := conf.FieldFloat(ocpFieldTemp)
		if err != nil {
			return nil, err
		}
		t := float32(ft)
		temp = &t
	}
	var user *service.InterpolatedString
	if conf.Contains(ocpFieldUser) {
		user, err = conf.FieldInterpolatedString(ocpFieldUser)
		if err != nil {
			return nil, err
		}
	}
	var topP *float32
	if conf.Contains(ocpFieldTopP) {
		v, err := conf.FieldFloat(ocpFieldTopP)
		if err != nil {
			return nil, err
		}
		tp := float32(v)
		topP = &tp
	}
	var frequencyPenalty *float32
	if conf.Contains(ocpFieldFrequencyPenalty) {
		v, err := conf.FieldFloat(ocpFieldFrequencyPenalty)
		if err != nil {
			return nil, err
		}
		fp := float32(v)
		frequencyPenalty = &fp
	}
	var presencePenalty *float32
	if conf.Contains(ocpFieldPresencePenalty) {
		v, err := conf.FieldFloat(ocpFieldPresencePenalty)
		if err != nil {
			return nil, err
		}
		pp := float32(v)
		presencePenalty = &pp
	}
	var seed *int
	if conf.Contains(ocpFieldSeed) {
		intSeed, err := conf.FieldInt(ocpFieldSeed)
		if err != nil {
			return nil, err
		}
		seed = &intSeed
	}
	var stop []string
	if conf.Contains(ocpFieldStop) {
		stop, err = conf.FieldStringList(ocpFieldStop)
		if err != nil {
			return nil, err
		}
	}
	v, err := conf.FieldString(ocpFieldResponseFormat)
	if err != nil {
		return nil, err
	}
	var responseFormat oai.ChatCompletionResponseFormatType
	var schemaProvider jsonSchemaProvider
	switch v {
	case "json":
		fallthrough
	case "json_object":
		responseFormat = oai.ChatCompletionResponseFormatTypeJSONObject
	case "json_schema":
		responseFormat = oai.ChatCompletionResponseFormatTypeJSONSchema
		if conf.Contains(ocpFieldJSONSchema) {
			schemaProvider, err = newFixedSchemaProvider(conf.Namespace(ocpFieldJSONSchema))
			if err != nil {
				return nil, err
			}
		} else if conf.Contains(ocpFieldSchemaRegistry) {
			schemaProvider, err = newDynamicSchemaProvider(conf.Namespace(ocpFieldSchemaRegistry), mgr)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("using %s %q, but did not specify %s or %s", ocpFieldResponseFormat, v, ocpFieldJSONSchema, ocpFieldSchemaRegistry)
		}
	case "text":
		responseFormat = oai.ChatCompletionResponseFormatTypeText
	default:
		return nil, fmt.Errorf("unknown %s: %q", ocpFieldResponseFormat, v)
	}
	return &chatProcessor{b, up, sp, maxTokens, temp, user, topP, frequencyPenalty, presencePenalty, seed, stop, responseFormat, schemaProvider}, nil
}

func newFixedSchemaProvider(conf *service.ParsedConfig) (jsonSchemaProvider, error) {
	name, err := conf.FieldString(ocpFieldJSONSchemaName)
	if err != nil {
		return nil, err
	}
	description := ""
	if conf.Contains(ocpFieldJSONSchemaDesc) {
		description, err = conf.FieldString(ocpFieldJSONSchemaDesc)
		if err != nil {
			return nil, err
		}
	}
	schema, err := conf.FieldString(ocpFieldJSONSchemaSchema)
	if err != nil {
		return nil, err
	}
	return newFixedSchema(name, description, schema)
}

func newDynamicSchemaProvider(conf *service.ParsedConfig, mgr *service.Resources) (jsonSchemaProvider, error) {
	url, err := conf.FieldString(ocpFieldSchemaRegistryURL)
	if err != nil {
		return nil, err
	}
	reqSigner, err := conf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, err
	}
	tlsConfig, err := conf.FieldTLS(ocpFieldSchemaRegistryTLS)
	if err != nil {
		return nil, err
	}
	client, err := sr.NewClient(url, reqSigner, tlsConfig, mgr)
	if err != nil {
		return nil, fmt.Errorf("unable to create schema registry client: %w", err)
	}
	subject, err := conf.FieldString(ocpFieldSchemaRegistrySubject)
	if err != nil {
		return nil, err
	}
	var refreshInterval time.Duration = math.MaxInt64
	if conf.Contains(ocpFieldSchemaRegistryRefreshInterval) {
		refreshInterval, err = conf.FieldDuration(ocpFieldSchemaRegistryRefreshInterval)
		if err != nil {
			return nil, err
		}
	}
	namePrefix, err := conf.FieldString(ocpFieldSchemaRegistryNamePrefix)
	if err != nil {
		return nil, err
	}
	return newDynamicSchema(client, subject, namePrefix, refreshInterval), nil
}

type chatProcessor struct {
	*baseProcessor

	userPrompt       *service.InterpolatedString
	systemPrompt     *service.InterpolatedString
	maxTokens        *int
	temperature      *float32
	user             *service.InterpolatedString
	topP             *float32
	frequencyPenalty *float32
	presencePenalty  *float32
	seed             *int
	stop             []string
	responseFormat   oai.ChatCompletionResponseFormatType
	schemaProvider   jsonSchemaProvider
}

func (p *chatProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var body oai.ChatCompletionRequest
	body.Model = p.model
	if p.maxTokens != nil {
		body.MaxTokens = *p.maxTokens
	}
	if p.temperature != nil {
		body.Temperature = *p.temperature
	}
	if p.topP != nil {
		body.TopP = *p.topP
	}
	body.Seed = p.seed
	if p.frequencyPenalty != nil {
		body.FrequencyPenalty = *p.frequencyPenalty
	}
	if p.presencePenalty != nil {
		body.PresencePenalty = *p.presencePenalty
	}
	if p.responseFormat != oai.ChatCompletionResponseFormatTypeText {
		body.ResponseFormat = &oai.ChatCompletionResponseFormat{Type: p.responseFormat}
		if p.schemaProvider != nil {
			s, err := p.schemaProvider.GetJSONSchema(ctx)
			if err != nil {
				return nil, err
			}
			body.ResponseFormat.JSONSchema = s
		}
	}
	body.Stop = p.stop
	if p.user != nil {
		u, err := p.user.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", ocpFieldUser, err)
		}
		body.User = u
	}
	if p.systemPrompt != nil {
		s, err := p.systemPrompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", ocpFieldSystemPrompt, err)
		}
		body.Messages = append(body.Messages, oai.ChatCompletionMessage{
			Role:    "system",
			Content: s,
		})
	}
	if p.userPrompt != nil {
		s, err := p.userPrompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", ocpFieldUserPrompt, err)
		}
		body.Messages = append(body.Messages, oai.ChatCompletionMessage{
			Role:    "user",
			Content: s,
		})
	} else {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		body.Messages = append(body.Messages, oai.ChatCompletionMessage{
			Role:    "user",
			Content: string(b),
		})
	}
	resp, err := p.client.CreateChatCompletion(ctx, body)
	if err != nil {
		return nil, err
	}
	if len(resp.Choices) != 1 {
		return nil, fmt.Errorf("invalid number of choices in response: %d", len(resp.Choices))
	}
	msg = msg.Copy()
	msg.SetBytes([]byte(resp.Choices[0].Message.Content))
	return service.MessageBatch{msg}, nil
}
