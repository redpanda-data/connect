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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	oai "github.com/sashabaranov/go-openai"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	ocpFieldUserPrompt       = "prompt"
	ocpFieldSystemPrompt     = "system_prompt"
	ocpFieldHistory          = "history"
	ocpFieldImage            = "image"
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
	// Tool options
	ocpFieldTools                    = "tools"
	ocpToolFieldName                 = "name"
	ocpToolFieldDesc                 = "description"
	ocpToolFieldParams               = "parameters"
	ocpToolParamFieldRequired        = "required"
	ocpToolParamFieldProps           = "properties"
	ocpToolParamPropFieldType        = "type"
	ocpToolParamPropFieldDescription = "description"
	ocpToolParamPropFieldEnum        = "enum"
	ocpToolFieldPipeline             = "processors"
)

type pipelineTool struct {
	tool       oai.Tool
	processors []*service.OwnedProcessor
}

func init() {
	service.MustRegisterProcessor(
		"openai_chat_completion",
		chatProcessorConfig(),
		makeChatProcessor,
	)
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
			service.NewBloblangField(ocpFieldHistory).
				Description(`The history of the prior conversation. A bloblang query that should result in an array of objects of the form: [{"role": "user", "content": "<text>"}, {"role":"assistant", "content":"<text>"}]`).
				Optional(),
			service.NewBloblangField(ocpFieldImage).
				Description("An image to send along with the prompt. The mapping result must be a byte array.").
				Version("4.38.0").
				Example(`root = this.image.decode("base64") # decode base64 encoded image`).
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
			service.NewObjectListField(
				ocpFieldTools,
				service.NewStringField(ocpToolFieldName).Description("The name of this tool."),
				service.NewStringField(ocpToolFieldDesc).Description("A description of this tool, the LLM uses this to decide if the tool should be used."),
				service.NewObjectField(
					ocpToolFieldParams,
					service.NewStringListField(ocpToolParamFieldRequired).Default([]string{}).Description("The required parameters for this pipeline."),
					service.NewObjectMapField(
						ocpToolParamFieldProps,
						service.NewStringField(ocpToolParamPropFieldType).Description("The type of this parameter."),
						service.NewStringField(ocpToolParamPropFieldDescription).Description("A description of this parameter."),
						service.NewStringListField(ocpToolParamPropFieldEnum).Default([]string{}).Description("Specifies that this parameter is an enum and only these specific values should be used."),
					).Description("The properties for the processor's input data"),
				).Description("The parameters the LLM needs to provide to invoke this tool.").
					Default([]any{}),
				service.NewProcessorListField(ocpToolFieldPipeline).Description("The pipeline to execute when the LLM uses this tool.").Optional(),
			).Description("The tools to allow the LLM to invoke. This allows building subpipelines that the LLM can choose to invoke to execute agentic-like actions."),
		).LintRule(`
      root = match {
        this.exists("`+ocpFieldJSONSchema+`") && this.exists("`+ocpFieldSchemaRegistry+`") => ["cannot set both `+"`"+ocpFieldJSONSchema+"`"+` and `+"`"+ocpFieldSchemaRegistry+"`"+`"]
        this.response_format == "json_schema" && !this.exists("`+ocpFieldJSONSchema+`") && !this.exists("`+ocpFieldSchemaRegistry+`") => ["schema must be specified using either `+"`"+ocpFieldJSONSchema+"`"+` or `+"`"+ocpFieldSchemaRegistry+"`"+`"]
      }
    `).
		Example(
			"Use GPT-4o analyze an image",
			"This example fetches image URLs from stdin and has GPT-4o describe the image.",
			`
input:
  stdin:
    scanner:
      lines: {}
pipeline:
  processors:
    - http:
        verb: GET
        url: "${!content().string()}"
    - openai_chat_completion:
        model: gpt-4o
        api_key: TODO
        prompt: "Describe the following image"
        image: "root = content()"
output:
  stdout:
    codec: lines
`).
		Example(
			"Provide historical chat history",
			"This pipeline provides a historical chat history to GPT-4o using a cache.",
			`
input:
  stdin:
    scanner:
      lines: {}
pipeline:
  processors:
    - mapping: |
        root.prompt = content().string()
    - branch:
        processors:
          - cache:
              resource: mem
              operator: get
              key: history
          - catch:
            - mapping: 'root = []'
        result_map: 'root.history = this'
    - branch:
        processors:
        - openai_chat_completion:
            model: gpt-4o
            api_key: TODO
            prompt: "${!this.prompt}"
            history: 'root = this.history'
        result_map: 'root.response = content().string()'
    - mutation: |
        root.history = this.history.concat([
          {"role": "user", "content": this.prompt},
          {"role": "assistant", "content": this.response},
        ])
    - cache:
        resource: mem
        operator: set
        key: history
        value: '${!this.history}'
    - mapping: |
        root = this.response
output:
  stdout:
    codec: lines

cache_resources:
  - label: mem 
    memory: {}
`).
		Example(
			"Use GPT-4o to call a tool",
			"This example asks GPT-4o to respond with the weather by invoking an HTTP processor to get the forecast.",
			`
input:
  generate:
    count: 1
    mapping: |
      root = "What is the weather like in Chicago?"
pipeline:
  processors:
    - openai_chat_completion:
        model: gpt-4o
        api_key: "${OPENAI_API_KEY}"
        prompt: "${!content().string()}"
        tools:
          - name: GetWeather
            description: "Retrieve the weather for a specific city"
            parameters:
              required: ["city"]
              properties:
                city:
                  type: string
                  description: the city to look up the weather for
            processors:
              - http:
                  verb: GET
                  url: 'https://wttr.in/${!this.city}?T'
                  headers:
                    User-Agent: curl/8.11.1 # Returns a text string from the weather website
output:
  stdout: {}
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
	var h *bloblang.Executor
	if conf.Contains(ocpFieldHistory) {
		h, err = conf.FieldBloblang(ocpFieldHistory)
		if err != nil {
			return nil, err
		}
	}
	var i *bloblang.Executor
	if conf.Contains(ocpFieldImage) {
		i, err = conf.FieldBloblang(ocpFieldImage)
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
	var tools []pipelineTool
	if conf.Contains(ocpFieldTools) {
		toolSpecs, err := conf.FieldObjectList(ocpFieldTools)
		if err != nil {
			return nil, err
		}
		for _, toolConf := range toolSpecs {
			t := oai.Tool{Type: oai.ToolTypeFunction, Function: &oai.FunctionDefinition{}}
			t.Function.Name, err = toolConf.FieldString(ocpToolFieldName)
			if err != nil {
				return nil, err
			}
			t.Function.Description, err = toolConf.FieldString(ocpToolFieldDesc)
			if err != nil {
				return nil, err
			}
			type toolParam = struct {
				Type        string   `json:"type"`
				Description string   `json:"description"`
				Enum        []string `json:"enum,omitempty"`
			}
			type toolParams = struct {
				Type       string               `json:"type"`
				Required   []string             `json:"required"`
				Properties map[string]toolParam `json:"properties"`
			}
			parameters := toolParams{
				Type:       "object",
				Properties: map[string]toolParam{},
			}
			paramsConf := toolConf.Namespace(ocpToolFieldParams)
			parameters.Required, err = paramsConf.FieldStringList(ocpToolParamFieldRequired)
			if err != nil {
				return nil, err
			}
			propsConf, err := paramsConf.FieldObjectMap(ocpToolParamFieldProps)
			if err != nil {
				return nil, err
			}
			for name, paramConf := range propsConf {
				paramType, err := paramConf.FieldString(ocpToolParamPropFieldType)
				if err != nil {
					return nil, err
				}
				desc, err := paramConf.FieldString(ocpToolParamPropFieldDescription)
				if err != nil {
					return nil, err
				}
				enum, err := paramConf.FieldStringList(ocpToolParamPropFieldEnum)
				if err != nil {
					return nil, err
				}
				parameters.Properties[name] = toolParam{
					Type:        paramType,
					Description: desc,
					Enum:        enum,
				}
			}
			t.Function.Parameters = parameters
			pipeline, err := toolConf.FieldProcessorList(ocpToolFieldPipeline)
			if err != nil {
				return nil, err
			}
			tools = append(tools, pipelineTool{t, pipeline})
		}
	}
	return &chatProcessor{
		b,
		up,
		sp,
		h,
		i,
		maxTokens,
		temp,
		user,
		topP,
		frequencyPenalty,
		presencePenalty,
		seed,
		stop,
		responseFormat,
		schemaProvider,
		tools,
	}, nil
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
	history          *bloblang.Executor
	image            *bloblang.Executor
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
	tools            []pipelineTool
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
	if p.history != nil {
		msg, err := msg.BloblangQuery(p.history)
		if err != nil {
			return nil, fmt.Errorf("%s execution error: %w", ocpFieldHistory, err)
		}
		b, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("%s extraction error: %w", ocpFieldHistory, err)
		}
		var msgs []oai.ChatCompletionMessage
		if err := json.Unmarshal(b, &msgs); err != nil {
			return nil, fmt.Errorf("unable to unmarshal %s: %w", ocpFieldHistory, err)
		}
		body.Messages = append(body.Messages, msgs...)
	}
	chatMsg := oai.ChatCompletionMessage{
		Role: "user",
	}
	if p.userPrompt != nil {
		s, err := p.userPrompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", ocpFieldUserPrompt, err)
		}
		chatMsg.Content = s
	} else {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		chatMsg.Content = string(b)
	}
	body.Messages = append(body.Messages, chatMsg)
	if p.image != nil {
		i, err := msg.BloblangQuery(p.image)
		if err != nil {
			return nil, fmt.Errorf("%s execution error: %w", ocpFieldImage, err)
		}
		b, err := i.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("%s conversion error: %w", ocpFieldImage, err)
		}
		mimeType := http.DetectContentType(b)
		if !strings.HasPrefix(mimeType, "image/") {
			return nil, fmt.Errorf("invalid %s data, detected mime type: %s", ocpFieldImage, mimeType)
		}
		body.Messages = append(body.Messages, oai.ChatCompletionMessage{
			Role: "user",
			MultiContent: []oai.ChatMessagePart{{
				Type: oai.ChatMessagePartTypeImageURL,
				ImageURL: &oai.ChatMessageImageURL{
					URL: "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(b),
				},
			}},
		})
	}
	if len(p.tools) > 0 {
		// TODO: Support parallel tool calls
		body.ParallelToolCalls = false
		for _, t := range p.tools {
			body.Tools = append(body.Tools, t.tool)
		}
	}
	const maxToolCalls = 10
	for range maxToolCalls {
		resp, err := p.client.CreateChatCompletion(ctx, body)
		if err != nil {
			return nil, err
		}
		if len(resp.Choices) != 1 {
			return nil, fmt.Errorf("invalid number of choices in response: %d", len(resp.Choices))
		}
		respMessage := resp.Choices[0].Message
		if len(respMessage.ToolCalls) == 0 {
			msg = msg.Copy()
			msg.SetBytes([]byte(respMessage.Content))
			return service.MessageBatch{msg}, nil
		} else if len(respMessage.ToolCalls) > 1 {
			return nil, fmt.Errorf("parallel tool calling disabled, but got %d parallel tool calls", len(respMessage.ToolCalls))
		}
		invoked := respMessage.ToolCalls[0]
		idx := slices.IndexFunc(p.tools, func(t pipelineTool) bool {
			return t.tool.Function.Name == invoked.Function.Name
		})
		if idx == -1 {
			return nil, fmt.Errorf("unknown tool call from model %s", invoked.Function.Name)
		}
		toolMsg := service.NewMessage([]byte(invoked.Function.Arguments))
		toolBatches, err := service.ExecuteProcessors(ctx, p.tools[idx].processors, service.MessageBatch{toolMsg})
		if err != nil {
			return nil, fmt.Errorf("error calling tool %s: %w", invoked.Function.Name, err)
		}
		output, err := combineToSingleMessage(toolBatches)
		if err != nil {
			return nil, fmt.Errorf("error processing pipeline %s output: %w", invoked.Function.Name, err)
		}
		body.Messages = append(body.Messages, respMessage, oai.ChatCompletionMessage{
			Role:       oai.ChatMessageRoleTool,
			Content:    output,
			Name:       invoked.Function.Name,
			ToolCallID: invoked.ID,
		})
	}
	return nil, fmt.Errorf("model did not finish after %d function calls", maxToolCalls)
}

func combineToSingleMessage(batches []service.MessageBatch) (string, error) {
	msgs := []any{}
	for _, batch := range batches {
		for _, msg := range batch {
			if err := msg.GetError(); err != nil {
				return "", fmt.Errorf("pipeline resulted in message with error: %w", err)
			}
			if msg.HasStructured() {
				v, err := msg.AsStructured()
				if err != nil {
					return "", fmt.Errorf("unable to extract JSON result: %w", err)
				}
				msgs = append(msgs, v)
			} else {
				b, err := msg.AsBytes()
				if err != nil {
					return "", fmt.Errorf("unable to extract raw bytes result: %w", err)
				}
				msgs = append(msgs, string(b))
			}
		}
	}
	if len(msgs) == 1 {
		return bloblang.ValueToString(msgs[0]), nil
	}
	return bloblang.ValueToString(msgs), nil
}
