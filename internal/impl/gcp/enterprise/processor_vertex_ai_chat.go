// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"unicode/utf8"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"google.golang.org/genai"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	vaicpFieldProject          = "project"
	vaicpFieldCredentialsJSON  = "credentials_json"
	vaicpFieldModel            = "model"
	vaicpFieldLocation         = "location"
	vaicpFieldPrompt           = "prompt"
	vaicpFieldHistory          = "history"
	vaicpFieldSystemPrompt     = "system_prompt"
	vaicpFieldAttachment       = "attachment"
	vaicpFieldTemp             = "temperature"
	vaicpFieldTopP             = "top_p"
	vaicpFieldTopK             = "top_k"
	vaicpFieldMaxTokens        = "max_tokens"
	vaicpFieldStop             = "stop"
	vaicpFieldPresencePenalty  = "presence_penalty"
	vaicpFieldFrequencyPenalty = "frequency_penalty"
	vaicpFieldResponseFormat   = "response_format"
	vaicpFieldMaxToolCalls     = "max_tool_calls"
	// Tool options
	vaicpFieldTool                     = "tools"
	vaicpToolFieldName                 = "name"
	vaicpToolFieldDesc                 = "description"
	vaicpToolFieldParams               = "parameters"
	vaicpToolParamFieldRequired        = "required"
	vaicpToolParamFieldProps           = "properties"
	vaicpToolParamPropFieldType        = "type"
	vaicpToolParamPropFieldDescription = "description"
	vaicpToolParamPropFieldEnum        = "enum"
	vaicpToolFieldPipeline             = "processors"
)

func init() {
	service.MustRegisterProcessor(
		"gcp_vertex_ai_chat",
		newVertexAIProcessorConfig(),
		newVertexAIProcessor,
	)
}

func newVertexAIProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Generates responses to messages in a chat conversation, using the Vertex AI API.").
		Description(`This processor sends prompts to your chosen large language model (LLM) and generates text from the responses, using the Vertex AI API.

For more information, see the https://cloud.google.com/vertex-ai/docs[Vertex AI documentation^].`).
		Version("4.34.0").
		Fields(
			service.NewStringField(vaicpFieldProject).
				Description("GCP project ID to use"),
			service.NewStringField(vaicpFieldCredentialsJSON).
				Description("An optional field to set google Service Account Credentials json.").
				Secret().
				Optional(),
			service.NewStringField(vaicpFieldLocation).
				Description("The location of the model if using a fined tune model. For base models this can be omitted").
				Examples("us-central1"),
			service.NewStringField(vaicpFieldModel).
				Description("The name of the LLM to use. For a full list of models, see the https://console.cloud.google.com/vertex-ai/model-garden[Vertex AI Model Garden].").
				Examples("gemini-1.5-pro-001", "gemini-1.5-flash-001"),
			service.NewInterpolatedStringField(vaicpFieldPrompt).
				Description("The prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
				Optional(),
			service.NewInterpolatedStringField(vaicpFieldSystemPrompt).
				Description("The system prompt to submit to the Vertex AI LLM.").
				Advanced().
				Optional(),
			service.NewBloblangField(vaicpFieldHistory).
				Description(`Historical messages to include in the chat request. The result of the bloblang query should be an array of objects of the form of [{"role": "", "content":""}], where role is "user" or "model".`).
				Optional(),
			service.NewBloblangField(vaicpFieldAttachment).
				Description("Additional data like an image to send with the prompt to the model. The result of the mapping must be a byte array, and the content type is automatically detected.").
				Version("4.38.0").
				Example(`root = this.image.decode("base64") # decode base64 encoded image`).
				Optional(),
			service.NewFloatField(vaicpFieldTemp).
				Description("Controls the randomness of predications.").
				Optional().
				LintRule(`root = if this < 0 || this > 2 { ["field must be between 0.0-2.0"] }`),
			service.NewIntField(vaicpFieldMaxTokens).
				Description("The maximum number of output tokens to generate per message.").
				Optional(),
			service.NewStringEnumField(vaicpFieldResponseFormat, "text", "json").
				Description("The response format of generated type, the model must also be prompted to output the appropriate response type.").
				Default("text"),
			service.NewFloatField(vaicpFieldTopP).
				Advanced().
				Description("If specified, nucleus sampling will be used.").
				Optional().
				LintRule(`root = if this < 0 || this > 1 { ["field must be between 0.0-1.0"] }`),
			service.NewFloatField(vaicpFieldTopK).
				Advanced().
				Description("If specified top-k sampling will be used.").
				Optional().
				LintRule(`root = if this < 1 || this > 40 { ["field must be between 1-40"] }`),
			service.NewStringListField(vaicpFieldStop).
				Advanced().
				Description("Stop sequences to when the model will stop generating further tokens.").
				Optional(),
			service.NewFloatField(vaicpFieldPresencePenalty).
				Advanced().
				Description("Positive values penalize new tokens based on whether they appear in the text so far, increasing the model's likelihood to talk about new topics.").
				Optional().
				LintRule(`root = if this < -2 || this > 2 { ["field must be greater than -2.0 and less than 2.0"] }`),
			service.NewFloatField(vaicpFieldFrequencyPenalty).
				Advanced().
				Description("Positive values penalize new tokens based on their existing frequency in the text so far, decreasing the model's likelihood to repeat the same line verbatim.").
				Optional().
				LintRule(`root = if this < -2 || this > 2 { ["field must be greater than -2.0 and less than 2.0"] }`),
			service.NewIntField(vaicpFieldMaxToolCalls).
				Default(10).
				Advanced().
				Description(`The maximum number of sequential tool calls.`).
				LintRule(`root = if this <= 0 { ["field must be greater than zero"] }`),
			service.NewObjectListField(
				vaicpFieldTool,
				service.NewStringField(vaicpToolFieldName).Description("The name of this tool."),
				service.NewStringField(vaicpToolFieldDesc).Description("A description of this tool, the LLM uses this to decide if the tool should be used."),
				service.NewObjectField(
					vaicpToolFieldParams,
					service.NewStringListField(vaicpToolParamFieldRequired).Default([]string{}).Description("The required parameters for this pipeline."),
					service.NewObjectMapField(
						vaicpToolParamFieldProps,
						service.NewStringField(vaicpToolParamPropFieldType).Description("The type of this parameter."),
						service.NewStringField(vaicpToolParamPropFieldDescription).Description("A description of this parameter."),
						service.NewStringListField(vaicpToolParamPropFieldEnum).Default([]string{}).Description("Specifies that this parameter is an enum and only these specific values should be used."),
					).Description("The properties for the processor's input data"),
				).Description("The parameters the LLM needs to provide to invoke this tool."),
				service.NewProcessorListField(vaicpToolFieldPipeline).Description("The pipeline to execute when the LLM uses this tool.").Optional(),
			).Description("The tools to allow the LLM to invoke. This allows building subpipelines that the LLM can choose to invoke to execute agentic-like actions.").
				Default([]any{}),
		).
		Example(
			"Use processors as tool calls",
			"This example allows gemini to execute a subpipeline as a tool call to get more data.",
			`
input:
  generate:
    count: 1
    mapping: |
      root = "What is the weather like in Chicago?"
pipeline:
  processors:
    - gcp_vertex_ai_chat:
        model: gemini-2.5-flash-preview-05-20
        project: my-project
        location: us-central1
        prompt: "${!content().string()}"
        tools:
          - name: GetWeather
            description: "Retrieve the weather for a specific city"
            parameters:
              required: ["city"]
              properties:
                city:
                  type: string
                  description: the city to lookup the weather for
            processors:
              - http:
                  verb: GET
                  url: 'https://wttr.in/${!this.city}?T'
                  headers:
                    # Spoof curl user-agent to get a plaintext text
                    User-Agent: curl/8.11.1
output:
  stdout: {}
`)
}

func newVertexAIProcessor(conf *service.ParsedConfig, mgr *service.Resources) (p service.Processor, err error) {
	if err = license.CheckRunningEnterprise(mgr); err != nil {
		return
	}

	ctx := context.Background()
	proc := &vertexAIChatProcessor{}
	var project string
	project, err = conf.FieldString(vaicpFieldProject)
	if err != nil {
		return
	}
	location, err := conf.FieldString(vaicpFieldLocation)
	if err != nil {
		return
	}
	var creds *auth.Credentials
	if conf.Contains(vaicpFieldCredentialsJSON) {
		var jsonObject string
		jsonObject, err = conf.FieldString(vaicpFieldCredentialsJSON)
		if err != nil {
			return
		}
		creds, err = credentials.DetectDefault(&credentials.DetectOptions{
			Scopes:           []string{"https://www.googleapis.com/auth/cloud-vertex-ai.firstparty.predict"},
			CredentialsJSON:  []byte(jsonObject),
			UseSelfSignedJWT: true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to load json credentials: %w", err)
		}
	}
	proc.client, err = genai.NewClient(ctx, &genai.ClientConfig{
		Project:     project,
		Location:    location,
		Backend:     genai.BackendVertexAI,
		Credentials: creds,
	})
	if err != nil {
		return
	}
	proc.model, err = conf.FieldString(vaicpFieldModel)
	if err != nil {
		return
	}
	if conf.Contains(vaicpFieldPrompt) {
		proc.userPrompt, err = conf.FieldInterpolatedString(vaicpFieldPrompt)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaicpFieldSystemPrompt) {
		proc.systemPrompt, err = conf.FieldInterpolatedString(vaicpFieldSystemPrompt)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaicpFieldAttachment) {
		proc.attachment, err = conf.FieldBloblang(vaicpFieldAttachment)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaicpFieldHistory) {
		proc.history, err = conf.FieldBloblang(vaicpFieldHistory)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaicpFieldTemp) {
		var temp float64
		temp, err = conf.FieldFloat(vaicpFieldTemp)
		if err != nil {
			return
		}
		proc.temp = genai.Ptr(float32(temp))
	}
	if conf.Contains(vaicpFieldTopP) {
		var topP float64
		topP, err = conf.FieldFloat(vaicpFieldTopP)
		if err != nil {
			return
		}
		proc.topP = genai.Ptr(float32(topP))
	}
	if conf.Contains(vaicpFieldTopK) {
		var topK float64
		topK, err = conf.FieldFloat(vaicpFieldTopK)
		if err != nil {
			return
		}
		proc.topK = genai.Ptr(float32(topK))
	}
	if conf.Contains(vaicpFieldMaxTokens) {
		var maxTokens int
		maxTokens, err = conf.FieldInt(vaicpFieldMaxTokens)
		if err != nil {
			return
		}
		proc.maxTokens = int32(maxTokens)
	}
	if conf.Contains(vaicpFieldStop) {
		proc.stopSequences, err = conf.FieldStringList(vaicpFieldStop)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaicpFieldPresencePenalty) {
		var pp float64
		pp, err = conf.FieldFloat(vaicpFieldPresencePenalty)
		if err != nil {
			return
		}
		proc.presencePenalty = genai.Ptr(float32(pp))
	}
	if conf.Contains(vaicpFieldFrequencyPenalty) {
		var fp float64
		fp, err = conf.FieldFloat(vaicpFieldFrequencyPenalty)
		if err != nil {
			return
		}
		proc.frequencyPenalty = genai.Ptr(float32(fp))
	}
	var format string
	format, err = conf.FieldString(vaicpFieldResponseFormat)
	if err != nil {
		return nil, err
	}
	switch format {
	case "json":
		proc.responseMIMEType = "application/json"
	case "text":
		proc.responseMIMEType = "text/plain"
	default:
		return nil, fmt.Errorf("invalid value %q for `%s`", format, vaicpFieldResponseFormat)
	}
	proc.maxToolCalls, err = conf.FieldInt(vaicpFieldMaxToolCalls)
	if err != nil {
		return nil, err
	}
	toolsConf, err := conf.FieldObjectList(vaicpFieldTool)
	if err != nil {
		return nil, err
	}
	for _, toolConf := range toolsConf {
		name, err := toolConf.FieldString(vaicpToolFieldName)
		if err != nil {
			return nil, err
		}
		desc, err := toolConf.FieldString(vaicpToolFieldDesc)
		if err != nil {
			return nil, err
		}
		paramsConf := toolConf.Namespace(vaicpToolFieldParams)
		required, err := paramsConf.FieldStringList(vaicpToolParamFieldRequired)
		if err != nil {
			return nil, err
		}
		propsConf, err := paramsConf.FieldObjectMap(vaicpToolParamFieldProps)
		if err != nil {
			return nil, err
		}
		props := map[string]*genai.Schema{}
		for propName, propConf := range propsConf {
			typeStr, err := propConf.FieldString(vaicpToolParamPropFieldType)
			if err != nil {
				return nil, err
			}
			typeStr = strings.ToUpper(typeStr)
			validTypes := []genai.Type{
				genai.TypeArray,
				genai.TypeBoolean,
				genai.TypeInteger,
				genai.TypeNULL,
				genai.TypeNumber,
				genai.TypeObject,
				genai.TypeString,
			}
			if !slices.Contains(validTypes, genai.Type(typeStr)) {
				return nil, fmt.Errorf("invalid type %q for property %q in tool %q, valid types: %v", typeStr, propName, name, validTypes)
			}
			fieldDesc, err := propConf.FieldString(vaicpToolParamPropFieldDescription)
			if err != nil {
				return nil, err
			}
			enum, err := propConf.FieldStringList(vaicpToolParamPropFieldEnum)
			if err != nil {
				return nil, err
			}
			props[propName] = &genai.Schema{
				Type:        genai.Type(typeStr),
				Description: fieldDesc,
				Enum:        enum,
			}
		}
		pipeline, err := toolConf.FieldProcessorList(vaicpToolFieldPipeline)
		if err != nil {
			return nil, err
		}
		proc.tools = append(proc.tools, tool{
			def: &genai.Tool{
				FunctionDeclarations: []*genai.FunctionDeclaration{
					{
						Name:        name,
						Description: desc,
						Parameters: &genai.Schema{
							Type:       genai.TypeObject,
							Required:   required,
							Properties: props,
						},
					},
				},
			},
			pipeline: pipeline,
		})
	}
	p = proc
	return
}

type tool struct {
	def      *genai.Tool
	pipeline []*service.OwnedProcessor
}

type vertexAIChatProcessor struct {
	client *genai.Client
	model  string

	userPrompt       *service.InterpolatedString
	systemPrompt     *service.InterpolatedString
	attachment       *bloblang.Executor
	history          *bloblang.Executor
	temp             *float32
	topP             *float32
	topK             *float32
	maxTokens        int32
	stopSequences    []string
	presencePenalty  *float32
	frequencyPenalty *float32
	responseMIMEType string
	maxToolCalls     int
	tools            []tool
}

func (p *vertexAIChatProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	cfg := &genai.GenerateContentConfig{}
	for _, tool := range p.tools {
		cfg.Tools = append(cfg.Tools, tool.def)
	}
	cfg.Temperature = p.temp
	cfg.TopP = p.topP
	cfg.TopK = p.topK
	cfg.MaxOutputTokens = p.maxTokens
	cfg.StopSequences = p.stopSequences
	cfg.PresencePenalty = p.presencePenalty
	cfg.FrequencyPenalty = p.frequencyPenalty
	cfg.ResponseMIMEType = p.responseMIMEType
	if p.systemPrompt != nil {
		p, err := p.systemPrompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("unable to evaluate `%s`: %w", vaicpFieldSystemPrompt, err)
		}
		cfg.SystemInstruction = &genai.Content{
			Role:  genai.RoleUser,
			Parts: []*genai.Part{{Text: p}},
		}
	}
	var history []*genai.Content
	if p.history != nil {
		h, err := msg.BloblangQuery(p.history)
		if err != nil {
			return nil, fmt.Errorf("unable to evaluate `%s`: %w", vaicpFieldHistory, err)
		}
		b, err := h.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("unable to extract `%s` output: %w", vaicpFieldHistory, err)
		}
		var bloblOutput []struct {
			Role    genai.Role `json:"role"`
			Content string     `json:"content"`
		}
		if err := json.Unmarshal(b, &bloblOutput); err != nil {
			return nil, fmt.Errorf("unable to unmarshal `%s` output: %w", vaicpFieldHistory, err)
		}
		for _, h := range bloblOutput {
			history = append(history, genai.NewContentFromText(h.Content, h.Role))
		}
	}
	chat, err := p.client.Chats.Create(ctx, p.model, cfg, history)
	if err != nil {
		return nil, fmt.Errorf("failed to create chat: %w", err)
	}
	prompt, err := p.computePrompt(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to compute prompt: %w", err)
	}
	reqParts := []genai.Part{{Text: prompt}}
	if p.attachment != nil {
		v, err := msg.BloblangQuery(p.attachment)
		if err != nil {
			return nil, fmt.Errorf("unable to evaluate `%s`: %w", vaicpFieldAttachment, err)
		}
		i, err := v.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("unable to convert `%s` to bytes: %w", vaicpFieldAttachment, err)
		}
		contentType := http.DetectContentType(i)
		if contentType == "application/octet-stream" {
			return nil, fmt.Errorf("unable to detect content-type of `%s`", vaicpFieldAttachment)
		}
		reqParts = append(reqParts, genai.Part{InlineData: &genai.Blob{MIMEType: contentType, Data: i}})
	}
	for range p.maxToolCalls {
		resp, err := chat.SendMessage(ctx, reqParts...)
		if err != nil {
			return nil, fmt.Errorf("failed to generate response: %w", err)
		}
		if len(resp.Candidates) != 1 {
			if resp.PromptFeedback != nil && resp.PromptFeedback.BlockReasonMessage != "" {
				return nil, fmt.Errorf("response blocked due to: %s", resp.PromptFeedback.BlockReasonMessage)
			}
			return nil, errors.New("no candidate responses returned")
		}
		respParts := resp.Candidates[0].Content.Parts
		reqParts = nil
		for _, part := range respParts {
			if part.FunctionCall == nil {
				continue
			}
			var funcResp genai.Part
			idx := slices.IndexFunc(p.tools, func(t tool) bool {
				return t.def.FunctionDeclarations[0].Name == part.FunctionCall.Name
			})
			if idx < 0 {
				return nil, fmt.Errorf("no function for tool call %q", part.FunctionCall.Name)
			}
			tool := p.tools[idx]
			funcParams := msg.Copy()
			funcParams.SetStructured(part.FunctionCall.Args)
			batches, err := service.ExecuteProcessors(ctx, tool.pipeline, service.MessageBatch{funcParams})
			funcResp.FunctionResponse = &genai.FunctionResponse{
				ID:       part.FunctionCall.ID,
				Name:     part.FunctionCall.Name,
				Response: map[string]any{},
			}
			if err != nil {
				funcResp.FunctionResponse.Response["error"] = err.Error()
				reqParts = append(reqParts, funcResp)
				continue
			}
			var outputs []any
			var errs []error
			for _, m := range slices.Concat(batches...) {
				if err := m.GetError(); err != nil {
					errs = append(errs, err)
				} else if m.HasStructured() {
					v, err := m.AsStructured()
					if err != nil {
						errs = append(errs, err)
					} else {
						outputs = append(outputs, v)
					}
				} else {
					v, err := m.AsBytes()
					if err != nil {
						errs = append(errs, err)
					} else if utf8.Valid(v) {
						outputs = append(outputs, string(v))
					} else {
						outputs = append(outputs, v)
					}
				}
			}
			if len(errs) > 0 {
				funcResp.FunctionResponse.Response["error"] = errors.Join(errs...).Error()
			}
			if len(outputs) > 1 {
				funcResp.FunctionResponse.Response["output"] = outputs
			} else if len(outputs) == 1 {
				funcResp.FunctionResponse.Response["output"] = outputs[0]
			}
			reqParts = append(reqParts, funcResp)
		}
		if len(reqParts) > 0 {
			continue
		}
		if len(respParts) != 1 {
			if resp.PromptFeedback != nil && resp.PromptFeedback.BlockReasonMessage != "" {
				return nil, fmt.Errorf("response blocked due to: %s", resp.PromptFeedback.BlockReasonMessage)
			}
			return nil, errors.New("no candidate response parts returned")
		}
		out := msg.Copy()
		part := respParts[0]
		switch {
		case part.InlineData != nil:
			out.SetBytes(part.InlineData.Data)
			out.MetaSetMut("content_type", part.InlineData.MIMEType)
		case part.FileData != nil:
			out.SetStructured(part.FileData.FileURI)
			out.MetaSetMut("content_type", part.FileData.MIMEType)
		case part.Text != "":
			out.SetBytes([]byte(part.Text))
			out.MetaSetMut("content_type", "text/plain")
		default:
			return nil, fmt.Errorf("unknown response content: %T", respParts[0])
		}
		return service.MessageBatch{out}, nil
	}
	return nil, fmt.Errorf("exceeded maximum number of tool calls (%d)", p.maxToolCalls)
}

func (p *vertexAIChatProcessor) computePrompt(msg *service.Message) (string, error) {
	if p.userPrompt != nil {
		return p.userPrompt.TryString(msg)
	}
	b, err := msg.AsBytes()
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", errors.New("message payload contained invalid UTF8")
	}
	return string(b), nil
}

func (*vertexAIChatProcessor) Close(context.Context) error {
	return nil
}
