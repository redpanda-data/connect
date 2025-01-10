// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package ollama

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"unicode/utf8"

	"github.com/Jeffail/gabs/v2"
	"github.com/ollama/ollama/api"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	ocpFieldUserPrompt     = "prompt"
	ocpFieldSystemPrompt   = "system_prompt"
	ocpFieldResponseFormat = "response_format"
	ocpFieldImage          = "image"
	// Prediction options
	ocpFieldMaxTokens          = "max_tokens"
	ocpFieldNumKeep            = "num_keep"
	ocpFieldSeed               = "seed"
	ocpFieldTopK               = "top_k"
	ocpFieldTopP               = "top_p"
	ocpFieldTemp               = "temperature"
	ocpFieldRepeatPenalty      = "repeat_penalty"
	ocpFieldPresencePenalty    = "presence_penalty"
	ocpFieldFrequencyPenalty   = "frequency_penalty"
	ocpFieldStop               = "stop"
	ocpFieldEmitPromptMetadata = "save_prompt_metadata"
	ocpFieldMaxToolCalls       = "max_tool_calls"

	// Tool options
	ocpFieldTool                     = "tools"
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

func init() {
	err := service.RegisterProcessor(
		"ollama_chat",
		ollamaChatProcessorConfig(),
		makeOllamaCompletionProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func ollamaChatProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Generates responses to messages in a chat conversation, using the Ollama API.").
		Description(`This processor sends prompts to your chosen Ollama large language model (LLM) and generates text from the responses, using the Ollama API.

By default, the processor starts and runs a locally installed Ollama server. Alternatively, to use an already running Ollama server, add your server details to the `+"`"+bopFieldServerAddress+"`"+` field. You can https://ollama.com/download[download and install Ollama from the Ollama website^].

For more information, see the https://github.com/ollama/ollama/tree/main/docs[Ollama documentation^].`).
		Version("4.32.0").
		Fields(
			service.NewStringField(bopFieldModel).
				Description("The name of the Ollama LLM to use. For a full list of models, see the https://ollama.com/models[Ollama website].").
				Examples("llama3.1", "gemma2", "qwen2", "phi3"),
			service.NewInterpolatedStringField(ocpFieldUserPrompt).
				Description("The prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
				Optional(),
			service.NewInterpolatedStringField(ocpFieldSystemPrompt).
				Description("The system prompt to submit to the Ollama LLM.").
				Advanced().
				Optional(),
			service.NewBloblangField(ocpFieldImage).
				Description("The image to submit along with the prompt to the model. The result should be a byte array.").
				Version("4.38.0").
				Optional().
				Example(`root = this.image.decode("base64") # decode base64 encoded image`),
			service.NewStringEnumField(ocpFieldResponseFormat, "text", "json").
				Description("The format of the response that the Ollama model generates. If specifying JSON output, then the `"+ocpFieldUserPrompt+"` should specify that the output should be in JSON as well.").
				Default("text"),
			service.NewIntField(ocpFieldMaxTokens).
				Optional().
				Description("The maximum number of tokens to predict and output. Limiting the amount of output means that requests are processed faster and have a fixed limit on the cost."),
			service.NewIntField(ocpFieldTemp).
				Optional().
				Description("The temperature of the model. Increasing the temperature makes the model answer more creatively.").
				LintRule(`root = if this > 2 || this < 0 { [ "field must be between 0.0 and 2.0" ] }`),
			service.NewIntField(ocpFieldNumKeep).
				Optional().
				Advanced().
				Description("Specify the number of tokens from the initial prompt to retain when the model resets its internal context. By default, this value is set to `4`. Use `-1` to retain all tokens from the initial prompt."),
			service.NewIntField(ocpFieldSeed).
				Optional().
				Advanced().
				Description("Sets the random number seed to use for generation. Setting this to a specific number will make the model generate the same text for the same prompt.").
				Example(42),
			service.NewIntField(ocpFieldTopK).
				Optional().
				Advanced().
				Description("Reduces the probability of generating nonsense. A higher value, for example `100`, will give more diverse answers. A lower value, for example `10`, will be more conservative."),
			service.NewFloatField(ocpFieldTopP).
				Optional().
				Advanced().
				Description("Works together with `top-k`. A higher value, for example 0.95, will lead to more diverse text. A lower value, for example 0.5, will generate more focused and conservative text.").
				LintRule(`root = if this > 1 || this < 0 { [ "field must be between 0.0 and 1.0" ] }`),
			service.NewFloatField(ocpFieldRepeatPenalty).
				Optional().
				Advanced().
				Description(`Sets how strongly to penalize repetitions. A higher value, for example 1.5, will penalize repetitions more strongly. A lower value, for example 0.9, will be more lenient.`).
				LintRule(`root = if this > 2 || this < -2 { [ "field must be between -2.0 and 2.0" ] }`),
			service.NewFloatField(ocpFieldPresencePenalty).
				Optional().
				Advanced().
				Description(`Positive values penalize new tokens if they have appeared in the text so far. This increases the model's likelihood to talk about new topics.`).
				LintRule(`root = if this > 2 || this < -2 { [ "field must be between -2.0 and 2.0" ] }`),
			service.NewFloatField(ocpFieldFrequencyPenalty).
				Optional().
				Advanced().
				Description(`Positive values penalize new tokens based on the frequency of their appearance in the text so far. This decreases the model's likelihood to repeat the same line verbatim.`).
				LintRule(`root = if this > 2 || this < -2 { [ "field must be between -2.0 and 2.0" ] }`),
			service.NewStringListField(ocpFieldStop).
				Optional().
				Advanced().
				Description(`Sets the stop sequences to use. When this pattern is encountered the LLM stops generating text and returns the final response.`),
			service.NewBoolField(ocpFieldEmitPromptMetadata).
				Default(false).
				Description(`If enabled the prompt is saved as @prompt metadata on the output message. If system_prompt is used it's also saved as @system_prompt`),
			service.NewIntField(ocpFieldMaxToolCalls).
				Default(3).
				Advanced().
				Description(`The maximum number of sequential tool calls.`).
				LintRule(`root = if this <= 0 { ["field must be greater than zero"] }`),
			service.NewObjectListField(
				ocpFieldTool,
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
				).Description("The parameters the LLM needs to provide to invoke this tool."),
				service.NewProcessorListField(ocpToolFieldPipeline).Description("The pipeline to execute when the LLM uses this tool.").Optional(),
			).Description("The tools to allow the LLM to invoke. This allows building subpipelines that the LLM can choose to invoke to execute agentic-like actions."),
		).Fields(commonFields()...).
		Example(
			"Use Llava to analyze an image",
			"This example fetches image URLs from stdin and has a multimodal LLM describe the image.",
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
    - ollama_chat:
        model: llava
        prompt: "Describe the following image"
        image: "root = content()"
output:
  stdout:
    codec: lines
`).
		Example(
			"Use subpipelines as tool calls",
			"This example allows llama3.2 to execute a subpipeline as a tool call to get more data.",
			`
input:
  generate:
    count: 1
    mapping: |
      root = "What is the weather like in Chicago?"
pipeline:
  processors:
    - ollama_chat:
        model: llama3.2
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
                    # Spoof curl user-ageent to get a plaintext text
                    User-Agent: curl/8.11.1
output:
  stdout: {}
`)
}

func makeOllamaCompletionProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	p := ollamaCompletionProcessor{}
	if conf.Contains(ocpFieldUserPrompt) {
		pf, err := conf.FieldInterpolatedString(ocpFieldUserPrompt)
		if err != nil {
			return nil, err
		}
		p.userPrompt = pf
	}
	if conf.Contains(ocpFieldSystemPrompt) {
		pf, err := conf.FieldInterpolatedString(ocpFieldSystemPrompt)
		if err != nil {
			return nil, err
		}
		p.systemPrompt = pf
	}
	if conf.Contains(ocpFieldImage) {
		i, err := conf.FieldBloblang(ocpFieldImage)
		if err != nil {
			return nil, err
		}
		p.image = i
	}
	format, err := conf.FieldString(ocpFieldResponseFormat)
	if err != nil {
		return nil, err
	}
	if format == "json" {
		p.format = json.RawMessage(`"json"`)
	} else if format == "text" {
		p.format = nil
	} else {
		return nil, fmt.Errorf("invalid %s: %q", ocpFieldResponseFormat, format)
	}
	p.savePrompt, err = conf.FieldBool(ocpFieldEmitPromptMetadata)
	if err != nil {
		return nil, err
	}
	p.maxToolCalls, err = conf.FieldInt(ocpFieldMaxToolCalls)
	if err != nil {
		return nil, err
	}
	if conf.Contains(ocpFieldTool) {
		tools, err := conf.FieldObjectList(ocpFieldTool)
		if err != nil {
			return nil, err
		}
		for _, toolConf := range tools {
			t := api.Tool{Type: "function"}
			t.Function.Name, err = toolConf.FieldString(ocpToolFieldName)
			if err != nil {
				return nil, err
			}
			t.Function.Description, err = toolConf.FieldString(ocpToolFieldDesc)
			if err != nil {
				return nil, err
			}
			t.Function.Parameters.Type = "object"
			paramsConf := toolConf.Namespace(ocpToolFieldParams)
			t.Function.Parameters.Required, err = paramsConf.FieldStringList(ocpToolParamFieldRequired)
			if err != nil {
				return nil, err
			}
			propsConf, err := paramsConf.FieldObjectMap(ocpToolParamFieldProps)
			if err != nil {
				return nil, err
			}
			type toolParam = struct {
				Type        string   `json:"type"`
				Description string   `json:"description"`
				Enum        []string `json:"enum,omitempty"`
			}
			t.Function.Parameters.Properties = map[string]toolParam{}
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
				t.Function.Parameters.Properties[name] = toolParam{
					Type:        paramType,
					Description: desc,
					Enum:        enum,
				}
			}

			pipeline, err := toolConf.FieldProcessorList(ocpToolFieldPipeline)
			if err != nil {
				return nil, err
			}
			p.tools = append(p.tools, tool{t, pipeline})
		}
	}
	b, err := newBaseProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}
	p.baseOllamaProcessor = b
	return &p, nil
}

type tool struct {
	spec     api.Tool
	pipeline []*service.OwnedProcessor
}

type ollamaCompletionProcessor struct {
	*baseOllamaProcessor

	format       json.RawMessage
	userPrompt   *service.InterpolatedString
	systemPrompt *service.InterpolatedString
	image        *bloblang.Executor
	savePrompt   bool
	maxToolCalls int
	tools        []tool
}

func (o *ollamaCompletionProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var sp string
	if o.systemPrompt != nil {
		p, err := o.systemPrompt.TryString(msg)
		if err != nil {
			return nil, err
		}
		sp = p
	}
	up, err := o.computePrompt(msg)
	if err != nil {
		return nil, err
	}
	var image []byte
	if o.image != nil {
		o, err := msg.BloblangQuery(o.image)
		if err != nil {
			return nil, fmt.Errorf("unable to execute bloblang for `%s`: %w", ocpFieldImage, err)
		}
		image, err = o.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("unable to convert `%s` result to a byte array: %w", ocpFieldImage, err)
		}
	}
	g, err := o.generateCompletion(ctx, sp, up, image)
	if err != nil {
		return nil, err
	}
	m := msg.Copy()
	m.SetBytes([]byte(g))
	if o.savePrompt {
		if sp != "" {
			m.MetaSet("system_prompt", sp)
		}
		m.MetaSet("prompt", up)
	}
	return service.MessageBatch{m}, nil
}

func (o *ollamaCompletionProcessor) computePrompt(msg *service.Message) (string, error) {
	if o.userPrompt != nil {
		return o.userPrompt.TryString(msg)
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

func (o *ollamaCompletionProcessor) generateCompletion(ctx context.Context, systemPrompt, userPrompt string, image []byte) (string, error) {
	var req api.ChatRequest
	req.Model = o.model
	req.Options = o.opts
	if o.format != nil {
		req.Format = o.format
	}
	if systemPrompt != "" {
		req.Messages = append(req.Messages, api.Message{
			Role:    "system",
			Content: systemPrompt,
		})
	}
	var images []api.ImageData
	if image != nil {
		images = []api.ImageData{image}
	}
	req.Messages = append(req.Messages, api.Message{
		Role:    "user",
		Content: userPrompt,
		Images:  images,
	})
	shouldStream := false
	req.Stream = &shouldStream
	for _, t := range o.tools {
		req.Tools = append(req.Tools, t.spec)
	}
	// Allow up to N iterations of calling tools
	for range o.maxToolCalls + 1 {
		var resp api.ChatResponse
		o.logger.Tracef("making LLM chat request messages: %s", gabs.Wrap(req.Messages).EncodeJSON())
		err := o.client.Chat(ctx, &req, func(r api.ChatResponse) error {
			resp = r
			return nil
		})
		if err != nil {
			return "", err
		}
		if len(resp.Message.ToolCalls) == 0 {
			return resp.Message.Content, nil
		}
		req.Messages = append(req.Messages, resp.Message)
		for _, toolCall := range resp.Message.ToolCalls {
			o.logger.Debugf("LLM requested tool %s with arguments: %s", toolCall.Function.Name, toolCall.Function.Arguments.String())
			idx := slices.IndexFunc(o.tools, func(t tool) bool { return t.spec.Function.Name == toolCall.Function.Name })
			if idx < 0 {
				return "", fmt.Errorf("unknown tool call requested: %s", toolCall.Function.Name)
			}
			pipeline := o.tools[idx].pipeline
			msg := service.NewMessage(nil)
			msg.SetStructuredMut(map[string]any(toolCall.Function.Arguments))
			output, err := service.ExecuteProcessors(ctx, pipeline, service.MessageBatch{msg})
			if err != nil {
				return "", fmt.Errorf("error calling tool %s: %w", toolCall.Function.Name, err)
			}
			resp, err := combineToSingleMessage(output)
			if err != nil {
				return "", fmt.Errorf("error processing pipeline %s output: %w", toolCall.Function.Name, err)
			}
			o.logger.Debugf("Tool %s response: %s", toolCall.Function.Name, resp)
			req.Messages = append(req.Messages, api.Message{Role: "tool", Content: resp})
		}
	}
	return "", fmt.Errorf("model did not finish after %d function calls", o.maxToolCalls)
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

func (o *ollamaCompletionProcessor) Close(ctx context.Context) error {
	for _, tool := range o.tools {
		for _, processor := range tool.pipeline {
			if err := processor.Close(ctx); err != nil {
				return err
			}
		}
	}
	return o.baseOllamaProcessor.Close(ctx)
}
