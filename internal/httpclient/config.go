package httpclient

import (
	"errors"

	"github.com/Jeffail/gabs/v2"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/docs/interop"
	"github.com/benthosdev/benthos/v4/internal/metadata"
	"github.com/benthosdev/benthos/v4/internal/tls"
	"github.com/benthosdev/benthos/v4/public/service"
)

// ConfigField returns a public API config field spec for an HTTP component,
// with optional extra fields added to the end.
func ConfigField(defaultVerb string, forOutput bool, extraChildren ...*service.ConfigField) *service.ConfigField {
	extraOldStyle := make([]docs.FieldSpec, len(extraChildren))
	for i, v := range extraChildren {
		extraOldStyle[i] = interop.Unwrap(v)
	}

	oldField := OldFieldSpec(defaultVerb, forOutput, extraOldStyle...)
	return service.NewInternalField(oldField)
}

// OldFieldSpec returns a field spec for an http client component.
func OldFieldSpec(defaultVerb string, forOutput bool, extraChildren ...docs.FieldSpec) docs.FieldSpec {
	httpSpecs := docs.FieldSpecs{
		docs.FieldURL("url", "The URL to connect to.").IsInterpolated(),
		docs.FieldString("verb", "A verb to connect with", "POST", "GET", "DELETE").HasDefault(defaultVerb),
		docs.FieldString("headers", "A map of headers to add to the request.", map[string]any{
			"Content-Type": "application/octet-stream",
			"traceparent":  `${! tracing_span().traceparent }`,
		}).IsInterpolated().Map().HasDefault(map[string]any{}),
		docs.FieldObject("metadata", "Specify optional matching rules to determine which metadata keys should be added to the HTTP request as headers.").Advanced().
			WithChildren(metadata.IncludeFilterDocs()...),
		docs.FieldString("dump_request_log_level", "EXPERIMENTAL: Optionally set a level at which the request and response payload of each request made will be logged.").Advanced().HasDefault("").HasOptions("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL").AtVersion("4.12.0"),
	}

	extractHeadersDesc := "Specify which response headers should be added to resulting messages as metadata. Header keys are lowercased before matching, so ensure that your patterns target lowercased versions of the header keys that you expect."
	if forOutput {
		extractHeadersDesc = "Specify which response headers should be added to resulting synchronous response messages as metadata. Header keys are lowercased before matching, so ensure that your patterns target lowercased versions of the header keys that you expect. This field is not applicable unless `propagate_response` is set to `true`."
	}

	for _, s := range AuthFieldSpecsExpanded() {
		httpSpecs = append(httpSpecs, interop.Unwrap(s))
	}

	httpSpecs = append(httpSpecs, tls.FieldSpec(),
		docs.FieldObject("extract_headers", extractHeadersDesc).WithChildren(metadata.IncludeFilterDocs()...).Advanced(),
		docs.FieldString("rate_limit", "An optional [rate limit](/docs/components/rate_limits/about) to throttle requests by.").Optional(),
		docs.FieldString("timeout", "A static timeout to apply to requests.").HasDefault("5s"),
		docs.FieldString("retry_period", "The base period to wait between failed requests.").Advanced().HasDefault("1s"),
		docs.FieldString("max_retry_backoff", "The maximum period to wait between failed requests.").Advanced().HasDefault("300s"),
		docs.FieldInt("retries", "The maximum number of retry attempts to make.").Advanced().HasDefault(3),
		docs.FieldInt("backoff_on", "A list of status codes whereby the request should be considered to have failed and retries should be attempted, but the period between them should be increased gradually.").Array().Advanced().HasDefault([]any{429}),
		docs.FieldInt("drop_on", "A list of status codes whereby the request should be considered to have failed but retries should not be attempted. This is useful for preventing wasted retries for requests that will never succeed. Note that with these status codes the _request_ is dropped, but _message_ that caused the request will not be dropped.").Array().Advanced().HasDefault([]any{}),
		docs.FieldInt("successful_on", "A list of status codes whereby the attempt should be considered successful, this is useful for dropping requests that return non-2XX codes indicating that the message has been dealt with, such as a 303 See Other or a 409 Conflict. All 2XX codes are considered successful unless they are present within `backoff_on` or `drop_on`, regardless of this field.").Array().Advanced().HasDefault([]any{}),
		docs.FieldURL("proxy_url", "An optional HTTP proxy URL.").Advanced().HasDefault(""),
	)
	httpSpecs = append(httpSpecs, extraChildren...)

	return docs.FieldComponent().WithChildren(httpSpecs...).
		LinterFunc((func(ctx docs.LintContext, line, col int, value any) []docs.Lint {
			if _, ok := value.(map[string]any); !ok {
				return nil
			}
			gObj := gabs.Wrap(value)
			copyResponseHeaders, copyResponseHeadersSet := gObj.S("copy_response_headers").Data().(bool)
			metaPrefixCount, _ := gObj.ArrayCountP("extract_headers.include_prefixes")
			metaPatternCount, _ := gObj.ArrayCountP("extract_headers.include_patterns")
			if copyResponseHeadersSet && copyResponseHeaders && (metaPrefixCount > 0 || metaPatternCount > 0) {
				return []docs.Lint{
					docs.NewLintError(line, docs.LintCustom, "Cannot use extract_headers when copy_response_headers is true."),
				}
			}
			return nil
		}))
}

//------------------------------------------------------------------------------

// ConfigFromAny is a temporary work around for components that get a new style
// parsed config and need to get an old style struct config.
func ConfigFromAny(v any) (conf OldConfig, err error) {
	if pNode, ok := v.(*yaml.Node); ok {
		err = pNode.Decode(&conf)
		return
	}

	var node yaml.Node
	if err = node.Encode(v); err != nil {
		err = errors.New("failed to parse http config")
		return
	}

	conf = NewOldConfig()
	if err = node.Decode(&conf); err != nil {
		err = errors.New("failed to marshal http config")
		return
	}
	return
}

// OldConfig is a configuration struct for an HTTP client.
type OldConfig struct {
	URL                 string                       `json:"url" yaml:"url"`
	Verb                string                       `json:"verb" yaml:"verb"`
	Headers             map[string]string            `json:"headers" yaml:"headers"`
	Metadata            metadata.IncludeFilterConfig `json:"metadata" yaml:"metadata"`
	ExtractMetadata     metadata.IncludeFilterConfig `json:"extract_headers" yaml:"extract_headers"`
	RateLimit           string                       `json:"rate_limit" yaml:"rate_limit"`
	Timeout             string                       `json:"timeout" yaml:"timeout"`
	Retry               string                       `json:"retry_period" yaml:"retry_period"`
	MaxBackoff          string                       `json:"max_retry_backoff" yaml:"max_retry_backoff"`
	NumRetries          int                          `json:"retries" yaml:"retries"`
	BackoffOn           []int                        `json:"backoff_on" yaml:"backoff_on"`
	DropOn              []int                        `json:"drop_on" yaml:"drop_on"`
	SuccessfulOn        []int                        `json:"successful_on" yaml:"successful_on"`
	DumpRequestLogLevel string                       `json:"dump_request_log_level" yaml:"dump_request_log_level"`
	TLS                 tls.Config                   `json:"tls" yaml:"tls"`
	ProxyURL            string                       `json:"proxy_url" yaml:"proxy_url"`
	AuthConfig          `json:",inline" yaml:",inline"`
	OAuth2              OAuth2Config `json:"oauth2" yaml:"oauth2"`
}

// NewOldConfig creates a new Config with default values.
func NewOldConfig() OldConfig {
	return OldConfig{
		URL:             "",
		Verb:            "POST",
		Headers:         map[string]string{},
		Metadata:        metadata.NewIncludeFilterConfig(),
		ExtractMetadata: metadata.NewIncludeFilterConfig(),
		RateLimit:       "",
		Timeout:         "5s",
		Retry:           "1s",
		MaxBackoff:      "300s",
		NumRetries:      3,
		BackoffOn:       []int{429},
		DropOn:          []int{},
		SuccessfulOn:    []int{},
		TLS:             tls.NewConfig(),
		AuthConfig:      NewAuthConfig(),
		OAuth2:          NewOAuth2Config(),
	}
}
