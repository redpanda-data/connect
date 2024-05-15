package httpclient

import (
	"context"
	"crypto/tls"
	"io/fs"
	"net/http"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	hcFieldURL                 = "url"
	hcFieldVerb                = "verb"
	hcFieldHeaders             = "headers"
	hcFieldMetadata            = "metadata"
	hcFieldExtractHeaders      = "extract_headers"
	hcFieldRateLimit           = "rate_limit"
	hcFieldTimeout             = "timeout"
	hcFieldRetryPeriod         = "retry_period"
	hcFieldMaxRetryBackoff     = "max_retry_backoff"
	hcFieldRetries             = "retries"
	hcFieldBackoffOn           = "backoff_on"
	hcFieldDropOn              = "drop_on"
	hcFieldSuccessfulOn        = "successful_on"
	hcFieldDumpRequestLogLevel = "dump_request_log_level"
	hcFieldTLS                 = "tls"
	hcFieldProxyURL            = "proxy_url"
)

// ConfigField returns a public API config field spec for an HTTP component,
// with optional extra fields added to the end.
func ConfigField(defaultVerb string, forOutput bool, extraChildren ...*service.ConfigField) *service.ConfigField {
	innerFields := []*service.ConfigField{
		service.NewInterpolatedStringField(hcFieldURL).
			Description("The URL to connect to."),
		service.NewStringField(hcFieldVerb).
			Description("A verb to connect with").
			Examples("POST", "GET", "DELETE").
			Default(defaultVerb),
		service.NewInterpolatedStringMapField(hcFieldHeaders).
			Description("A map of headers to add to the request.").
			Example(map[string]any{
				"Content-Type": "application/octet-stream",
				"traceparent":  `${! tracing_span().traceparent }`,
			}).
			Default(map[string]any{}),
		service.NewMetadataFilterField(hcFieldMetadata).
			Description("Specify optional matching rules to determine which metadata keys should be added to the HTTP request as headers.").
			Advanced().
			Optional(),
		service.NewStringEnumField(hcFieldDumpRequestLogLevel, "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "").
			Description("EXPERIMENTAL: Optionally set a level at which the request and response payload of each request made will be logged.").
			Advanced().
			Default("").
			Version("4.12.0"),
	}
	innerFields = append(innerFields, AuthFieldSpecsExpanded()...)

	extractHeadersDesc := "Specify which response headers should be added to resulting messages as metadata. Header keys are lowercased before matching, so ensure that your patterns target lowercased versions of the header keys that you expect."
	if forOutput {
		extractHeadersDesc = "Specify which response headers should be added to resulting synchronous response messages as metadata. Header keys are lowercased before matching, so ensure that your patterns target lowercased versions of the header keys that you expect. This field is not applicable unless `propagate_response` is set to `true`."
	}
	innerFields = append(innerFields,
		service.NewTLSToggledField(hcFieldTLS),
		service.NewMetadataFilterField(hcFieldExtractHeaders).
			Description(extractHeadersDesc).
			Advanced(),
		service.NewStringField(hcFieldRateLimit).
			Description("An optional [rate limit](/docs/components/rate_limits/about) to throttle requests by.").
			Optional(),
		service.NewDurationField(hcFieldTimeout).
			Description("A static timeout to apply to requests.").
			Default("5s"),
		service.NewDurationField(hcFieldRetryPeriod).
			Description("The base period to wait between failed requests.").
			Advanced().
			Default("1s"),
		service.NewDurationField(hcFieldMaxRetryBackoff).
			Description("The maximum period to wait between failed requests.").
			Advanced().
			Default("300s"),
		service.NewIntField(hcFieldRetries).
			Description("The maximum number of retry attempts to make.").
			Advanced().
			Default(3),
		service.NewIntListField(hcFieldBackoffOn).
			Description("A list of status codes whereby the request should be considered to have failed and retries should be attempted, but the period between them should be increased gradually.").
			Advanced().
			Default([]any{429}),
		service.NewIntListField(hcFieldDropOn).
			Description("A list of status codes whereby the request should be considered to have failed but retries should not be attempted. This is useful for preventing wasted retries for requests that will never succeed. Note that with these status codes the _request_ is dropped, but _message_ that caused the request will not be dropped.").
			Advanced().
			Default([]any{}),
		service.NewIntListField(hcFieldSuccessfulOn).
			Description("A list of status codes whereby the attempt should be considered successful, this is useful for dropping requests that return non-2XX codes indicating that the message has been dealt with, such as a 303 See Other or a 409 Conflict. All 2XX codes are considered successful unless they are present within `backoff_on` or `drop_on`, regardless of this field.").
			Advanced().
			Default([]any{}),
		service.NewStringField(hcFieldProxyURL).
			Description("An optional HTTP proxy URL.").
			Advanced().
			Optional(),
	)

	innerFields = append(innerFields, extraChildren...)
	return service.NewObjectField("", innerFields...)
}

//------------------------------------------------------------------------------

// ConfigFromParsed attempts to parse an http client config struct from a parsed
// plugin config.
func ConfigFromParsed(pConf *service.ParsedConfig) (conf OldConfig, err error) {
	if conf.URL, err = pConf.FieldInterpolatedString(hcFieldURL); err != nil {
		return
	}
	if conf.Verb, err = pConf.FieldString(hcFieldVerb); err != nil {
		return
	}
	if conf.Headers, err = pConf.FieldInterpolatedStringMap(hcFieldHeaders); err != nil {
		return
	}
	if conf.Metadata, err = pConf.FieldMetadataFilter(hcFieldMetadata); err != nil {
		return
	}
	if conf.ExtractMetadata, err = pConf.FieldMetadataFilter(hcFieldExtractHeaders); err != nil {
		return
	}
	conf.RateLimit, _ = pConf.FieldString(hcFieldRateLimit)
	if conf.Timeout, err = pConf.FieldDuration(hcFieldTimeout); err != nil {
		return
	}
	if conf.Retry, err = pConf.FieldDuration(hcFieldRetryPeriod); err != nil {
		return
	}
	if conf.MaxBackoff, err = pConf.FieldDuration(hcFieldMaxRetryBackoff); err != nil {
		return
	}
	if conf.NumRetries, err = pConf.FieldInt(hcFieldRetries); err != nil {
		return
	}
	if conf.BackoffOn, err = pConf.FieldIntList(hcFieldBackoffOn); err != nil {
		return
	}
	if conf.DropOn, err = pConf.FieldIntList(hcFieldDropOn); err != nil {
		return
	}
	if conf.SuccessfulOn, err = pConf.FieldIntList(hcFieldSuccessfulOn); err != nil {
		return
	}
	conf.DumpRequestLogLevel, _ = pConf.FieldString(hcFieldDumpRequestLogLevel)
	if conf.TLSConf, conf.TLSEnabled, err = pConf.FieldTLSToggled(hcFieldTLS); err != nil {
		return
	}
	conf.ProxyURL, _ = pConf.FieldString(hcFieldProxyURL)
	if conf.authSigner, err = pConf.HTTPRequestAuthSignerFromParsed(); err != nil {
		return
	}
	if conf.clientCtor, err = oauth2ClientCtorFromParsed(pConf); err != nil {
		return
	}
	return
}

// OldConfig is a configuration struct for an HTTP client.
type OldConfig struct {
	URL                 *service.InterpolatedString
	Verb                string
	Headers             map[string]*service.InterpolatedString
	Metadata            *service.MetadataFilter
	ExtractMetadata     *service.MetadataFilter
	RateLimit           string
	Timeout             time.Duration
	Retry               time.Duration
	MaxBackoff          time.Duration
	NumRetries          int
	BackoffOn           []int
	DropOn              []int
	SuccessfulOn        []int
	DumpRequestLogLevel string
	TLSEnabled          bool
	TLSConf             *tls.Config
	ProxyURL            string
	authSigner          func(f fs.FS, req *http.Request) error
	clientCtor          func(context.Context, *http.Client) *http.Client
}
