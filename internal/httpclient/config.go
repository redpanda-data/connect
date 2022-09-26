package httpclient

import (
	"github.com/Jeffail/gabs/v2"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/metadata"
	"github.com/benthosdev/benthos/v4/internal/tls"
)

// OldFieldSpec returns a field spec for an http client component.
func OldFieldSpec(forOutput bool, extraChildren ...docs.FieldSpec) docs.FieldSpec {
	httpSpecs := docs.FieldSpecs{
		docs.FieldString("url", "The URL to connect to.").IsInterpolated(),
		docs.FieldString("verb", "A verb to connect with", "POST", "GET", "DELETE"),
		docs.FieldString("headers", "A map of headers to add to the request.", map[string]any{
			"Content-Type": "application/octet-stream",
			"traceparent":  `${! tracing_span().traceparent }`,
		}).IsInterpolated().Map().HasDefault(map[string]any{}),
		docs.FieldObject("metadata", "Specify optional matching rules to determine which metadata keys should be added to the HTTP request as headers.").Advanced().
			WithChildren(metadata.IncludeFilterDocs()...),
	}

	extractHeadersDesc := "Specify which response headers should be added to resulting messages as metadata. Header keys are lowercased before matching, so ensure that your patterns target lowercased versions of the header keys that you expect."
	if forOutput {
		extractHeadersDesc = "Specify which response headers should be added to resulting synchronous response messages as metadata. Header keys are lowercased before matching, so ensure that your patterns target lowercased versions of the header keys that you expect. This field is not applicable unless `propagate_response` is set to `true`."
	}

	httpSpecs = append(httpSpecs, OldAuthFieldSpecsExpanded()...)
	httpSpecs = append(httpSpecs, tls.FieldSpec(),
		docs.FieldObject("extract_headers", extractHeadersDesc).WithChildren(metadata.IncludeFilterDocs()...).Advanced(),
		docs.FieldString("rate_limit", "An optional [rate limit](/docs/components/rate_limits/about) to throttle requests by."),
		docs.FieldString("timeout", "A static timeout to apply to requests."),
		docs.FieldString("retry_period", "The base period to wait between failed requests.").Advanced(),
		docs.FieldString("max_retry_backoff", "The maximum period to wait between failed requests.").Advanced(),
		docs.FieldInt("retries", "The maximum number of retry attempts to make.").Advanced(),
		docs.FieldInt("backoff_on", "A list of status codes whereby the request should be considered to have failed and retries should be attempted, but the period between them should be increased gradually.").Array().Advanced(),
		docs.FieldInt("drop_on", "A list of status codes whereby the request should be considered to have failed but retries should not be attempted. This is useful for preventing wasted retries for requests that will never succeed. Note that with these status codes the _request_ is dropped, but _message_ that caused the request will not be dropped.").Array().Advanced(),
		docs.FieldInt("successful_on", "A list of status codes whereby the attempt should be considered successful, this is useful for dropping requests that return non-2XX codes indicating that the message has been dealt with, such as a 303 See Other or a 409 Conflict. All 2XX codes are considered successful unless they are present within `backoff_on` or `drop_on`, regardless of this field.").Array().Advanced(),
		docs.FieldString("proxy_url", "An optional HTTP proxy URL.").Advanced(),
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
