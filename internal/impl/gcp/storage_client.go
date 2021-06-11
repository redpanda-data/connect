package gcp

import (
	"context"
	"net/http"
	"net/url"
	"os"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type transport struct {
	URL *url.URL
}

func (t transport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Host = t.URL.Host
	req.URL.Scheme = t.URL.Scheme
	return http.DefaultTransport.RoundTrip(req)
}

// NewStorageClient creates a storage.Client instance and, if the
// `GCP_CLOUD_STORAGE_EMULATOR_URL` env var is set, then a custom HTTPClient is
// used, which forwards all requests to the provided URL.
// Note: [this bug](https://github.com/googleapis/google-cloud-go/issues/2476)
// prevents the `STORAGE_EMULATOR_HOST` environment variable from working
// correctly, so this alternative approach is needed when using a Google Cloud
// Storage emulator.
func NewStorageClient(ctx context.Context) (*storage.Client, error) {
	var opts []option.ClientOption
	if rawURL := os.Getenv("GCP_CLOUD_STORAGE_EMULATOR_URL"); len(rawURL) > 0 {
		if u, err := url.Parse(rawURL); err == nil {
			opts = append(opts,
				option.WithHTTPClient(
					&http.Client{Transport: transport{URL: u}},
				),
			)
		}
	}
	return storage.NewClient(ctx, opts...)
}
