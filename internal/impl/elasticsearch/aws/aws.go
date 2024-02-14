package aws

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"

	baws "github.com/benthosdev/benthos/v4/internal/impl/aws"
	"github.com/benthosdev/benthos/v4/internal/impl/elasticsearch"
	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

func init() {
	elasticsearch.AWSOptFn = func(conf *service.ParsedConfig) ([]elastic.ClientOptionFunc, error) {
		if enabled, _ := conf.FieldBool(elasticsearch.ESOFieldAWSEnabled); !enabled {
			return nil, nil
		}

		tsess, err := baws.GetSession(context.TODO(), conf)
		if err != nil {
			return nil, err
		}

		region := tsess.Region
		if region == "" {
			return nil, errors.New("unable to detect target AWS region, if you encounter this error please report it via: https://github.com/benthosdev/benthos/issues/new")
		}

		creds, err := tsess.Credentials.Retrieve(context.TODO())
		if err != nil {
			return nil, err
		}

		signingClient := newV4SigningClientWithHTTPClient(creds, region, http.DefaultClient)
		return []elastic.ClientOptionFunc{elastic.SetHttpClient(signingClient)}, nil
	}
}

func newV4SigningClientWithHTTPClient(creds aws.Credentials, region string, httpClient *http.Client) *http.Client {
	return &http.Client{
		Transport: Transport{
			client: httpClient,
			creds:  creds,
			signer: v4.NewSigner(),
			region: region,
		},
	}
}

// Transport is a RoundTripper that will sign requests with AWS V4 Signing
type Transport struct {
	client *http.Client
	creds  aws.Credentials
	signer *v4.Signer
	region string
}

// RoundTrip uses the underlying RoundTripper transport, but signs request first with AWS V4 Signing
func (st Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if h, ok := req.Header["Authorization"]; ok && len(h) > 0 && strings.HasPrefix(h[0], "AWS4") {
		// Received a signed request, just pass it on.
		return st.client.Do(req)
	}

	if strings.Contains(req.URL.RawPath, "%2C") {
		// Escaping path
		req.URL.RawPath = url.PathEscape(req.URL.RawPath)
	}

	hash, err := hexEncodedSha256OfRequest(req)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Amz-Content-Sha256", hash)

	if err := st.signer.SignHTTP(req.Context(), st.creds, req, hash, "es", st.region, time.Now().UTC()); err != nil {
		return nil, err
	}
	return st.client.Do(req)
}

func hexEncodedSha256OfRequest(r *http.Request) (string, error) {
	if r.Body == nil {
		return "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", nil
	}

	hasher := sha256.New()

	reqBodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return "", err
	}

	if err := r.Body.Close(); err != nil {
		return "", err
	}

	r.Body = io.NopCloser(bytes.NewBuffer(reqBodyBytes))
	hasher.Write(reqBodyBytes)
	digest := hasher.Sum(nil)

	return hex.EncodeToString(digest), nil
}
