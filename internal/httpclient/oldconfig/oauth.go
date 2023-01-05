package oldconfig

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// OAuthConfig holds the configuration parameters for an OAuth exchange.
type OAuthConfig struct {
	Enabled           bool   `json:"enabled" yaml:"enabled"`
	ConsumerKey       string `json:"consumer_key" yaml:"consumer_key"`
	ConsumerSecret    string `json:"consumer_secret" yaml:"consumer_secret"`
	AccessToken       string `json:"access_token" yaml:"access_token"`
	AccessTokenSecret string `json:"access_token_secret" yaml:"access_token_secret"`
}

// NewOAuthConfig returns a new OAuthConfig with default values.
func NewOAuthConfig() OAuthConfig {
	return OAuthConfig{
		Enabled:           false,
		ConsumerKey:       "",
		ConsumerSecret:    "",
		AccessToken:       "",
		AccessTokenSecret: "",
	}
}

//------------------------------------------------------------------------------

// Sign method to sign an HTTP request for an OAuth exchange.
func (oauth OAuthConfig) Sign(req *http.Request) error {
	if !oauth.Enabled {
		return nil
	}

	nonceGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
	nonce := strconv.FormatInt(nonceGenerator.Int63(), 10)
	ts := fmt.Sprintf("%d", time.Now().Unix())

	params := &url.Values{}
	params.Add("oauth_consumer_key", oauth.ConsumerKey)
	params.Add("oauth_nonce", nonce)
	params.Add("oauth_signature_method", "HMAC-SHA1")
	params.Add("oauth_timestamp", ts)
	params.Add("oauth_token", oauth.AccessToken)
	params.Add("oauth_version", "1.0")

	sig, err := oauth.getSignature(req, params)
	if err != nil {
		return err
	}

	str := fmt.Sprintf(
		` oauth_consumer_key="%s", oauth_nonce="%s", oauth_signature="%s",`+
			` oauth_signature_method="%s", oauth_timestamp="%s",`+
			` oauth_token="%s", oauth_version="%s"`,
		url.QueryEscape(oauth.ConsumerKey),
		nonce,
		url.QueryEscape(sig),
		"HMAC-SHA1",
		ts,
		url.QueryEscape(oauth.AccessToken),
		"1.0",
	)
	req.Header.Add("Authorization", str)

	return nil
}

func (oauth OAuthConfig) getSignature(
	req *http.Request,
	params *url.Values,
) (string, error) {
	baseSignatureString := req.Method + "&" +
		url.QueryEscape(req.URL.String()) + "&" +
		url.QueryEscape(params.Encode())

	signingKey := url.QueryEscape(oauth.ConsumerSecret) + "&" +
		url.QueryEscape(oauth.AccessTokenSecret)

	return oauth.computeHMAC(baseSignatureString, signingKey)
}

func (oauth OAuthConfig) computeHMAC(
	message string,
	key string,
) (string, error) {
	h := hmac.New(sha1.New, []byte(key))
	if _, err := h.Write([]byte(message)); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}
