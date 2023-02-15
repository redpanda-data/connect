package httpclient

import (
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/golang-jwt/jwt"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

// AuthConfig contains configuration params for various HTTP auth strategies.
type AuthConfig struct {
	OAuth     OAuthConfig     `json:"oauth" yaml:"oauth"`
	BasicAuth BasicAuthConfig `json:"basic_auth" yaml:"basic_auth"`
	JWT       JWTConfig       `json:"jwt" yaml:"jwt"`
}

// NewAuthConfig creates a new Config with default values.
func NewAuthConfig() AuthConfig {
	return AuthConfig{
		OAuth:     NewOAuthConfig(),
		BasicAuth: NewBasicAuthConfig(),
		JWT:       NewJWTConfig(),
	}
}

// Sign method to sign an HTTP request for configured auth strategies.
func (c AuthConfig) Sign(f ifs.FS, req *http.Request) error {
	if err := c.OAuth.Sign(req); err != nil {
		return err
	}
	if err := c.JWT.Sign(f, req); err != nil {
		return err
	}
	return c.BasicAuth.Sign(req)
}

//------------------------------------------------------------------------------

// BasicAuthConfig contains fields for setting basic authentication in HTTP
// requests.
type BasicAuthConfig struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// NewBasicAuthConfig returns a default configuration for basic authentication
// in HTTP client requests.
func NewBasicAuthConfig() BasicAuthConfig {
	return BasicAuthConfig{
		Enabled:  false,
		Username: "",
		Password: "",
	}
}

// Sign method to sign an HTTP request for an OAuth exchange.
func (basic BasicAuthConfig) Sign(req *http.Request) error {
	if basic.Enabled {
		req.SetBasicAuth(basic.Username, basic.Password)
	}
	return nil
}

//------------------------------------------------------------------------------

// JWTConfig holds the configuration parameters for an JWT exchange.
type JWTConfig struct {
	Enabled        bool           `json:"enabled" yaml:"enabled"`
	Claims         jwt.MapClaims  `json:"claims" yaml:"claims"`
	Headers        map[string]any `json:"headers" yaml:"headers"`
	SigningMethod  string         `json:"signing_method" yaml:"signing_method"`
	PrivateKeyFile string         `json:"private_key_file" yaml:"private_key_file"`

	// internal private fields
	keyMx *sync.Mutex
	key   *crypto.PrivateKey
}

// NewJWTConfig returns a new JWTConfig with default values.
func NewJWTConfig() JWTConfig {
	var key crypto.PrivateKey
	return JWTConfig{
		Enabled:        false,
		Claims:         map[string]any{},
		Headers:        map[string]any{},
		SigningMethod:  "",
		PrivateKeyFile: "",
		keyMx:          &sync.Mutex{},
		key:            &key,
	}
}

// Sign method to sign an HTTP request for an JWT exchange.
func (j JWTConfig) Sign(f ifs.FS, req *http.Request) error {
	if !j.Enabled {
		return nil
	}

	if err := j.parsePrivateKey(f); err != nil {
		return err
	}

	var token *jwt.Token
	switch j.SigningMethod {
	case "RS256":
		token = jwt.NewWithClaims(jwt.SigningMethodRS256, j.Claims)
	case "RS384":
		token = jwt.NewWithClaims(jwt.SigningMethodRS384, j.Claims)
	case "RS512":
		token = jwt.NewWithClaims(jwt.SigningMethodRS512, j.Claims)
	case "EdDSA":
		token = jwt.NewWithClaims(jwt.SigningMethodEdDSA, j.Claims)
	default:
		return fmt.Errorf("jwt signing method %s not acepted. Try with RS256, RS384, RS512 or EdDSA", j.SigningMethod)
	}

	for name, value := range j.Headers {
		token.Header[name] = value
	}

	ss, err := token.SignedString(*j.key)
	if err != nil {
		return fmt.Errorf("failed to sign jwt: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+ss)
	return nil
}

// parsePrivateKey parses once the RSA private key.
// Needs mutex locking as Sign might be called by parallel threads.
func (j JWTConfig) parsePrivateKey(fs ifs.FS) error {
	j.keyMx.Lock()
	defer j.keyMx.Unlock()

	if *j.key != nil {
		return nil
	}

	privateKey, err := ifs.ReadFile(fs, j.PrivateKeyFile)
	if err != nil {
		return fmt.Errorf("failed to read private key: %v", err)
	}

	switch j.SigningMethod {
	case "RS256", "RS384", "RS512":
		*j.key, err = jwt.ParseRSAPrivateKeyFromPEM(privateKey)
	case "EdDSA":
		*j.key, err = jwt.ParseEdPrivateKeyFromPEM(privateKey)
	}
	if err != nil {
		return fmt.Errorf("failed to parse %s private key: %v", j.SigningMethod, err)
	}

	return nil
}

//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------

// OAuth2Config holds the configuration parameters for an OAuth2 exchange.
type OAuth2Config struct {
	Enabled      bool     `json:"enabled" yaml:"enabled"`
	ClientKey    string   `json:"client_key" yaml:"client_key"`
	ClientSecret string   `json:"client_secret" yaml:"client_secret"`
	TokenURL     string   `json:"token_url" yaml:"token_url"`
	Scopes       []string `json:"scopes" yaml:"scopes"`
}

// NewOAuth2Config returns a new OAuth2Config with default values.
func NewOAuth2Config() OAuth2Config {
	return OAuth2Config{
		Enabled:      false,
		ClientKey:    "",
		ClientSecret: "",
		TokenURL:     "",
		Scopes:       []string{},
	}
}

// Client returns an http.Client with OAuth2 configured.
func (oauth OAuth2Config) Client(ctx context.Context) *http.Client {
	if !oauth.Enabled {
		var client http.Client
		return &client
	}

	conf := &clientcredentials.Config{
		ClientID:     oauth.ClientKey,
		ClientSecret: oauth.ClientSecret,
		TokenURL:     oauth.TokenURL,
		Scopes:       oauth.Scopes,
	}

	return conf.Client(ctx)
}
