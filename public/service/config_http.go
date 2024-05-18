package service

import (
	"crypto"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io/fs"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	aFieldBasicAuth = "basic_auth"
	aFieldOAuth     = "oauth"
	aFieldJWT       = "jwt"
)

// NewHTTPRequestAuthSignerFields returns a list of config fields for adding
// authentication to HTTP requests. The options available with this field
// include OAuth (v1), basic authentication, and JWT as these are mechanisms
// that can be implemented by mutating a request object.
func NewHTTPRequestAuthSignerFields() []*ConfigField {
	return []*ConfigField{
		oAuthFieldSpec(),
		basicAuthField(),
		jwtFieldSpec(),
	}
}

// HTTPRequestAuthSignerFromParsed takes a parsed config which is expected to
// contain fields from NewHTTPRequestAuthSignerFields, and returns a func that
// applies those configured authentication mechanisms to a given HTTP request.
func (p *ParsedConfig) HTTPRequestAuthSignerFromParsed() (fn func(fs.FS, *http.Request) error, err error) {
	var oldConf authConfig
	if oldConf.OAuth, err = oauthFromParsed(p); err != nil {
		return
	}
	if oldConf.BasicAuth, err = basicAuthFromParsed(p); err != nil {
		return
	}
	if oldConf.JWT, err = jwtAuthFromParsed(p); err != nil {
		return
	}
	fn = oldConf.Sign
	return
}

type authConfig struct {
	OAuth     oauthConfig
	BasicAuth basicAuthConfig
	JWT       jwtConfig
}

// Sign method to sign an HTTP request for configured auth strategies.
func (c authConfig) Sign(f fs.FS, req *http.Request) error {
	if err := c.OAuth.Sign(req); err != nil {
		return err
	}
	if err := c.JWT.Sign(f, req); err != nil {
		return err
	}
	return c.BasicAuth.Sign(req)
}

//------------------------------------------------------------------------------

const (
	abFieldEnabled  = "enabled"
	abFieldUsername = "username"
	abFieldPassword = "password"
)

func basicAuthField() *ConfigField {
	return NewObjectField(aFieldBasicAuth,
		NewBoolField(abFieldEnabled).
			Description("Whether to use basic authentication in requests.").
			Default(false),

		NewStringField(abFieldUsername).
			Description("A username to authenticate as.").
			Default(""),

		NewStringField(abFieldPassword).
			Description("A password to authenticate with.").
			Default("").Secret(),
	).Description("Allows you to specify basic authentication.").
		Advanced().
		Optional()
}

func basicAuthFromParsed(conf *ParsedConfig) (res basicAuthConfig, err error) {
	if !conf.Contains(aFieldBasicAuth) {
		return
	}
	conf = conf.Namespace(aFieldBasicAuth)
	if res.Enabled, err = conf.FieldBool(abFieldEnabled); err != nil {
		return
	}
	if res.Username, err = conf.FieldString(abFieldUsername); err != nil {
		return
	}
	if res.Password, err = conf.FieldString(abFieldPassword); err != nil {
		return
	}
	return
}

type basicAuthConfig struct {
	Enabled  bool
	Username string
	Password string
}

// Sign method to sign an HTTP request for an OAuth exchange.
func (basic basicAuthConfig) Sign(req *http.Request) error {
	if basic.Enabled {
		req.SetBasicAuth(basic.Username, basic.Password)
	}
	return nil
}

//------------------------------------------------------------------------------

const (
	aoFieldEnabled           = "enabled"
	aoFieldConsumerKey       = "consumer_key"
	aoFieldConsumerSecret    = "consumer_secret"
	aoFieldAccessToken       = "access_token"
	aoFieldAccessTokenSecret = "access_token_secret"
)

func oAuthFieldSpec() *ConfigField {
	return NewObjectField(aFieldOAuth,
		NewBoolField(aoFieldEnabled).
			Description("Whether to use OAuth version 1 in requests.").
			Default(false),

		NewStringField(aoFieldConsumerKey).
			Description("A value used to identify the client to the service provider.").
			Default(""),

		NewStringField(aoFieldConsumerSecret).
			Description("A secret used to establish ownership of the consumer key.").
			Default("").Secret(),

		NewStringField(aoFieldAccessToken).
			Description("A value used to gain access to the protected resources on behalf of the user.").
			Default(""),

		NewStringField(aoFieldAccessTokenSecret).
			Description("A secret provided in order to establish ownership of a given access token.").
			Default("").Secret(),
	).
		Description("Allows you to specify open authentication via OAuth version 1.").
		Advanced().
		Optional()
}

func oauthFromParsed(conf *ParsedConfig) (res oauthConfig, err error) {
	if !conf.Contains(aFieldOAuth) {
		return
	}
	conf = conf.Namespace(aFieldOAuth)
	if res.Enabled, err = conf.FieldBool(aoFieldEnabled); err != nil {
		return
	}
	if res.ConsumerKey, err = conf.FieldString(aoFieldConsumerKey); err != nil {
		return
	}
	if res.ConsumerSecret, err = conf.FieldString(aoFieldConsumerSecret); err != nil {
		return
	}
	if res.AccessToken, err = conf.FieldString(aoFieldAccessToken); err != nil {
		return
	}
	if res.AccessTokenSecret, err = conf.FieldString(aoFieldAccessTokenSecret); err != nil {
		return
	}
	return
}

type oauthConfig struct {
	Enabled           bool
	ConsumerKey       string
	ConsumerSecret    string
	AccessToken       string
	AccessTokenSecret string
}

// Sign method to sign an HTTP request for an OAuth exchange.
func (oauth oauthConfig) Sign(req *http.Request) error {
	if !oauth.Enabled {
		return nil
	}

	nonceGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
	nonce := strconv.FormatInt(nonceGenerator.Int63(), 10)
	ts := strconv.FormatInt(time.Now().Unix(), 10)

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

func (oauth oauthConfig) getSignature(
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

func (oauth oauthConfig) computeHMAC(
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

const (
	ajFieldEnabled        = "enabled"
	ajFieldPrivateKeyFile = "private_key_file"
	ajFieldSigningMethod  = "signing_method"
	ajFieldClaims         = "claims"
	ajFieldHeaders        = "headers"
)

func jwtFieldSpec() *ConfigField {
	return NewObjectField(aFieldJWT,
		NewBoolField(ajFieldEnabled).
			Description("Whether to use JWT authentication in requests.").
			Default(false),

		NewStringField(ajFieldPrivateKeyFile).
			Description("A file with the PEM encoded via PKCS1 or PKCS8 as private key.").
			Default(""),

		NewStringField(ajFieldSigningMethod).
			Description("A method used to sign the token such as RS256, RS384, RS512 or EdDSA.").
			Default(""),

		NewAnyMapField(ajFieldClaims).
			Description("A value used to identify the claims that issued the JWT.").
			Default(map[string]any{}).
			Advanced(),

		NewAnyMapField(ajFieldHeaders).
			Description("Add optional key/value headers to the JWT.").
			Default(map[string]any{}).
			Advanced(),
	).
		Description("BETA: Allows you to specify JWT authentication.").
		Advanced()
}

func jwtAuthFromParsed(conf *ParsedConfig) (res jwtConfig, err error) {
	if !conf.Contains(aFieldJWT) {
		return
	}

	var key crypto.PrivateKey
	res.key = &key
	res.keyMx = &sync.Mutex{}

	conf = conf.Namespace(aFieldJWT)
	if res.Enabled, err = conf.FieldBool(ajFieldEnabled); err != nil {
		return
	}
	var claimsConfs map[string]*ParsedConfig
	if claimsConfs, err = conf.FieldAnyMap(ajFieldClaims); err != nil {
		return
	}
	res.Claims = jwt.MapClaims{}
	for k, v := range claimsConfs {
		if res.Claims[k], err = v.FieldAny(); err != nil {
			return
		}
	}
	var headersConfs map[string]*ParsedConfig
	if headersConfs, err = conf.FieldAnyMap(ajFieldHeaders); err != nil {
		return
	}
	res.Headers = map[string]any{}
	for k, v := range headersConfs {
		if res.Headers[k], err = v.FieldAny(); err != nil {
			return
		}
	}
	if res.SigningMethod, err = conf.FieldString(ajFieldSigningMethod); err != nil {
		return
	}
	if res.PrivateKeyFile, err = conf.FieldString(ajFieldPrivateKeyFile); err != nil {
		return
	}
	return
}

type jwtConfig struct {
	Enabled        bool
	Claims         jwt.MapClaims
	Headers        map[string]any
	SigningMethod  string
	PrivateKeyFile string

	// internal private fields
	keyMx *sync.Mutex
	key   *crypto.PrivateKey
}

// Sign method to sign an HTTP request for an JWT exchange.
func (j jwtConfig) Sign(f fs.FS, req *http.Request) error {
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
func (j jwtConfig) parsePrivateKey(fs fs.FS) error {
	j.keyMx.Lock()
	defer j.keyMx.Unlock()

	if *j.key != nil {
		return nil
	}

	privateKey, err := ReadFile(fs, j.PrivateKeyFile)
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
