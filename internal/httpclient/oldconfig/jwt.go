package oldconfig

import (
	"crypto/rsa"
	"fmt"
	"net/http"
	"sync"

	"github.com/golang-jwt/jwt"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

// JWTConfig holds the configuration parameters for an JWT exchange.
type JWTConfig struct {
	Enabled        bool           `json:"enabled" yaml:"enabled"`
	Claims         jwt.MapClaims  `json:"claims" yaml:"claims"`
	Headers        map[string]any `json:"headers" yaml:"headers"`
	SigningMethod  string         `json:"signing_method" yaml:"signing_method"`
	PrivateKeyFile string         `json:"private_key_file" yaml:"private_key_file"`

	// internal private fields
	rsaKeyMx *sync.Mutex
	rsaKey   **rsa.PrivateKey
}

// NewJWTConfig returns a new JWTConfig with default values.
func NewJWTConfig() JWTConfig {
	var privKey *rsa.PrivateKey
	return JWTConfig{
		Enabled:        false,
		Claims:         map[string]any{},
		Headers:        map[string]any{},
		SigningMethod:  "",
		PrivateKeyFile: "",
		rsaKeyMx:       &sync.Mutex{},
		rsaKey:         &privKey,
	}
}

//------------------------------------------------------------------------------

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
	default:
		return fmt.Errorf("jwt signing method %s not acepted. Try with RS256, RS384 or RS512", j.SigningMethod)
	}

	for name, value := range j.Headers {
		token.Header[name] = value
	}

	ss, err := token.SignedString(*j.rsaKey)
	if err != nil {
		return fmt.Errorf("failed to sign jwt: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+ss)
	return nil
}

// parsePrivateKey parses once the RSA private key.
// Needs mutex locking as Sign might be called by parallel threads.
func (j JWTConfig) parsePrivateKey(fs ifs.FS) error {
	j.rsaKeyMx.Lock()
	defer j.rsaKeyMx.Unlock()

	if *j.rsaKey != nil {
		return nil
	}

	privateKey, err := ifs.ReadFile(fs, j.PrivateKeyFile)
	if err != nil {
		return fmt.Errorf("failed to read private key: %v", err)
	}

	*j.rsaKey, err = jwt.ParseRSAPrivateKeyFromPEM(privateKey)
	if err != nil {
		return fmt.Errorf("failed to parse private key: %v", err)
	}

	return nil
}
