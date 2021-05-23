package auth

import (
	"crypto/rsa"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/dgrijalva/jwt-go"
)

//------------------------------------------------------------------------------

// JWTConfig holds the configuration parameters for an JWT exchange.
type JWTConfig struct {
	Enabled       bool          `json:"enabled" yaml:"enabled"`
	Claims        jwt.MapClaims `json:"claims" yaml:"claims"`
	SigningMethod string        `json:"signing_method" yaml:"signing_method"`
	PrivateKey    string        `json:"private_key" yaml:"private_key"`

	// internal private fields
	rsaKey *rsa.PrivateKey
}

// NewJWTConfig returns a new JWTConfig with default values.
func NewJWTConfig() JWTConfig {
	return JWTConfig{
		Enabled:       false,
		Claims:        nil,
		SigningMethod: "",
		PrivateKey:    "",
	}
}

//------------------------------------------------------------------------------

// Sign method to sign an HTTP request for an JWT exchange.
func (j JWTConfig) Sign(req *http.Request) error {
	if !j.Enabled {
		return nil
	}

	// Must parse private key only once
	if j.rsaKey == nil {
		privateKey, err := ioutil.ReadFile(j.PrivateKey)
		if err != nil {
			return fmt.Errorf("failed to read private key: %v", err)
		}

		j.rsaKey, err = jwt.ParseRSAPrivateKeyFromPEM(privateKey)
		if err != nil {
			return fmt.Errorf("failed to parse private key: %v", err)
		}
	}

	var bearer *jwt.Token
	switch j.SigningMethod {
	case "RS256":
		bearer = jwt.NewWithClaims(jwt.SigningMethodRS256, j.Claims)
	case "RS384":
		bearer = jwt.NewWithClaims(jwt.SigningMethodRS384, j.Claims)
	case "RS512":
		bearer = jwt.NewWithClaims(jwt.SigningMethodRS512, j.Claims)
	default:
		return fmt.Errorf("jwt signing method %s not acepted. Try with RS256, RS384 or RS512", j.SigningMethod)
	}

	ss, err := bearer.SignedString(j.rsaKey)
	if err != nil {
		return fmt.Errorf("failed to sign jwt: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+ss)
	return nil
}

//------------------------------------------------------------------------------
