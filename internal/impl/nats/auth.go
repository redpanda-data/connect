package nats

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func authDescription() string {
	return `

== Authentication

There are several components within Benthos which uses NATS services. You will find that each of these components
support optional advanced authentication parameters for https://docs.nats.io/nats-server/configuration/securing_nats/auth_intro/nkey_auth[NKeys^]
and https://docs.nats.io/using-nats/developer/connecting/creds[User Credentials^].

See an https://docs.nats.io/running-a-nats-service/nats_admin/security/jwt[in-depth tutorial^].

=== NKey file

The NATS server can use these NKeys in several ways for authentication. The simplest is for the server to be configured
with a list of known public keys and for the clients to respond to the challenge by signing it with its private NKey
configured in the ` + "`nkey_file`" + ` field.

https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro/nkey_auth[More details^].

=== User credentials

NATS server supports decentralized authentication based on JSON Web Tokens (JWT). Clients need an https://docs.nats.io/nats-server/configuration/securing_nats/jwt#json-web-tokens[user JWT^]
and a corresponding https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro/nkey_auth[NKey secret^] when connecting to a server
which is configured to use this authentication scheme.

The ` + "`user_credentials_file`" + ` field should point to a file containing both the private key and the JWT and can be
generated with the https://docs.nats.io/nats-tools/nsc[nsc tool^].

Alternatively, the ` + "`user_jwt`" + ` field can contain a plain text JWT and the ` + "`user_nkey_seed`" + `can contain
the plain text NKey Seed.

https://docs.nats.io/using-nats/developer/connecting/creds[More details^].`
}

func authFieldSpec() *service.ConfigField {
	return service.NewObjectField("auth",
		service.NewStringField("nkey_file").
			Description("An optional file containing a NKey seed.").
			Example("./seed.nk").
			Optional(),
		service.NewStringField("user_credentials_file").
			Description("An optional file containing user credentials which consist of an user JWT and corresponding NKey seed.").
			Example("./user.creds").
			Optional(),
		service.NewStringField("user_jwt").
			Description("An optional plain text user JWT (given along with the corresponding user NKey Seed).").
			Secret().
			Optional(),
		service.NewStringField("user_nkey_seed").
			Description("An optional plain text user NKey Seed (given along with the corresponding user JWT).").
			Secret().
			Optional(),
	).Description("Optional configuration of NATS authentication parameters.").
		Advanced()
}

type authConfig struct {
	NKeyFile            string
	UserCredentialsFile string
	UserJWT             string
	UserNkeySeed        string
}

//------------------------------------------------------------------------------

func authConfToOptions(auth authConfig, fs *service.FS) []nats.Option {
	var opts []nats.Option
	if auth.NKeyFile != "" {
		if opt, err := nats.NkeyOptionFromSeed(auth.NKeyFile); err != nil {
			opts = append(opts, func(*nats.Options) error { return err })
		} else {
			opts = append(opts, opt)
		}
	}

	// Previously we used nats.UserCredentials to authenticate. In order to
	// support a custom FS implementation in our NATS components, we needed to
	// switch to the nats.UserJWT option, while still preserving the behaviour
	// of the nats.UserCredentials option, which includes things like path
	// expansing, home directory support and wiping credentials held in memory
	if auth.UserCredentialsFile != "" {
		opts = append(opts, nats.UserJWT(
			userJWTHandler(auth.UserCredentialsFile, fs),
			sigHandler(auth.UserCredentialsFile, fs),
		))
	}

	if auth.UserJWT != "" && auth.UserNkeySeed != "" {
		opts = append(opts, nats.UserJWTAndSeed(
			auth.UserJWT, auth.UserNkeySeed,
		))
	}

	return opts
}

// AuthFromParsedConfig attempts to extract an auth config from a ParsedConfig.
func AuthFromParsedConfig(p *service.ParsedConfig) (c authConfig, err error) {
	if p.Contains("nkey_file") {
		if c.NKeyFile, err = p.FieldString("nkey_file"); err != nil {
			return
		}
	}
	if p.Contains("user_credentials_file") {
		if c.UserCredentialsFile, err = p.FieldString("user_credentials_file"); err != nil {
			return
		}
	}
	if p.Contains("user_jwt") || p.Contains("user_nkey_seed") {
		if !p.Contains("user_jwt") {
			err = errors.New("missing auth.user_jwt config field")
			return
		}
		if !p.Contains("user_nkey_seed") {
			err = errors.New("missing auth.user_nkey_seed config field")
			return
		}
		if c.UserJWT, err = p.FieldString("user_jwt"); err != nil {
			return
		}
		if c.UserNkeySeed, err = p.FieldString("user_nkey_seed"); err != nil {
			return
		}
	}
	return
}

func userJWTHandler(filename string, fs *service.FS) nats.UserJWTHandler {
	return func() (string, error) {
		contents, err := loadFileContents(filename, fs)
		if err != nil {
			return "", err
		}
		defer wipeSlice(contents)

		return nkeys.ParseDecoratedJWT(contents)
	}
}

func sigHandler(filename string, fs *service.FS) nats.SignatureHandler {
	return func(nonce []byte) ([]byte, error) {
		contents, err := loadFileContents(filename, fs)
		if err != nil {
			return nil, err
		}
		defer wipeSlice(contents)

		kp, err := nkeys.ParseDecoratedNKey(contents)
		if err != nil {
			return nil, fmt.Errorf("unable to extract key pair from file %q: %v", filename, err)
		}
		defer kp.Wipe()

		sig, _ := kp.Sign(nonce)
		return sig, nil
	}
}

// Just wipe slice with 'x', for clearing contents of creds or nkey seed file.
func wipeSlice(buf []byte) {
	for i := range buf {
		buf[i] = 'x'
	}
}

func expandPath(p string) (string, error) {
	p = os.ExpandEnv(p)

	if !strings.HasPrefix(p, "~") {
		return p, nil
	}

	home, err := homeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(home, p[1:]), nil
}

func homeDir() (string, error) {
	if runtime.GOOS == "windows" {
		homeDrive, homePath := os.Getenv("HOMEDRIVE"), os.Getenv("HOMEPATH")
		userProfile := os.Getenv("USERPROFILE")

		var home string
		if homeDrive == "" || homePath == "" {
			if userProfile == "" {
				return "", errors.New("nats: failed to get home dir, require %HOMEDRIVE% and %HOMEPATH% or %USERPROFILE%")
			}
			home = userProfile
		} else {
			home = filepath.Join(homeDrive, homePath)
		}

		return home, nil
	}

	home := os.Getenv("HOME")
	if home == "" {
		return "", errors.New("nats: failed to get home dir, require $HOME")
	}
	return home, nil
}

func loadFileContents(filename string, fs *service.FS) ([]byte, error) {
	path, err := expandPath(filename)
	if err != nil {
		return nil, err
	}

	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return io.ReadAll(f)
}
