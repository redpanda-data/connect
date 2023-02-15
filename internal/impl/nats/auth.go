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

	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/public/service"
)

func authConfToOptions(auth auth.Config, fs *service.FS) []nats.Option {
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
	// switch to the nats.UserJWT option, while still preserving the behavior
	// of the nats.UserCredentials option, which includes things like path
	// expansing, home directory support and wiping credentials held in memory
	if auth.UserCredentialsFile != "" {
		opts = append(opts, nats.UserJWT(
			userJWTHandler(auth.UserCredentialsFile, fs),
			sigHandler(auth.UserCredentialsFile, fs),
		))
	}

	return opts
}

// AuthFromParsedConfig attempts to extract an auth config from a ParsedConfig.
func AuthFromParsedConfig(p *service.ParsedConfig) (c auth.Config, err error) {
	c = auth.New()
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
