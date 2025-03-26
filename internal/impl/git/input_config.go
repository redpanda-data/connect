// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package git

import (
	"fmt"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// gitInputConfig returns the configuration specification for the Git input plugin.
func gitInputConfig() *service.ConfigSpec {
	desc := `
The git input clones the specified repository (or pulls updates if already cloned) and reads 
the content of the specified file. It periodically polls the repository for new commits and emits 
a message when changes are detected.

== Metadata

This input adds the following metadata fields to each message:

- git_file_path
- git_file_size
- git_file_mode
- git_file_modified
- git_commit
- git_mime_type
- git_is_binary
- git_encoding (present if the file was base64 encoded)
- git_deleted (only present if the file was deleted)

You can access these metadata fields using function interpolation.`

	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.51.0").
		Summary(`A Git input that clones (or pulls) a repository and reads the repository contents.`).
		Description(desc).
		Fields(
			// General git cloning & polling settings
			service.NewStringField("repository_url").
				Description("The URL of the Git repository to clone.").
				Example("https://github.com/username/repo.git"),
			service.NewStringField("branch").
				Description("The branch to check out.").
				Default("main"),
			service.NewDurationField("poll_interval").
				Description("Duration between polling attempts").
				Default("10s").
				Example("10s"),
			service.NewStringListField("include_patterns").
				Description("A list of file patterns to include (e.g., '**/*.md', 'configs/*.yaml'). If empty, all files will be included. "+
					"Supports glob patterns: *, /**/, ?, and character ranges [a-z]. Any character with a special meaning can be escaped with a backslash.").
				Default([]any{}).
				Optional(),
			service.NewStringListField("exclude_patterns").
				Description("A list of file patterns to exclude (e.g., '.git/**', '**/*.png'). These patterns take precedence over include_patterns. "+
					"Supports glob patterns: *, /**/, ?, and character ranges [a-z]. Any character with a special meaning can be escaped with a backslash.").
				Default([]any{}).
				Optional(),
			service.NewIntField("max_file_size").
				Description("The maximum size of files to include in bytes. Files larger than this will be skipped. Set to 0 for no limit.").
				Default(10*1024*1024), // 10MB default

			// Checkpoint caching settings
			service.NewStringField("checkpoint_cache").
				Description("A cache resource to store the last processed commit hash, allowing the input to resume from where it left off after a restart.").
				Optional(),
			service.NewStringField("checkpoint_key").
				Description("The key to use when storing the last processed commit hash in the cache.").
				Default("git_last_commit").
				Optional(),

			// Authentication options
			service.NewObjectField("auth",
				// HTTP Basic Auth
				service.NewObjectField("basic",
					service.NewStringField("username").
						Description("Username for basic authentication").
						Default("").
						Optional(),
					service.NewStringField("password").
						Description("Password for basic authentication").
						Default("").
						Secret().
						Optional(),
				).
					Description("Basic authentication credentials").
					Optional(),
				// SSH key authentication (file or contents)
				service.NewObjectField("ssh_key",
					service.NewStringField("private_key_path").
						Description("Path to SSH private key file").
						Default("").
						Optional(),
					service.NewStringField("private_key").
						Description("SSH private key content").
						Default("").
						Secret().
						Optional(),
					service.NewStringField("passphrase").
						Description("Passphrase for the SSH private key").
						Default("").
						Secret().
						Optional(),
				).
					Description("SSH key authentication").
					Optional(),
				// Token-based authentication
				service.NewObjectField("token",
					service.NewStringField("value").
						Description("Token value for token-based authentication").
						Default("").
						Secret().
						Optional(),
				).
					Description("Token-based authentication").
					Optional(),
			).
				Description("Authentication options for the Git repository").
				Optional(),
		)
}

// inputCfg defines all config parameters that shall be considered by the git input.
type inputCfg struct {
	// repoURL is the URL of the Git repository to clone.
	repoURL string
	// branch is the Git branch to check out.
	branch string
	// pollInterval is the duration between repository update checks.
	pollInterval time.Duration
	// includePatterns is a list of glob file patterns to include.
	includePatterns []string
	// excludePatterns is a list of glob file patterns to exclude.
	excludePatterns []string
	// maxFileSize is the maximum size of binary files to include.
	maxFileSize int

	// checkpointCache is the name of the cache resource to store the last processed commit hash.
	checkpointCache string
	// checkpointKey is the key to use when storing the last processed commit hash.
	checkpointKey string

	// auth settings for cloning private git repositories.
	auth authConfig
}

// authConfig represents all authentication configurations.
type authConfig struct {
	basic  basicAuthConfig
	sshKey sshKeyAuthConfig
	token  tokenAuthConfig
}

// basicAuthConfig represents the configuration for basic authentication.
type basicAuthConfig struct {
	username string
	password string
}

// sshKeyAuthConfig represents the configuration for SSH key authentication.
type sshKeyAuthConfig struct {
	privateKeyPath string
	privateKey     string
	passphrase     string
}

// tokenAuthConfig represents the configuration for token authentication.
type tokenAuthConfig struct {
	value string
}

// parseBasicAuth parses the basic authentication configuration.
func parseBasicAuth(conf *service.ParsedConfig) (basicAuthConfig, error) {
	var auth basicAuthConfig

	if !conf.Contains("auth", "basic") {
		return auth, nil
	}

	var err error
	if conf.Contains("auth", "basic", "username") {
		auth.username, err = conf.FieldString("auth", "basic", "username")
		if err != nil {
			return auth, fmt.Errorf("failed to parse basic auth username: %w", err)
		}
	}

	if conf.Contains("auth", "basic", "password") {
		auth.password, err = conf.FieldString("auth", "basic", "password")
		if err != nil {
			return auth, fmt.Errorf("failed to parse basic auth password: %w", err)
		}
	}

	return auth, nil
}

// parseSSHKeyAuth parses the SSH key authentication configuration.
func parseSSHKeyAuth(conf *service.ParsedConfig) (sshKeyAuthConfig, error) {
	var auth sshKeyAuthConfig

	if !conf.Contains("auth", "ssh_key") {
		return auth, nil
	}

	var err error
	if conf.Contains("auth", "ssh_key", "private_key_path") {
		auth.privateKeyPath, err = conf.FieldString("auth", "ssh_key", "private_key_path")
		if err != nil {
			return auth, fmt.Errorf("failed to parse SSH private key path: %w", err)
		}
	}

	if conf.Contains("auth", "ssh_key", "private_key") {
		auth.privateKey, err = conf.FieldString("auth", "ssh_key", "private_key")
		if err != nil {
			return auth, fmt.Errorf("failed to parse SSH private key: %w", err)
		}
	}

	if conf.Contains("auth", "ssh_key", "passphrase") {
		auth.passphrase, err = conf.FieldString("auth", "ssh_key", "passphrase")
		if err != nil {
			return auth, fmt.Errorf("failed to parse SSH key passphrase: %w", err)
		}
	}

	return auth, nil
}

// parseTokenAuth parses the token authentication configuration.
func parseTokenAuth(conf *service.ParsedConfig) (tokenAuthConfig, error) {
	var auth tokenAuthConfig

	if !conf.Contains("auth", "token") {
		return auth, nil
	}

	var err error
	if conf.Contains("auth", "token", "value") {
		auth.value, err = conf.FieldString("auth", "token", "value")
		if err != nil {
			return auth, fmt.Errorf("failed to parse token value: %w", err)
		}
	}

	return auth, nil
}

// parseAuthConfig parses all authentication configurations.
func parseAuthConfig(conf *service.ParsedConfig) (authConfig, error) {
	var auth authConfig

	if !conf.Contains("auth") {
		return auth, nil
	}

	var err error
	auth.basic, err = parseBasicAuth(conf)
	if err != nil {
		return auth, err
	}

	auth.sshKey, err = parseSSHKeyAuth(conf)
	if err != nil {
		return auth, err
	}

	auth.token, err = parseTokenAuth(conf)
	if err != nil {
		return auth, err
	}

	return auth, nil
}

// inputCfgFromParsed constructs an inputCfg by extracting fields from parsedCfg,
// returning an error if any field parsing fails.
func inputCfgFromParsed(parsedCfg *service.ParsedConfig) (inputCfg, error) {
	var conf inputCfg
	var err error

	if conf.repoURL, err = parsedCfg.FieldString("repository_url"); err != nil {
		return conf, err
	}

	if conf.branch, err = parsedCfg.FieldString("branch"); err != nil {
		return conf, err
	}

	if conf.pollInterval, err = parsedCfg.FieldDuration("poll_interval"); err != nil {
		return conf, err
	}

	if conf.includePatterns, err = parsedCfg.FieldStringList("include_patterns"); err != nil {
		return conf, err
	}

	// Patterns are validated at runtime as well, but we want to give early feedback to
	// avoid issues at runtime.
	for _, pattern := range conf.includePatterns {
		isValid := doublestar.ValidatePathPattern(pattern)
		if !isValid {
			return conf, fmt.Errorf("pattern %q is not a supported glob pattern", pattern)
		}
	}

	if conf.excludePatterns, err = parsedCfg.FieldStringList("exclude_patterns"); err != nil {
		return conf, err
	}

	for _, pattern := range conf.excludePatterns {
		isValid := doublestar.ValidatePathPattern(pattern)
		if !isValid {
			return conf, fmt.Errorf("pattern %q is not a supported glob pattern", pattern)
		}
	}

	if conf.maxFileSize, err = parsedCfg.FieldInt("max_file_size"); err != nil {
		return conf, err
	}

	// Parse authentication configuration
	conf.auth, err = parseAuthConfig(parsedCfg)
	if err != nil {
		return conf, err
	}

	// Parse checkpoint cache settings
	if parsedCfg.Contains("checkpoint_cache") {
		if conf.checkpointCache, err = parsedCfg.FieldString("checkpoint_cache"); err != nil {
			return conf, err
		}
	}
	if conf.checkpointKey, err = parsedCfg.FieldString("checkpoint_key"); err != nil {
		return conf, err
	}

	return conf, nil
}
