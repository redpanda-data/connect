package vault

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	"github.com/hashicorp/vault-client-go"
	"github.com/hashicorp/vault-client-go/schema"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var (
	_                                service.Processor = (*processor)(nil)
	spec                             *service.ConfigSpec
	errLoginResponseEmpty            = errors.New("login responded with unexpected empty response")
	errLoginResponseMissingAuth      = errors.New("login responded with unexpected missing auth")
	errLoginResponseEmptyClientToken = errors.New("login responded with unexpected missing or empty client_token")
)

func init() {
	spec = service.NewConfigSpec().
		Beta().
		Summary("Fetches a Value for a Key from Hashicorp Vault").
		Description(`
The fields `+"`mount_path`"+`, `+"`path`"+` and `+"`version`"+` support
xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions], allowing
you to create a unique `+"`mount_path`"+`, `+"`path`"+` and/or `+"`version`"+` for each message.

`).
		Fields(
			service.NewStringField("url").
				Description("The base URL of the Vault server."),
			service.NewObjectField(
				"auth",
				service.NewStringField("mount_path").
					Optional(),
				service.NewObjectField(
					"app_role",
					service.NewStringField("role_id").
						Description("Unique identifier of the Role").
						Secret(),
					service.NewStringField("secret_id").
						Description("SecretID belong to the App role").
						Secret(),
				),
			),
			service.NewBloblangField("mount_path").
				Description(`Supports xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions].
`).
				Optional(),
			service.NewBloblangField("path").
				Description(`The key path to fetch from Vault.
Supports xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions].
If root gets deleted no message gets produced.
`),
			service.NewInterpolatedStringField("version").
				Description(`The specific key version to fetch from Vault.
Supports xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions].
`).
				Optional(),
		)
	err := service.RegisterProcessor("vault_key", spec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {

	url, err := conf.FieldString("url")
	if err != nil {
		return nil, fmt.Errorf("missing url for Vault: %w", err)
	}
	if strings.TrimSpace(url) == "" {
		return nil, fmt.Errorf("unexpected empty url for Vault AppRole login: %w", err)
	}

	roleID, err := conf.FieldString("auth", "app_role", "role_id")
	if err != nil {
		return nil, fmt.Errorf("missing role_id for Vault AppRole login: %w", err)
	}
	if strings.TrimSpace(roleID) == "" {
		return nil, fmt.Errorf("unexpected empty role_id for Vault AppRole login: %w", err)
	}

	secretID, err := conf.FieldString("auth", "app_role", "secret_id")
	if err != nil {
		return nil, fmt.Errorf("missing secret_id for Vault AppRole login: %w", err)
	}
	if strings.TrimSpace(secretID) == "" {
		return nil, fmt.Errorf("unexpected empty secret_id for Vault AppRole login: %w", err)
	}

	client, err := vault.New(
		vault.WithAddress(url),
	)
	if err != nil {
		return nil, fmt.Errorf("failed creating Vault client: %w", err)
	}

	var authOptions []vault.RequestOption
	if conf.Contains("auth", "mount_path") {
		authMountPath, err := conf.FieldString("auth", "mount_path")
		if err != nil {
			return nil, err
		}
		authOptions = append(authOptions, vault.WithMountPath(authMountPath))
	}

	ctx := context.Background()
	resp, err := client.Auth.AppRoleLogin(ctx, schema.AppRoleLoginRequest{
		RoleId:   roleID,
		SecretId: secretID,
	}, authOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to login via Vault client: %w", err)
	}
	if resp == nil {
		return nil, errLoginResponseEmpty
	}

	var mountPath *service.InterpolatedString
	if conf.Contains("mount_path") {
		mountPath, err = conf.FieldInterpolatedString("mount_path")
		if err != nil {
			return nil, err
		}
	}

	var path *bloblang.Executor
	path, err = conf.FieldBloblang("path")
	if err != nil {
		return nil, fmt.Errorf("missing key path for Vault fetch: %w", err)
	}

	var version *service.InterpolatedString
	if conf.Contains("version") {
		version, err = conf.FieldInterpolatedString("version")
		if err != nil {
			return nil, err
		}
	}

	if resp.Auth == nil {
		return nil, errLoginResponseMissingAuth
	}

	if resp.Auth.ClientToken == "" {
		return nil, errLoginResponseEmptyClientToken
	}

	clientToken := resp.Auth.ClientToken

	return &processor{
		client:      client,
		clientToken: clientToken,
		logger:      mgr.Logger(),
		metrics:     mgr.Metrics(),
		mountPath:   mountPath,
		path:        path,
		version:     version,
	}, nil
}

type processor struct {
	client      *vault.Client
	clientToken string
	logger      *service.Logger
	metrics     *service.Metrics
	mountPath   *service.InterpolatedString
	path        *bloblang.Executor
	version     *service.InterpolatedString
}

func (p *processor) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {

	opts := []vault.RequestOption{
		vault.WithToken(p.clientToken),
	}

	mountPath := ""
	if p.mountPath != nil {
		var err error
		mountPath, err = p.mountPath.TryString(message)
		if err != nil {
			return nil, err
		}
		if mountPath != "" {
			opts = append(opts, vault.WithMountPath(mountPath))
		}
	}

	output, err := p.path.Query(message)
	if errors.Is(err, bloblang.ErrRootDeleted) {
		// Take this as an indicator to not produce a message
		return nil, nil
	}
	path := output.(string)
	if path == "" {
		return nil, errors.New("empty key path")
	}

	version := ""
	if p.version != nil {
		version, err := p.version.TryString(message)
		if err != nil {
			return nil, err
		}
		if version != "" {
			opts = append(opts, vault.WithQueryParameters(url.Values{
				"version": []string{version},
			}))
		}
	}

	p.logger.Tracef("Reading key value from Vault (mount_path: %s, path: %s, version: %s)", mountPath, path, version)
	kv, err := p.client.Secrets.KvV2Read(ctx, path, opts...)
	if err != nil {
		outMsg := message.Copy()
		outMsg.SetError(err)
		return service.MessageBatch{outMsg}, nil
	}

	bs, err := json.Marshal(kv.Data.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Vault response: %w", err)
	}

	outMsg := message.Copy()
	outMsg.SetBytes(bs)
	for k, v := range kv.Data.Metadata {
		outMsg.MetaSetMut(k, v)
	}

	return service.MessageBatch{
		outMsg,
	}, nil
}

func (p *processor) Close(ctx context.Context) error {
	return nil
}
