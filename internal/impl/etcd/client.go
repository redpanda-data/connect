package etcd

import (
	"context"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func getClient(parsedConf *service.ParsedConfig) (*clientv3.Client, error) {
	endpoints, err := parsedConf.FieldStringList("endpoints")
	if err != nil {
		return nil, err
	}

	var autoSyncInterval time.Duration
	if parsedConf.Contains("auto_sync_interval") {
		autoSyncIntervalTmp, err := parsedConf.FieldDuration("auto_sync_interval")
		if err != nil {
			return nil, err
		}
		autoSyncInterval = autoSyncIntervalTmp
	}

	var dialTimeout time.Duration
	if parsedConf.Contains("dialTimeout") {
		dialTimeoutTmp, err := parsedConf.FieldDuration("dial_timeout")
		if err != nil {
			return nil, err
		}
		dialTimeout = dialTimeoutTmp
	}

	var dialKeepAliveTime time.Duration
	if parsedConf.Contains("dialKeepAliveTime") {
		dialKeepAliveTimeTmp, err := parsedConf.FieldDuration("dial_keep_alive_time")
		if err != nil {
			return nil, err
		}
		dialKeepAliveTime = dialKeepAliveTimeTmp
	}

	var dialKeepAliveTimeout time.Duration
	if parsedConf.Contains("dialKeepAliveTimeout") {
		dialKeepAliveTimeoutTmp, err := parsedConf.FieldDuration("dial_keep_alive_timeout")
		if err != nil {
			return nil, err
		}
		dialKeepAliveTimeout = dialKeepAliveTimeoutTmp
	}
	maxCallSendMsgSize, err := parsedConf.FieldInt("max_call_send_msg_size")
	if err != nil {
		return nil, err
	}
	maxCallRecvMsgSize, err := parsedConf.FieldInt("max_call_recv_msg_size")
	if err != nil {
		return nil, err
	}
	tlsConf, tlsEnabled, err := parsedConf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if !tlsEnabled {
		tlsConf = nil
	}
	username, err := parsedConf.FieldString("username")
	if err != nil {
		return nil, err
	}
	password, err := parsedConf.FieldString("password")
	if err != nil {
		return nil, err
	}
	rejectOldCluster, err := parsedConf.FieldBool("reject_old_cluster")
	if err != nil {
		return nil, err
	}
	permitWithoutStream, err := parsedConf.FieldBool("permit_without_stream")
	if err != nil {
		return nil, err
	}
	maxUnaryRetries, err := parsedConf.FieldInt("max_unary_retries")
	if err != nil {
		return nil, err
	}

	var backOffWaitBetween time.Duration
	if parsedConf.Contains("backOffWaitBetween") {
		backOffWaitBetweenTmp, err := parsedConf.FieldDuration("back_off_wait_between")
		if err != nil {
			return nil, err
		}
		backOffWaitBetween = backOffWaitBetweenTmp
	}
	backOffJitterFraction, err := parsedConf.FieldFloat("back_off_jitter_fraction")
	if err != nil {
		return nil, err
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:             endpoints,
		AutoSyncInterval:      autoSyncInterval,
		DialTimeout:           dialTimeout,
		DialKeepAliveTime:     dialKeepAliveTime,
		DialKeepAliveTimeout:  dialKeepAliveTimeout,
		MaxCallSendMsgSize:    maxCallSendMsgSize,
		MaxCallRecvMsgSize:    maxCallRecvMsgSize,
		TLS:                   tlsConf,
		Username:              username,
		Password:              password,
		RejectOldCluster:      rejectOldCluster,
		Context:               context.Background(),
		Logger:                nil,
		LogConfig:             nil,
		PermitWithoutStream:   permitWithoutStream,
		MaxUnaryRetries:       uint(maxUnaryRetries),
		BackoffWaitBetween:    backOffWaitBetween,
		BackoffJitterFraction: backOffJitterFraction,
	})

	healthCheckKey, err := parsedConf.FieldString("health_check_key")
	if err != nil {
		return nil, err
	}
	// Is alive?
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cli.KV.Get(ctx, healthCheckKey)
	cancel()
	if err != nil {
		return nil, err
	}

	return cli, err
}
func clientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("endpoints").
			Description("Endpoints is a list of urls to connect to.").
			Default([]string{"localhost:2379"}),

		service.NewDurationField("auto_sync_interval").
			Description("This is the interval to update endpoints with its latest members. 0 disables auto-sync. By default auto-sync is disabled.").
			Advanced().
			Example("10s").
			Optional(),
		service.NewDurationField("dial_timeout").
			Description("This is the timeout for failing to establish a connection.").
			Advanced().
			Example("500ms").
			Optional(),
		service.NewDurationField("dial_keep_alive_time").
			Description("This is the time after which client pings the server to see if transport is alive").
			Advanced().
			Example("5s").
			Optional(),
		service.NewDurationField("dial_keep_alive_timeout").
			Description("This is the time that the client waits for a response for the keep-alive probe. If the response is not received in this time, the connection is closed.").
			Advanced().
			Example("5s").
			Optional(),
		service.NewIntField("max_call_send_msg_size").
			Description("This is the client-side request send limit in bytes. If 0, it defaults to 2.0 MiB (2 * 1024 * 1024). Make sure that 'MaxCallSendMsgSize' < server-side default send/recv limit.").
			Default(0).
			Advanced().
			Optional(),
		service.NewIntField("max_call_recv_msg_size").
			Description("MaxCallRecvMsgSize is the client-side response receive limit. If 0, it defaults to 'math.MaxInt32', because range response can easily exceed request send limits. Make sure that 'MaxCallRecvMsgSize' >= server-side default send/recv limit.").
			Default(0).
			Advanced().
			Optional(),
		service.NewTLSToggledField("tls").
			Description("Custom TLS settings can be used to override system defaults.").Optional().Advanced(),
		service.NewStringField("username").
			Description("Username is a user name for authentication.").
			Default("").
			Advanced().
			Optional(),
		service.NewStringField("password").
			Description("Password is a password name for authentication.").
			Default("").
			Optional().
			Advanced().
			Secret(),
		service.NewBoolField("reject_old_cluster").
			Description("RejectOldCluster when set will refuse to create a client against an outdated cluster.").
			Default(false).
			Advanced().
			Optional(),
		service.NewBoolField("permit_without_stream").
			Description("PermitWithoutStream when set will allow client to send keepalive pings to server without any active streams(RPCs).").
			Default(false).
			Advanced().
			Optional(),
		service.NewIntField("max_unary_retries").
			Description("MaxUnaryRetries is the maximum number of retries for unary RPCs.").
			Default(0).
			Advanced().
			Optional(),
		service.NewDurationField("back_off_wait_between").
			Description("BackoffWaitBetween is the wait time before retrying an RPC.").
			Advanced().
			Optional(),
		service.NewFloatField("back_off_jitter_fraction").
			Description("BackoffJitterFraction is the jitter fraction to randomize backoff wait time.").
			Default(0).
			Advanced().
			Optional(),
		service.NewStringField("health_check_key").
			Description("This key will be accessed subsequent to the establishment of the etcd client to verify the functionality of the connection. If strict access roles are in place, you may specify a predetermined key that has read permissions.").
			Default("ok").
			Advanced().
			Example("im-sure-this-key-is-readable").
			Optional(),
	}
}
