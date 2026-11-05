// Copyright 2026 Redpanda Data, Inc.
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

// Package aerospike implements an Aerospike output.
package aerospike

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	as "github.com/aerospike/aerospike-client-go/v8"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Configuration field names shared by every Aerospike component.
const (
	aoFieldHosts                   = "hosts"
	aoFieldClusterName             = "cluster_name"
	aoFieldUseServicesAlternate    = "use_services_alternate"
	aoFieldConnectTimeout          = "connect_timeout"
	aoFieldMaxConnsPerNode         = "max_connections_per_node"
	aoFieldMinConnsPerNode         = "min_connections_per_node"
	aoFieldWarmUp                  = "warm_up"
	aoFieldMaxErrorRate            = "max_error_rate"
	aoFieldErrorRateWindow         = "error_rate_window"
	aoFieldCredentials             = "credentials"
	aoFieldCredentialsUsername     = "username"
	aoFieldCredentialsPassword     = "password"
	aoFieldCredentialsPasswordFile = "password_file"
	aoFieldTLS                     = "tls"
	aoFieldCiphers                 = "ciphers"
	aoFieldProtocols               = "protocols"
	aoFieldRevokeCertificates      = "revoke_certificates"
	aoFieldAuthMode                = "auth_mode"

	// defaultServicePort is the Aerospike client service port.
	defaultServicePort = 3000
)

// clientFields returns the connection configuration fields common to every
// Aerospike component.
func clientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField(aoFieldHosts).
			Description("Seed nodes as `host`, `host:port`, `host:tlsname:port`, or `[ipv6]:tlsname:port`. Port defaults to 3000. The tlsname is the certificate name used for TLS verification and is required against a conventionally secured cluster. The client discovers and connects directly to every node, so all nodes must be reachable from this process — a load balancer in front of the seeds is not sufficient.").
			Example([]string{"localhost:3000"}).
			Example([]string{"as1.internal:clusterA:3000"}).
			Example([]string{"as1.internal:3000", "as2.internal:3000"}),

		service.NewStringField(aoFieldClusterName).
			Description("Expected cluster name. When set, the client refuses to talk to nodes reporting a different name.").
			Default("").
			Advanced(),

		service.NewBoolField(aoFieldUseServicesAlternate).
			Description("Use the nodes' `alternate-access-address` instead of `access-address`. Required when this process reaches Aerospike over a different network than the one the nodes advertise, which is the usual case for Kubernetes and NAT deployments.").
			Default(false).
			Advanced(),

		service.NewDurationField(aoFieldConnectTimeout).
			Description("Timeout for establishing the initial cluster connection. Capped by the remaining pipeline context deadline when one is set, so a shutdown mid-connect does not wait the full duration.").
			Default("30s").
			Advanced(),

		service.NewIntField(aoFieldMaxConnsPerNode).
			Description("Maximum size of the client connection pool per cluster node. The client opens connections to every node, so budget `max_connections_per_node` multiplied by the number of processes against the server's `proto-fd-max`.").
			Default(100).
			LintRule(positiveLint).
			Advanced(),

		service.NewIntField(aoFieldMinConnsPerNode).
			Description("Number of connections per node to keep open even while idle, so a burst after a quiet period does not pay reconnect cost. This is also how many connections `warm_up` opens. `0` sizes the pool automatically from the concurrency the component can reach, which is what keeps commands from failing while the pool is still filling. Set it explicitly only to override that: every connection is a file descriptor on the node, and with TLS a large pool makes startup and reconnect expensive in server CPU.").
			Default(0).
			LintRule(nonNegativeLint).
			Advanced(),

		service.NewBoolField(aoFieldWarmUp).
			Description("Open `min_connections_per_node` connections on startup rather than lazily, which avoids a latency spike and possible timeouts on the first commands. Disabling this leaves the pool to fill on demand, which with `max_retries` at `0` fails the commands that find it empty.").
			Default(true).
			Advanced(),

		service.NewIntField(aoFieldMaxErrorRate).
			Description("Errors permitted from a single node per `error_rate_window` before the client stops sending it commands and fails fast instead. This is a circuit breaker: it stops a pipeline from hammering a node that is already failing. `0` disables it.").
			Default(100).
			LintRule(nonNegativeLint).
			Advanced(),

		service.NewIntField(aoFieldErrorRateWindow).
			Description("Number of cluster tend iterations that make up the window for `max_error_rate`.").
			Default(1).
			LintRule(nonNegativeLint).
			Advanced(),

		service.NewObjectField(aoFieldCredentials,
			service.NewStringField(aoFieldCredentialsUsername).
				Description("Username for Aerospike access control.").
				Default(""),
			service.NewStringField(aoFieldCredentialsPassword).
				Description("Password for Aerospike access control. Set this or `password_file`, not both.").
				Default("").
				Secret(),
			service.NewStringField(aoFieldCredentialsPasswordFile).
				Description("File whose first line is the Aerospike password. The file is read once at startup, so a rotated password is picked up on the next restart. A trailing newline is not part of the password. Trailing spaces on the first line are kept. An empty first line fails startup. Set this or `password`, not both. A username with neither also fails startup. `pki` ignores this field. A missing or unreadable file fails startup. The file contents are not logged.").
				Default("").
				Example("/var/secrets/aerospike-password").
				Advanced(),
		).Description("Credentials for clusters with access control enabled. Aerospike Community Edition has no authentication; leave empty there.").
			Optional().
			Advanced(),

		service.NewStringAnnotatedEnumField(aoFieldAuthMode, map[string]string{
			"internal": "Hashed password stored on the server. Default.",
			"external": "External authentication such as LDAP. Requires TLS.",
			"pki":      "Certificate authentication. Requires TLS and a client certificate; do not set a password.",
		}).
			Description("Authentication mode. `internal` is password auth against the server. `external` is LDAP. `pki` is client-certificate auth and does not use a password.").
			Default("internal").
			Advanced(),

		service.NewTLSToggledField(aoFieldTLS),

		service.NewStringListField(aoFieldCiphers).
			Description("TLS 1.2 cipher suites to offer, as IANA names such as `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`. Empty leaves the suites selected by `tls`. Names are matched case-insensitively, and a name Go does not implement is rejected at startup. A suite Go marks insecure is accepted when it is named, so the list can match a server that still requires it, and startup logs a warning for each one. TLS 1.3 suites cannot be named here. They stay offered unless `protocols` is `TLSv1.2`. Naming `ciphers` when `protocols` offers TLS 1.3 only fails startup. Requires `tls.enabled`.").
			Default([]string{}).
			Example([]string{"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"}).
			Advanced(),

		service.NewStringListField(aoFieldProtocols).
			Description("TLS versions to offer. `TLSv1.2` does not offer TLS 1.3. `TLSv1.3` does not offer TLS 1.2. Empty, or both names, leaves the versions selected by `tls`: TLS 1.2 and TLS 1.3. Names are matched case-insensitively. `ciphers` fails startup when this list offers TLS 1.3 only. Requires `tls.enabled`.").
			Default([]string{}).
			Example([]string{"TLSv1.2"}).
			Advanced(),

		service.NewStringListField(aoFieldRevokeCertificates).
			Description("Certificate serial numbers to reject. A value is a decimal integer, or a hex integer with a `0x` prefix. `openssl x509 -noout -serial` prints hex, so convert that to decimal or add the `0x` prefix. A hex value without the prefix is read as decimal when it contains only digits. A serial is unique only for its issuing CA, and the same number on any other certificate in the verified chain, including a root or intermediate, is also rejected. With `tls.skip_cert_verify`, only certificates the server sends are checked, so a root the server does not send is not seen. An empty list leaves the CA check as the only certificate check. A serial may be larger than a 64-bit integer. Requires `tls.enabled`.").
			Default([]string{}).
			Example([]string{"123123", "0x1E0F3"}).
			Advanced(),
	}
}

// clientConfig is the parsed connection configuration.
type clientConfig struct {
	Hosts               []*as.Host
	Policy              *as.ClientPolicy
	WarmUp              bool
	insecureCiphers     []string
	ignoredPasswordFile bool
}

// sizePoolForConcurrency gives the connection pool a floor matching the number
// of commands that can be in flight at once, unless the user picked a floor
// themselves.
//
// An empty pool is not a soft condition in the Aerospike client: rather than
// blocking, it starts a connection in the background and fails the command with
// NO_AVAILABLE_CONNECTIONS_TO_NODE, expecting a retry to find the new
// connection. Writes default to max_retries: 0 because create_only and counters
// cannot be replayed, so there is no retry to absorb that and every command
// arriving before the pool has filled is lost. Sizing the floor to the
// concurrency keeps the pool from being empty in the first place.
func (c *clientConfig) sizePoolForConcurrency(concurrency int) {
	if c.Policy.MinConnectionsPerNode > 0 || concurrency <= 0 {
		return
	}
	c.Policy.MinConnectionsPerNode = min(concurrency, c.Policy.ConnectionQueueSize)
}

// parseClientConfig reads the fields produced by clientFields.
func parseClientConfig(conf *service.ParsedConfig) (*clientConfig, error) {
	hostStrs, err := conf.FieldStringList(aoFieldHosts)
	if err != nil {
		return nil, err
	}
	if len(hostStrs) == 0 {
		return nil, fmt.Errorf("field '%v' must contain at least one seed node", aoFieldHosts)
	}

	c := &clientConfig{Policy: as.NewClientPolicy()}
	for _, h := range hostStrs {
		host, err := parseHost(h)
		if err != nil {
			return nil, err
		}
		c.Hosts = append(c.Hosts, host)
	}

	if c.Policy.ClusterName, err = conf.FieldString(aoFieldClusterName); err != nil {
		return nil, err
	}
	if c.Policy.UseServicesAlternate, err = conf.FieldBool(aoFieldUseServicesAlternate); err != nil {
		return nil, err
	}
	if c.Policy.Timeout, err = conf.FieldDuration(aoFieldConnectTimeout); err != nil {
		return nil, err
	}
	if c.Policy.ConnectionQueueSize, err = conf.FieldInt(aoFieldMaxConnsPerNode); err != nil {
		return nil, err
	}
	if c.Policy.ConnectionQueueSize < 1 {
		return nil, fmt.Errorf("field '%v' must be at least 1", aoFieldMaxConnsPerNode)
	}
	if c.Policy.MinConnectionsPerNode, err = conf.FieldInt(aoFieldMinConnsPerNode); err != nil {
		return nil, err
	}
	if c.Policy.MinConnectionsPerNode < 0 {
		return nil, fmt.Errorf("field '%v' must not be negative", aoFieldMinConnsPerNode)
	}
	if c.Policy.MinConnectionsPerNode > c.Policy.ConnectionQueueSize {
		return nil, fmt.Errorf("field '%v' (%d) must not exceed '%v' (%d)",
			aoFieldMinConnsPerNode, c.Policy.MinConnectionsPerNode,
			aoFieldMaxConnsPerNode, c.Policy.ConnectionQueueSize)
	}
	if c.WarmUp, err = conf.FieldBool(aoFieldWarmUp); err != nil {
		return nil, err
	}
	if c.Policy.MaxErrorRate, err = conf.FieldInt(aoFieldMaxErrorRate); err != nil {
		return nil, err
	}
	if c.Policy.MaxErrorRate < 0 {
		return nil, fmt.Errorf("field '%v' must not be negative", aoFieldMaxErrorRate)
	}
	if c.Policy.ErrorRateWindow, err = conf.FieldInt(aoFieldErrorRateWindow); err != nil {
		return nil, err
	}
	if c.Policy.ErrorRateWindow < 0 {
		return nil, fmt.Errorf("field '%v' must not be negative", aoFieldErrorRateWindow)
	}

	var passwordFile string
	if conf.Contains(aoFieldCredentials) {
		cc := conf.Namespace(aoFieldCredentials)
		if c.Policy.User, err = cc.FieldString(aoFieldCredentialsUsername); err != nil {
			return nil, err
		}
		if c.Policy.Password, err = cc.FieldString(aoFieldCredentialsPassword); err != nil {
			return nil, err
		}
		if passwordFile, err = cc.FieldString(aoFieldCredentialsPasswordFile); err != nil {
			return nil, err
		}
		passwordFile = strings.TrimSpace(passwordFile)
	}

	mode, err := conf.FieldString(aoFieldAuthMode)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "internal":
		c.Policy.AuthMode = as.AuthModeInternal
	case "external":
		c.Policy.AuthMode = as.AuthModeExternal
	case "pki":
		c.Policy.AuthMode = as.AuthModePKI
	default:
		return nil, fmt.Errorf("field '%v': unknown auth mode %q", aoFieldAuthMode, mode)
	}
	ignoredPasswordFile, err := applyPasswordFile(c.Policy, passwordFile)
	if err != nil {
		return nil, err
	}
	c.ignoredPasswordFile = ignoredPasswordFile

	var tlsConf *tls.Config
	var tlsEnabled bool
	if tlsConf, tlsEnabled, err = conf.FieldTLSToggled(aoFieldTLS); err != nil {
		return nil, err
	}
	cipherNames, err := conf.FieldStringList(aoFieldCiphers)
	if err != nil {
		return nil, err
	}
	if len(cipherNames) > 0 && !tlsEnabled {
		return nil, fmt.Errorf("field '%s' requires '%s.enabled'", aoFieldCiphers, aoFieldTLS)
	}
	protocolNames, err := conf.FieldStringList(aoFieldProtocols)
	if err != nil {
		return nil, err
	}
	if len(protocolNames) > 0 && !tlsEnabled {
		return nil, fmt.Errorf("field '%s' requires '%s.enabled'", aoFieldProtocols, aoFieldTLS)
	}
	revokedNames, err := conf.FieldStringList(aoFieldRevokeCertificates)
	if err != nil {
		return nil, err
	}
	if len(revokedNames) > 0 && !tlsEnabled {
		return nil, fmt.Errorf("field '%s' requires '%s.enabled'", aoFieldRevokeCertificates, aoFieldTLS)
	}
	if tlsEnabled {
		if err := applyTLSProtocols(tlsConf, protocolNames); err != nil {
			return nil, err
		}
		if len(cipherNames) > 0 && tlsConf.MinVersion >= tls.VersionTLS13 {
			return nil, fmt.Errorf("field '%s': TLS 1.2 cipher suites have no effect when '%s' offers TLS 1.3 only", aoFieldCiphers, aoFieldProtocols)
		}
		suites, err := cipherSuiteIDs(cipherNames)
		if err != nil {
			return nil, err
		}
		if len(suites) > 0 {
			tlsConf.CipherSuites = suites
			c.insecureCiphers = insecureCipherNames(suites)
		}
		if err := applyRevokedCertificates(tlsConf, revokedNames); err != nil {
			return nil, err
		}
		c.Policy.TlsConfig = tlsConf
	}

	return c, nil
}

// parseHost accepts Aerospike seed addresses:
//
//	host
//	host:port
//	host:tlsname
//	host:tlsname:port
//
// IPv6 literals must be bracketed when a port or tlsname is present (`[::1]:3000`,
// `[2001:db8::1]:clusterA:3000`). A bare IPv6 address (`::1`) is a host with the
// default port. The tlsname is the certificate name used for TLS verification.
func parseHost(s string) (*as.Host, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty host entry in '%v'", aoFieldHosts)
	}

	name, rest, err := splitHostName(s)
	if err != nil {
		return nil, err
	}

	tlsName, port, err := parseHostSuffix(s, rest)
	if err != nil {
		return nil, err
	}

	h := as.NewHost(name, port)
	h.TLSName = tlsName
	return h, nil
}

func splitHostName(s string) (name, rest string, err error) {
	if strings.HasPrefix(s, "[") {
		end := strings.IndexByte(s, ']')
		if end < 0 {
			return "", "", fmt.Errorf("invalid host %q: unclosed IPv6 bracket", s)
		}
		name = s[1:end]
		if net.ParseIP(name) == nil {
			return "", "", fmt.Errorf("invalid host %q: %q is not an IP address", s, name)
		}
		return name, s[end+1:], nil
	}
	if ip := net.ParseIP(s); ip != nil {
		return s, "", nil
	}
	host, after, found := strings.Cut(s, ":")
	if !found {
		return s, "", nil
	}
	if host == "" {
		return "", "", fmt.Errorf("invalid host %q: missing hostname", s)
	}
	return host, ":" + after, nil
}

// parseHostSuffix reads the optional [:tlsname][:port] tail. A component that
// parses as an integer is a port; otherwise it is the TLS certificate name.
func parseHostSuffix(orig, rest string) (tlsName string, port int, err error) {
	port = defaultServicePort
	if rest == "" {
		return "", port, nil
	}
	if !strings.HasPrefix(rest, ":") {
		return "", 0, fmt.Errorf("invalid host %q: unexpected trailing %q", orig, rest)
	}
	rest = rest[1:]
	if rest == "" {
		return "", 0, fmt.Errorf("invalid host %q: trailing colon", orig)
	}

	first, second, hasSecond := strings.Cut(rest, ":")
	if first == "" {
		return "", 0, fmt.Errorf("invalid host %q", orig)
	}

	if !hasSecond {
		if p, ok, err := parsePort(orig, first); err != nil {
			return "", 0, err
		} else if ok {
			return "", p, nil
		}
		return first, port, nil
	}

	if _, isPort, err := parsePort(orig, first); err != nil {
		return "", 0, err
	} else if isPort {
		return "", 0, fmt.Errorf("invalid host %q: expected host:tlsname:port, not host:port:tlsname", orig)
	}
	if strings.Contains(second, ":") {
		return "", 0, fmt.Errorf("invalid host %q: expected host:tlsname:port", orig)
	}
	p, ok, err := parsePort(orig, second)
	if err != nil {
		return "", 0, err
	}
	if !ok {
		return "", 0, fmt.Errorf("invalid port in host %q: %q", orig, second)
	}
	return first, p, nil
}

// parsePort reports whether s is a port number, and its value. Only a wholly
// numeric component is a port: a TLS certificate name may legitimately begin
// with a digit, so treating any digit-led component as a port would make names
// like `1cluster` unconfigurable.
func parsePort(orig, s string) (int, bool, error) {
	if s == "" {
		return 0, false, nil
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return 0, false, nil
		}
	}
	p, err := strconv.Atoi(s)
	if err != nil {
		return 0, false, fmt.Errorf("invalid port in host %q: %w", orig, err)
	}
	if p < 1 || p > 65535 {
		return 0, false, fmt.Errorf("invalid port in host %q: %d is out of range", orig, p)
	}
	return p, true, nil
}

// connection owns a single Aerospike client for the lifetime of a component.
//
// The client is thread safe and holds the cluster state and connection pools,
// so one per component is correct; creating one per request would exhaust
// ports and add a cluster tend to every operation.
type connection struct {
	conf *clientConfig
	log  *service.Logger

	mu     sync.RWMutex
	client *as.Client

	warnedIgnoredPassword bool
}

// newConnection returns an unconnected handle. Call connect before issuing commands.
func newConnection(conf *clientConfig, log *service.Logger) *connection {
	return &connection{conf: conf, log: log}
}

// connect establishes the cluster client, or reuses it if it is already live.
func (c *connection) connect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil && c.client.IsConnected() {
		return nil
	}
	if c.client != nil {
		c.client.Close()
		c.client = nil
	}
	if c.conf.ignoredPasswordFile && c.log != nil && !c.warnedIgnoredPassword {
		c.log.Warnf("field '%s.%s' is ignored when '%s' is pki", aoFieldCredentials, aoFieldCredentialsPasswordFile, aoFieldAuthMode)
		c.warnedIgnoredPassword = true
	}

	policy := c.conf.Policy
	if policy == nil {
		policy = as.NewClientPolicy()
	}
	p := *policy
	timeout, err := limitDuration(ctx, p.Timeout)
	if err != nil {
		return err
	}
	p.Timeout = timeout

	type outcome struct {
		client *as.Client
		err    error
	}
	ch := make(chan outcome, 1)
	go func() {
		client, err := as.NewClientWithPolicyAndHost(&p, c.conf.Hosts...)
		if err != nil {
			ch <- outcome{err: err}
			return
		}
		// WarmUp(-1) fills the pool to its maximum, which for the default
		// max_connections_per_node opens 100 connections to every node at
		// once. Warm up to the configured minimum instead, so the size of the
		// startup connection burst is something the user chose.
		if c.conf.WarmUp && p.MinConnectionsPerNode > 0 {
			if _, werr := client.WarmUp(p.MinConnectionsPerNode); werr != nil && c.log != nil {
				c.log.Warnf("Connection pool warm up did not complete: %v", werr)
			}
		}
		ch <- outcome{client: client}
	}()

	select {
	case <-ctx.Done():
		go func() {
			o := <-ch
			if o.client != nil {
				o.client.Close()
			}
		}()
		return fmt.Errorf("connecting to aerospike: %w", ctx.Err())
	case o := <-ch:
		if o.err != nil {
			return fmt.Errorf("connecting to aerospike cluster: %w", o.err)
		}
		c.client = o.client
		if c.log != nil {
			c.log.Infof("Connected to Aerospike cluster with %v node(s)", len(o.client.GetNodes()))
		}
		return nil
	}
}

// close releases the cluster client. A cancelled ctx may return before the
// client is fully shut down; it is still closed in the background.
func (c *connection) close(ctx context.Context) error {
	c.mu.Lock()
	client := c.client
	c.client = nil
	c.mu.Unlock()
	if client == nil {
		return nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Close()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// asClient returns the live client, or nil when the component is not connected.
func (c *connection) asClient() *as.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.client == nil || !c.client.IsConnected() {
		return nil
	}
	return c.client
}
