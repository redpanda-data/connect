// Copyright 2024 Redpanda Data, Inc.
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

package scylla

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	cFieldAddresses           = "addresses"
	cFieldTLS                 = "tls"
	cFieldPassAuth            = "password_authenticator"
	cFieldHostPolicy          = "host_policy"
	cFieldHostPolicyName      = "name"
	cFieldHostPolicyDCName    = "dc_name"
	cFieldHostPolicyRackName  = "rack_name"
	cFieldHostPolicyFallback  = "fallback_policy"
	cFieldPassAuthEnabled     = "enabled"
	cFieldPassAuthUsername    = "username"
	cFieldPassAuthPassword    = "password"
	cFieldDisableIHL          = "disable_initial_host_lookup"
	cFieldMaxRetries          = "max_retries"
	cFieldBackoff             = "backoff"
	cFieldConsistency         = "consistency"
	cFieldBackoffInitInterval = "initial_interval"
	cFieldBackoffMaxInterval  = "max_interval"
	cFieldTimeout             = "timeout"
)

func clientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField(cFieldAddresses).
			Description("A list of Cassandra nodes to connect to. Multiple comma separated addresses can be specified on a single line.").
			Examples(
				[]string{"localhost:9042"},
				[]string{"foo:9042", "bar:9042"},
				[]string{"foo:9042,bar:9042"},
			),
		service.NewTLSToggledField(cFieldTLS).Advanced(),
		service.NewObjectField(cFieldPassAuth,
			service.NewBoolField(cFieldPassAuthEnabled).
				Description("Whether to use password authentication").
				Default(false),
			service.NewStringField(cFieldPassAuthUsername).
				Description("The username to authenticate as.").
				Default(""),
			service.NewStringField(cFieldPassAuthPassword).
				Description("The password to authenticate with.").
				Secret().
				Default(""),
		).
			Description("Optional configuration of Cassandra authentication parameters.").
			Advanced(),
		service.NewBoolField(cFieldDisableIHL).
			Description("If enabled the driver will not attempt to get host info from the system.peers table. This can speed up queries but will mean that data_centre, rack and token information will not be available.").
			Advanced().
			Default(false),
		service.NewIntField(cFieldMaxRetries).
			Description("The maximum number of retries before giving up on a request.").
			Advanced().
			Default(3),
		service.NewObjectField(cFieldBackoff,
			service.NewDurationField(cFieldBackoffInitInterval).
				Description("The initial period to wait between retry attempts.").
				Default("1s"),
			service.NewDurationField(cFieldBackoffMaxInterval).
				Description("The maximum period to wait between retry attempts.").
				Default("5s"),
		).
			Description("Control time intervals between retry attempts.").
			Advanced(),
		service.NewDurationField(cFieldTimeout).
			Description("The client connection timeout.").
			Default("600ms"),
		service.NewObjectField(cFieldHostPolicy,
			service.NewStringField(cFieldHostPolicyName).
				Description("A host selection policy name.").
				Examples("round_robin", "rack_aware", "dc_aware", "token_aware").
				Default("round_robin"),
			service.NewStringField(cFieldHostPolicyDCName).
				Description("").
				Default("").
				Example("dc1"),
			service.NewStringField(cFieldPassAuthPassword).
				Description("The password to authenticate with.").
				Default("").
				Example("rack1"),
			service.NewObjectField(cFieldHostPolicyFallback,
				service.NewStringField(cFieldHostPolicyName).
					Description("A fallback host selection policy name.").
					Examples("round_robin", "rack_aware", "dc_aware").
					Default(""),
				service.NewStringField(cFieldHostPolicyDCName).
					Description("").
					Default("").
					Example("dc1"),
				service.NewStringField(cFieldPassAuthPassword).
					Description("The password to authenticate with.").
					Default("").
					Example("rack1"),
			).Advanced().Description("A fallback host selection policy configuration. Only needed for if main policy is token_aware."),
		).Advanced().Description("A host selection policy configuration."),
	}
}

type clientConf struct {
	addresses           []string
	tlsEnabled          bool
	tlsConf             *tls.Config
	authEnabled         bool
	authUsername        string
	authPassword        string
	disableIHL          bool
	maxRetries          int
	backoffInitInterval time.Duration
	backoffMaxInterval  time.Duration
	timeout             time.Duration
	consistencyLevel    gocql.Consistency
	hostPolicy          gocql.HostSelectionPolicy
}

func (c *clientConf) Create() (*gocql.ClusterConfig, error) {
	conn := gocql.NewCluster(c.addresses...)
	if c.tlsEnabled {
		conn.SslOpts = &gocql.SslOptions{
			Config: c.tlsConf,
		}
		conn.DisableInitialHostLookup = c.tlsConf.InsecureSkipVerify
	} else {
		conn.DisableInitialHostLookup = c.disableIHL
	}

	if c.authEnabled {
		conn.Authenticator = gocql.PasswordAuthenticator{
			Username: c.authUsername,
			Password: c.authPassword,
		}
	}

	conn.Consistency = c.consistencyLevel

	conn.RetryPolicy = &decorator{
		NumRetries: c.maxRetries,
		Min:        c.backoffInitInterval,
		Max:        c.backoffMaxInterval,
	}

	conn.PoolConfig.HostSelectionPolicy = c.hostPolicy

	conn.Timeout = c.timeout
	return conn, nil
}

func clientConfFromParsed(conf *service.ParsedConfig) (c clientConf, err error) {
	var tmpAddresses []string
	if tmpAddresses, err = conf.FieldStringList(cFieldAddresses); err != nil {
		return
	}
	for _, a := range tmpAddresses {
		c.addresses = append(c.addresses, strings.Split(a, ",")...)
	}

	if c.tlsConf, c.tlsEnabled, err = conf.FieldTLSToggled(cFieldTLS); err != nil {
		return
	}

	{
		authConf := conf.Namespace(cFieldPassAuth)
		c.authEnabled, _ = authConf.FieldBool(cFieldPassAuthEnabled)
		c.authUsername, _ = authConf.FieldString(cFieldPassAuthUsername)
		c.authPassword, _ = authConf.FieldString(cFieldPassAuthPassword)
	}

	if c.disableIHL, err = conf.FieldBool(cFieldDisableIHL); err != nil {
		return
	}
	if c.maxRetries, err = conf.FieldInt(cFieldMaxRetries); err != nil {
		return
	}
	if c.backoffInitInterval, err = conf.FieldDuration(cFieldBackoff, cFieldBackoffInitInterval); err != nil {
		return
	}
	if c.backoffMaxInterval, err = conf.FieldDuration(cFieldBackoff, cFieldBackoffMaxInterval); err != nil {
		return
	}
	var cl string
	cl, _ = conf.FieldString(cFieldConsistency)
	if cl != "" {
		if c.consistencyLevel, err = gocql.ParseConsistencyWrapper(cl); err != nil {
			return
		}
	}

	hostPolicyNS := conf.Namespace(cFieldHostPolicy)
	policyName, _ := hostPolicyNS.FieldString(cFieldHostPolicyName)
	dcName, _ := hostPolicyNS.FieldString(cFieldHostPolicyDCName)
	rackName, _ := hostPolicyNS.FieldString(cFieldHostPolicyRackName)

	fallbackNS := hostPolicyNS.Namespace(cFieldHostPolicyFallback)
	fallbackPolicyName, _ := fallbackNS.FieldString(cFieldHostPolicyName)
	fallbackDCName, _ := fallbackNS.FieldString(cFieldHostPolicyDCName)
	fallbackRackName, _ := fallbackNS.FieldString(cFieldHostPolicyRackName)

	var fallbackPolicy gocql.HostSelectionPolicy
	if fallbackPolicyName != "" {
		fallbackPolicy, err = getFallbackPolicy(fallbackPolicyName, fallbackDCName, fallbackRackName)
		if err != nil {
			return
		}
	}

	c.hostPolicy, err = getMainPolicy(policyName, dcName, rackName, fallbackPolicy)
	if err != nil {
		return
	}

	if c.timeout, err = conf.FieldDuration(cFieldTimeout); err != nil {
		return
	}
	return
}

func getMainPolicy(policyName string, dcName, rackName string, fallback gocql.HostSelectionPolicy) (gocql.HostSelectionPolicy, error) {
	switch policyName {
	case "dc_aware":
		if dcName == "" {
			return nil, fmt.Errorf("dc_aware policy requires a dc_name")
		}
		return gocql.DCAwareRoundRobinPolicy(dcName), nil
	case "rack_aware":
		if dcName == "" || rackName == "" {
			return nil, fmt.Errorf("dc_aware policy requires a dc_name and rack_name")
		}
		return gocql.RackAwareRoundRobinPolicy(dcName, rackName), nil
	case "round_robin", "":
		return gocql.RoundRobinHostPolicy(), nil
	case "token_aware":
		if fallback == nil {
			return nil, fmt.Errorf("token_aware policy requires a fallback_policy")
		}
		return gocql.TokenAwareHostPolicy(fallback), nil
	default:
		return nil, fmt.Errorf("unrecognized policy name: %v", policyName)
	}
}

func getFallbackPolicy(policyName, dcName, rackName string) (gocql.HostSelectionPolicy, error) {
	switch policyName {
	case "dc_aware":
		if dcName == "" {
			return nil, fmt.Errorf("dc_aware policy requires a dc_name")
		}
		return gocql.DCAwareRoundRobinPolicy(dcName), nil
	case "rack_aware":
		if dcName == "" || rackName == "" {
			return nil, fmt.Errorf("dc_aware policy requires a dc_name and rack_name")
		}
		return gocql.RackAwareRoundRobinPolicy(dcName, rackName), nil
	case "round_robin", "":
		return gocql.RoundRobinHostPolicy(), nil
	default:
		return nil, fmt.Errorf("unrecognized policy name: %v", policyName)
	}
}
