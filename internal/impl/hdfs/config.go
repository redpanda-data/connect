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

package hdfs

import (
	"errors"
	"fmt"
	"strings"

	"github.com/colinmarc/hdfs/v2"
	krbclient "github.com/jcmturner/gokrb5/v8/client"
	krbconfig "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldHosts = "hosts"
	fieldUser  = "user"

	fieldAuth                            = "auth"
	fieldKerberos                        = "kerberos"
	fieldKerberosEnabled                 = "enabled"
	fieldKerberosKrb5Conf                = "krb5_conf"
	fieldKerberosKeytab                  = "keytab"
	fieldKerberosPrincipal               = "principal"
	fieldKerberosServicePrincipal        = "service_principal"
	fieldKerberosDataTransferProtection  = "data_transfer_protection"
	defaultKerberosServicePrincipal      = "nn/_HOST"
	defaultKerberosKrb5Conf              = "/etc/krb5.conf"
	dataTransferProtectionAuthentication = hdfs.DataTransferProtectionAuthentication
	dataTransferProtectionIntegrity      = hdfs.DataTransferProtectionIntegrity
	dataTransferProtectionPrivacy        = hdfs.DataTransferProtectionPrivacy
)

type hdfsConfig struct {
	hosts    []string
	user     string
	kerberos hdfsKerberosConfig
}

type hdfsKerberosConfig struct {
	enabled                bool
	krb5Conf               string
	keytab                 string
	principal              string
	servicePrincipal       string
	dataTransferProtection string
}

func hdfsCommonFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField(fieldHosts).
			Description("A list of target host addresses to connect to.").
			Example("localhost:9000"),
		service.NewStringField(fieldUser).
			Description("A user ID to connect as.").
			Default(""),
	}
}

func hdfsAuthField() *service.ConfigField {
	return hdfsAuthFieldWithDataTransferProtectionDescription(`The HDFS data transfer protection level. Must match the target HDFS cluster configuration when set. Valid values are "authentication", "integrity", "privacy", or empty.`)
}

func hdfsInputAuthField() *service.ConfigField {
	return hdfsAuthFieldWithDataTransferProtectionDescription(`The HDFS data transfer protection level. HDFS input reads currently support "authentication" or empty. The "integrity" and "privacy" modes are not supported for input reads.`)
}

func hdfsAuthFieldWithDataTransferProtectionDescription(dataTransferProtectionDescription string) *service.ConfigField {
	return service.NewObjectField(fieldAuth,
		service.NewObjectField(fieldKerberos,
			service.NewBoolField(fieldKerberosEnabled).
				Description("Whether to use Kerberos authentication.").
				Default(false),
			service.NewStringField(fieldKerberosKrb5Conf).
				Description("The path to a krb5.conf file.").
				Default(defaultKerberosKrb5Conf),
			service.NewStringField(fieldKerberosKeytab).
				Description("The path to a Kerberos keytab file. The Connect process must be able to read this file.").
				Default(""),
			service.NewStringField(fieldKerberosPrincipal).
				Description("The Kerberos principal to authenticate as, in the form username@REALM. Service principals may include a slash in the username, such as connect/worker@EXAMPLE.COM.").
				Default(""),
			service.NewStringField(fieldKerberosServicePrincipal).
				Description("The Kerberos service principal name for HDFS namenodes. The special string _HOST can be used for hostname substitution.").
				Default(defaultKerberosServicePrincipal),
			service.NewStringField(fieldKerberosDataTransferProtection).
				Description(dataTransferProtectionDescription).
				Default(""),
		).Description("Kerberos authentication settings.").Optional(),
	).Description("Optional HDFS authentication settings.").Advanced().Optional()
}

func hdfsConfigFromParsed(conf *service.ParsedConfig) (c hdfsConfig, err error) {
	if c.hosts, err = conf.FieldStringList(fieldHosts); err != nil {
		return
	}
	if c.user, err = conf.FieldString(fieldUser); err != nil {
		return
	}
	if c.kerberos, err = hdfsKerberosConfigFromParsed(conf); err != nil {
		return
	}
	err = c.kerberos.validate()
	return
}

func hdfsInputConfigFromParsed(conf *service.ParsedConfig) (c hdfsConfig, err error) {
	if c, err = hdfsConfigFromParsed(conf); err != nil {
		return
	}
	err = c.validateInput()
	return
}

func hdfsKerberosConfigFromParsed(conf *service.ParsedConfig) (c hdfsKerberosConfig, err error) {
	if !conf.Contains(fieldAuth) {
		return
	}
	authConf := conf.Namespace(fieldAuth)
	if !authConf.Contains(fieldKerberos) {
		return
	}
	kerberosConf := authConf.Namespace(fieldKerberos)
	if c.enabled, err = kerberosConf.FieldBool(fieldKerberosEnabled); err != nil {
		return
	}
	if c.krb5Conf, err = kerberosConf.FieldString(fieldKerberosKrb5Conf); err != nil {
		return
	}
	if c.keytab, err = kerberosConf.FieldString(fieldKerberosKeytab); err != nil {
		return
	}
	if c.principal, err = kerberosConf.FieldString(fieldKerberosPrincipal); err != nil {
		return
	}
	if c.servicePrincipal, err = kerberosConf.FieldString(fieldKerberosServicePrincipal); err != nil {
		return
	}
	if c.dataTransferProtection, err = kerberosConf.FieldString(fieldKerberosDataTransferProtection); err != nil {
		return
	}
	return
}

func (c hdfsKerberosConfig) validate() error {
	if !c.enabled {
		return nil
	}
	if c.keytab == "" {
		return errors.New("auth.kerberos.keytab is required when Kerberos is enabled")
	}
	if c.principal == "" {
		return errors.New("auth.kerberos.principal is required when Kerberos is enabled")
	}
	if _, _, err := splitKerberosPrincipal(c.principal); err != nil {
		return fmt.Errorf("auth.kerberos.principal is invalid: %w", err)
	}
	switch c.dataTransferProtection {
	case "", dataTransferProtectionAuthentication, dataTransferProtectionIntegrity, dataTransferProtectionPrivacy:
		return nil
	default:
		return fmt.Errorf("auth.kerberos.data_transfer_protection must be one of %q, %q, %q, or empty", dataTransferProtectionAuthentication, dataTransferProtectionIntegrity, dataTransferProtectionPrivacy)
	}
}

func (c hdfsConfig) validateInput() error {
	if !c.kerberos.enabled {
		return nil
	}
	switch c.kerberos.dataTransferProtection {
	case "", dataTransferProtectionAuthentication:
		return nil
	case dataTransferProtectionIntegrity, dataTransferProtectionPrivacy:
		return fmt.Errorf("auth.kerberos.data_transfer_protection %q is not supported for the hdfs input; use %q or leave it empty", c.kerberos.dataTransferProtection, dataTransferProtectionAuthentication)
	default:
		return c.kerberos.validate()
	}
}

func (c hdfsConfig) clientOptions() (hdfs.ClientOptions, error) {
	opts := hdfs.ClientOptions{
		Addresses: c.hosts,
		User:      c.user,
	}
	if !c.kerberos.enabled {
		return opts, nil
	}

	krbClient, err := c.kerberos.client()
	if err != nil {
		return opts, err
	}
	opts.KerberosClient = krbClient
	opts.KerberosServicePrincipleName = c.kerberos.servicePrincipal
	opts.DataTransferProtection = c.kerberos.dataTransferProtection
	return opts, nil
}

func (c hdfsKerberosConfig) client() (*krbclient.Client, error) {
	krbConf, err := krbconfig.Load(c.krb5Conf)
	if err != nil {
		return nil, fmt.Errorf("loading krb5.conf: %w", err)
	}
	kt, err := keytab.Load(c.keytab)
	if err != nil {
		return nil, fmt.Errorf("loading keytab: %w", err)
	}
	username, realm, err := splitKerberosPrincipal(c.principal)
	if err != nil {
		return nil, err
	}
	client := krbclient.NewWithKeytab(username, realm, kt, krbConf)
	if err := client.Login(); err != nil {
		return nil, fmt.Errorf("kerberos login: %w", err)
	}
	return client, nil
}

func splitKerberosPrincipal(principal string) (string, string, error) {
	username, realm, ok := strings.Cut(principal, "@")
	if !ok || username == "" || realm == "" {
		return "", "", errors.New("expected format username@REALM")
	}
	return username, realm, nil
}
