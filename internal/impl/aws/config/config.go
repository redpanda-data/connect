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

package config

import (
	"slices"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/utils/netutil"
)

// sessionFieldNames are the names of the fields that SessionFields returns, in
// order.
var sessionFieldNames = []string{"region", "endpoint", "tcp", "credentials"}

// SessionFields defines a re-usable set of config fields for an AWS session
// that is compatible with the public service APIs and avoids importing the full
// AWS dependencies.
func SessionFields() []*service.ConfigField {
	return SessionFieldsWithVersions(nil)
}

// SessionFieldsWithVersions is SessionFields for a component that gained some
// of the session fields after its first release. versions maps a field name
// (region, endpoint, tcp or credentials) to the release it first shipped in
// for that component. It panics on any other name.
func SessionFieldsWithVersions(versions map[string]string) []*service.ConfigField {
	fields := sessionFields()
	if len(fields) != len(sessionFieldNames) {
		panic("aws session field names are out of step with the fields")
	}
	for name, version := range versions {
		i := slices.Index(sessionFieldNames, name)
		if i < 0 {
			panic("unknown aws session field: " + name)
		}
		fields[i] = fields[i].Version(version)
	}
	return fields
}

func sessionFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("region").
			Description("The AWS region in which your resources are hosted.").
			Optional().
			Advanced(),
		service.NewStringField("endpoint").
			Description("A custom endpoint URL for AWS API requests. Use this to connect to AWS-compatible services or local testing environments instead of the standard AWS endpoints.").
			Optional().
			Advanced(),
		// tcp joined the session fields, and so every component that used them,
		// in 4.69.0.
		netutil.DialerConfigSpec().Version("4.69.0"),
		service.NewObjectField("credentials",
			service.NewStringField("profile").
				Description("The profile from `~/.aws/credentials` to use.").
				ShortDescription("A profile from ~/.aws/credentials to use.").
				Optional(),
			service.NewStringField("id").
				Description("The ID of the AWS credentials to use.").
				Optional().Advanced(),
			service.NewStringField("secret").
				Description("The secret for the AWS credentials in use.").
				Optional().Advanced().Secret(),
			service.NewStringField("token").
				Description("The token for the AWS credentials in use. Required only when using short-term credentials.").
				Optional().Advanced(),
			service.NewBoolField("from_ec2_role").
				Description("Use the credentials of a host EC2 machine configured to assume https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html[an IAM role associated with the instance^].").
				ShortDescription("Use the credentials of a host EC2 machine assuming an IAM role associated with the instance.").
				Optional().Version("4.2.0"),
			service.NewStringField("role").
				Description("The ARN of the role to assume.").
				Optional().Advanced(),
			service.NewStringField("role_external_id").
				Description("An external ID to use when assuming a role.").
				Optional().Advanced()).
			Advanced().
			Optional().
			Description("Manually configure the AWS credentials to use (optional). For more information, see the xref:guides:cloud/aws.adoc[Amazon Web Services guide].").
			ShortDescription("Optional manual configuration of AWS credentials to use."),
	}
}
