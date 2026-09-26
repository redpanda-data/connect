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

package config

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Fields shared by the `aws` IAM authentication blocks of the database
// components (mongodb, mysql_cdc, postgres_cdc).

// IAMAuthRegionField returns the region field of an IAM authentication block
// for a database that signs its authentication token for the region of the
// named product's instance.
func IAMAuthRegionField(product string) *service.ConfigField {
	return service.NewStringField("region").
		Description("The AWS region where the " + product + " instance is located. The region is used to sign the IAM authentication token and for STS calls when assuming roles. If no region is specified, the environment default is used.").
		ShortDescription("The AWS region where the " + product + " instance is located. Defaults to the environment region.").
		Optional()
}

// IAMAuthStaticCredentialFields returns the id, secret and token fields of an
// IAM authentication block.
func IAMAuthStaticCredentialFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("id").
			Description("The AWS access key ID to authenticate with. When empty, the default AWS credential chain is used.").Version("4.72.0").
			Optional().Advanced(),
		service.NewStringField("secret").
			Description("The AWS secret access key that pairs with `id`.").Version("4.72.0").
			Optional().Advanced().Secret(),
		service.NewStringField("token").
			Description("The AWS session token to use with `id` and `secret`. Required only when using short-term credentials.").Version("4.72.0").
			Optional().Advanced(),
	}
}

// IAMAuthRoleFields returns the role, role_external_id and roles fields of an
// IAM authentication block. When exclusive is true, role and roles cannot both
// be set; otherwise role is assumed first, followed by each entry of roles.
func IAMAuthRoleFields(exclusive bool) []*service.ConfigField {
	role := service.NewStringField("role").Optional()
	roles := service.NewObjectListField("roles",
		service.NewStringField("role").
			Default("").
			Description("AWS IAM role ARN to assume."),
		service.NewStringField("role_external_id").
			Description("Optional external ID for the role assumption.").
			Default("").
			Optional(),
	).
		ShortDescription("AWS IAM roles to assume for authentication. Assumed in sequence to allow role chaining.").
		Optional()

	const (
		roleDesc  = "Optional AWS IAM role ARN to assume for authentication."
		rolesDesc = "Optional array of AWS IAM roles to assume for authentication. Roles are assumed in sequence, each using the credentials of the previous one, enabling chaining for purposes such as cross-account access. Each role can optionally specify an external ID."
	)
	if exclusive {
		role = role.
			Description(roleDesc + " Cannot be combined with `roles`; use the `roles` array instead when chaining multiple roles.").
			ShortDescription("Optional AWS IAM role ARN to assume for authentication. Cannot be combined with roles.")
		roles = roles.Description(rolesDesc + " Cannot be combined with `role`.")
	} else {
		role = role.
			Description(roleDesc + " When `roles` is also set, this role is assumed first and the `roles` entries are assumed after it.").
			ShortDescription("Optional AWS IAM role ARN to assume for authentication.")
		roles = roles.Description(rolesDesc + " When `role` is also set, it is assumed before the first entry.")
	}

	return []*service.ConfigField{
		role,
		service.NewStringField("role_external_id").
			Description("Optional external ID to use when assuming the role set in `role`. Each entry in `roles` sets its own external ID.").Version("4.72.0").
			ShortDescription("Optional external ID for the role assumption. Only used alongside the role field.").
			Optional(),
		roles,
	}
}
