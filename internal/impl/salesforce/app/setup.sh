#!/usr/bin/env bash
# Deploys the Redpanda Connect Connected App to the authenticated Salesforce org
# and prints SALESFORCE_ORG_URL and SALESFORCE_CLIENT_ID to stdout.
# SALESFORCE_CLIENT_SECRET is not retrievable via API and must be added manually.
#
# Usage:
#   sf org login web
#   ./setup.sh > .env
#
# Target org selection:
#   - $SF_TARGET_ORG if set
#   - the default org (sf config set target-org=...)
#   - the only authenticated non-scratch org, if there is exactly one

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for cmd in sf jq envsubst xmllint curl; do
  command -v "$cmd" &>/dev/null || { echo "error: $cmd not found" >&2; exit 1; }
done

TARGET_ORG="${SF_TARGET_ORG:-}"
if [ -z "$TARGET_ORG" ]; then
  TARGET_ORG=$(sf config get target-org --json 2>/dev/null \
    | jq -r '.result[0].value // empty')
fi
if [ -z "$TARGET_ORG" ]; then
  ORGS=$(sf org list --json | jq -r '[.result.nonScratchOrgs[]?, .result.scratchOrgs[]?] | map(.username)')
  if [ "$(echo "$ORGS" | jq 'length')" = "1" ]; then
    TARGET_ORG=$(echo "$ORGS" | jq -r '.[0]')
  else
    echo "error: no target org found. Run 'sf org login web --set-default' or set SF_TARGET_ORG=<username>." >&2
    exit 1
  fi
fi

ORG_JSON=$(sf org display --target-org "$TARGET_ORG" --json | jq -r '.result')
export ADMIN_USERNAME=$(echo "$ORG_JSON" | jq -r '.username')
ORG_URL=$(echo "$ORG_JSON" | jq -r '.instanceUrl')

echo "org:  $ORG_URL" >&2
echo "user: $ADMIN_USERNAME" >&2

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

cp -R "$DIR/." "$TMPDIR/"
rm -f "$TMPDIR/setup.sh"
envsubst '${ADMIN_USERNAME}' \
  < "$DIR/connectedApps/RedpandaConnect.connectedApp" \
  > "$TMPDIR/connectedApps/RedpandaConnect.connectedApp"

DEPLOY_OUT=$(sf project deploy start --target-org "$TARGET_ORG" --metadata-dir "$TMPDIR" --wait 30 --json) || {
  echo "error: deploy failed" >&2
  echo "$DEPLOY_OUT" | jq '.result.details.componentFailures // .message // .' >&2
  exit 1
}

RETRIEVE_DIR="$TMPDIR/retrieve"
sf project retrieve start --target-org "$TARGET_ORG" \
  --metadata "ConnectedApp:RedpandaConnect" \
  --target-metadata-dir "$RETRIEVE_DIR" --unzip --json >/dev/null

RETRIEVED_APP="$RETRIEVE_DIR/unpackaged/unpackaged/connectedApps/RedpandaConnect.connectedApp"
if [ ! -f "$RETRIEVED_APP" ]; then
  echo "error: retrieved ConnectedApp not found at $RETRIEVED_APP (deploy may not have propagated yet)" >&2
  exit 1
fi
CONSUMER_KEY=$(xmllint --xpath "//*[local-name()='consumerKey']/text()" "$RETRIEVED_APP" 2>/dev/null || true)
if [ -z "$CONSUMER_KEY" ]; then
  echo "error: could not parse ConsumerKey from $RETRIEVED_APP" >&2
  exit 1
fi

echo "SALESFORCE_ORG_URL=\"$ORG_URL\""
echo "SALESFORCE_CLIENT_ID=\"$CONSUMER_KEY\""

ACCESS_TOKEN=$(echo "$ORG_JSON" | jq -r '.accessToken')
API="$ORG_URL/services/data/v65.0"

# sf_query <soql> [tooling/query]; defaults to standard query endpoint.
sf_query() {
  curl -sfS -H "Authorization: Bearer $ACCESS_TOKEN" --data-urlencode "q=$1" \
    -G "$API/${2:-query}" | jq -r '.records[0].Id // empty'
}
# sf_post <sobject> <json-body>; prints raw response body.
sf_post() {
  curl -sfS -X POST "$API/sobjects/$1" -H "Authorization: Bearer $ACCESS_TOKEN" \
    -H "Content-Type: application/json" -d "$2"
}

USER_ID=$(sf_query "SELECT Id FROM User WHERE Username='$ADMIN_USERNAME'")
APP_ID=$(sf_query "SELECT Id FROM ConnectedApplication WHERE Name='Redpanda Connect'" tooling/query)
if [ -z "$USER_ID" ]; then
  echo "error: User '$ADMIN_USERNAME' not found" >&2; exit 1
fi
if [ -z "$APP_ID" ]; then
  echo "error: ConnectedApplication 'Redpanda Connect' not queryable — metadata deploy may not have propagated yet" >&2; exit 1
fi

# client_credentials requires the Run-As user be pre-authorized for the app,
# otherwise the token endpoint returns "user is not admin approved".
PERMSET_ID=$(sf_query "SELECT Id FROM PermissionSet WHERE Name='RedpandaConnectApi'")
if [ -z "$PERMSET_ID" ]; then
  PERMSET_ID=$(sf_post PermissionSet \
    '{"Label":"Redpanda Connect API","Name":"RedpandaConnectApi"}' | jq -r '.id // empty')
  if [ -z "$PERMSET_ID" ]; then
    echo "error: failed to create RedpandaConnectApi permission set" >&2; exit 1
  fi
fi
SEA_ID=$(sf_query \
  "SELECT Id FROM SetupEntityAccess WHERE ParentId='$PERMSET_ID' AND SetupEntityId='$APP_ID'")
if [ -z "$SEA_ID" ]; then
  sf_post SetupEntityAccess \
    "{\"ParentId\":\"$PERMSET_ID\",\"SetupEntityId\":\"$APP_ID\"}" >/dev/null
fi
PSA_ID=$(sf_query \
  "SELECT Id FROM PermissionSetAssignment WHERE PermissionSetId='$PERMSET_ID' AND AssigneeId='$USER_ID'")
if [ -z "$PSA_ID" ]; then
  sf_post PermissionSetAssignment \
    "{\"PermissionSetId\":\"$PERMSET_ID\",\"AssigneeId\":\"$USER_ID\"}" >/dev/null
fi

SETUP_URL=$(echo "$ORG_URL" | sed 's/\.salesforce\.com$/.salesforce-setup.com/')
INNER="/app/mgmt/forceconnectedapps/forceAppDetail.apexp?connectedAppId=${APP_ID}&appLayout=setup"
ADDRESS=$(jq -rn --arg s "$INNER" '$s|@uri')
echo "Connected App: $SETUP_URL/lightning/setup/ConnectedApplication/page?address=$ADDRESS" >&2

# Verify client_credentials end-to-end when the secret is available.
if [ -n "${SALESFORCE_CLIENT_SECRET:-}" ]; then
  RESP=$(curl -sS -X POST "$ORG_URL/services/oauth2/token" \
    --data-urlencode "grant_type=client_credentials" \
    --data-urlencode "client_id=$CONSUMER_KEY" \
    --data-urlencode "client_secret=$SALESFORCE_CLIENT_SECRET")
  if echo "$RESP" | jq -e '.access_token' >/dev/null 2>&1; then
    echo "OAuth: OK" >&2
  else
    echo "OAuth: $(echo "$RESP" | jq -r '.error_description // .error // "unknown"')" >&2
    exit 1
  fi
fi
