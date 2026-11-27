#!/usr/bin/env bash
# Start the local EE clusters used by TestEE* (and later tickets).
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"

KEY="${AEROSPIKE_FEATURE_KEY:-$DIR/features.conf}"
if [[ ! -f "$KEY" ]]; then
  echo "Need an Enterprise feature-key file." >&2
  echo "  export AEROSPIKE_FEATURE_KEY=/path/to/features.conf" >&2
  echo "  or copy it to $DIR/features.conf (gitignored)" >&2
  exit 1
fi
export AEROSPIKE_FEATURE_KEY="$KEY"

chmod +x "$DIR/gen-certs.sh"
"$DIR/gen-certs.sh"

cd "$DIR"
docker compose up -d

echo "waiting for as-ee-1 / as-ee-2 / as-ee-sec ..."
for c in as-ee-1 as-ee-2 as-ee-sec; do
  ready=0
  for _ in $(seq 1 60); do
    # Capture first. Piping into grep -q lets grep exit early, the producer
    # dies with SIGPIPE, and pipefail turns a ready node into a failed check.
    logs="$(docker logs "$c" 2>&1 || true)"
    if grep -q "soon there will be cake" <<<"$logs"; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "$ready" -ne 1 ]]; then
    echo "$c did not become ready" >&2
    exit 1
  fi
done

# Strong-consistency namespace needs a roster before writes succeed.
echo "waiting for as-ee-1 + as-ee-2 to form a 2-node cluster ..."
ready=0
for _ in $(seq 1 60); do
  stats="$(docker exec as-ee-1 asinfo -v statistics 2>/dev/null || true)"
  if grep -q 'cluster_size=2' <<<"$stats"; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "$ready" -ne 1 ]]; then
  echo "as-ee-1 and as-ee-2 did not form a 2-node cluster" >&2
  exit 1
fi

# Auth tests expect user rpcn / rpcnpass. Create it before the roster step so
# a roster failure does not leave the TLS tests with only the admin user.
echo "creating user rpcn on as-ee-sec ..."
docker exec as-ee-sec asadm -U admin -P admin --enable -e \
  "manage acl create user rpcn password rpcnpass roles read-write sys-admin" || true
docker exec as-ee-sec asadm -U admin -P admin --enable -e \
  "manage acl grant user rpcn roles read-write sys-admin" || true

# A non-null roster is not enough. After a container restart the namespace can
# still name the previous node ids, and strong-consistency writes then fail
# until the roster is restaged onto the nodes that are actually up.
node_set() {
  printf '%s' "$1" | tr ',' '\n' | sed '/^$/d;/^null$/d' | sort -u | paste -sd, - || true
}

# recluster is ignored unless it is sent to the current principal.
principal_container() {
  local c stats node principal node_id principal_id
  for c in as-ee-1 as-ee-2; do
    stats="$(docker exec "$c" asinfo -v statistics 2>/dev/null || true)"
    node="$(docker exec "$c" asinfo -v node 2>/dev/null || true)"
    principal="$(printf '%s\n' "$stats" | tr ';' '\n' | sed -n 's/^cluster_principal=//p')"
    node_id="$(printf '%s' "$node" | tr '[:lower:]' '[:upper:]')"
    principal_id="$(printf '%s' "$principal" | tr '[:lower:]' '[:upper:]')"
    if [[ -n "$node_id" && "$node_id" == "$principal_id" ]]; then
      printf '%s' "$c"
      return
    fi
  done
  printf '%s' as-ee-1
}

# Memory storage has no copies after a restart, so a matching roster still
# leaves every strong-consistency partition dead until each node is revived.
sc_dead() {
  local c stats
  for c in as-ee-1 as-ee-2; do
    stats="$(docker exec "$c" asinfo -v 'namespace/sc' 2>/dev/null || true)"
    if ! grep -q 'dead_partitions=0;' <<<"${stats};"; then
      return 0
    fi
  done
  return 1
}

echo "staging SC roster on namespace sc ..."
ready=0
for _ in $(seq 1 30); do
  principal="$(principal_container)"
  roster="$(docker exec "$principal" asinfo -v 'roster:namespace=sc' 2>/dev/null || true)"
  roster_nodes="${roster#roster=}"
  roster_nodes="${roster_nodes%%:*}"
  observed="${roster##*observed_nodes=}"
  observed="${observed%%:*}"
  roster_set="$(node_set "$roster_nodes")"
  observed_set="$(node_set "$observed")"
  stats="$(docker exec "$principal" asinfo -v 'namespace/sc' 2>/dev/null || true)"
  observed_count=0
  if [[ -n "$observed_set" ]]; then
    observed_count="$(awk -F, '{print NF}' <<<"$observed_set")"
  fi
  if [[ "$observed_count" -eq 2 && "$roster_set" == "$observed_set" ]] && grep -q 'ns_cluster_size=2' <<<"$stats" && ! sc_dead; then
    echo "  $roster"
    ready=1
    break
  fi
  if [[ "$observed_count" -eq 2 ]]; then
    echo "  roster (${roster_set:-null}) observed ($observed_set); repairing on $principal"
    if [[ "$roster_set" != "$observed_set" ]]; then
      docker exec "$principal" asinfo -v "roster-set:namespace=sc;nodes=$observed" >/dev/null 2>&1 || true
    fi
    if sc_dead; then
      docker exec as-ee-1 asinfo -v 'revive:namespace=sc' >/dev/null 2>&1 || true
      docker exec as-ee-2 asinfo -v 'revive:namespace=sc' >/dev/null 2>&1 || true
    fi
    docker exec "$principal" asinfo -v 'recluster:' >/dev/null 2>&1 || true
  fi
  sleep 2
done
if [[ "$ready" -ne 1 ]]; then
  echo "namespace sc roster does not match the live nodes" >&2
  exit 1
fi

cat <<EOF

EE env for this checkout:

  export AEROSPIKE_EE_HOSTS=127.0.0.1:3100,127.0.0.1:3110
  export AEROSPIKE_SEC_HOST=127.0.0.1:3200
  export AEROSPIKE_TLS_HOST=127.0.0.1:clusterA:4333
  export AEROSPIKE_TLS_CA=$DIR/certs/ca.pem

  go test -count=1 -timeout 10m -v -run 'TestEE' ./internal/impl/aerospike/

Stop:  docker compose -f $DIR/docker-compose.yml down
EOF
