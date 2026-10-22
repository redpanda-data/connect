# Local Aerospike Enterprise for `TestEE*`

These tests do **not** run in CI, because they need an Aerospike Enterprise
feature key. Run them locally. `TestEE*` skip unless the env vars below are
set; they do **not** start Docker themselves.

Community Edition (`TestIntegration*`) still uses testcontainers and does not
need this stack.

## Prerequisites

- Docker (Desktop on macOS is fine)
- Go (same version as `go.mod`)
- An Aerospike **Enterprise feature-key** file (`features.conf`) from Aerospike.
  Do not commit it.

Images: `aerospike/aerospike-server-enterprise:8.1` and
`aerospike/aerospike-tools:13.1.0`. The tools image uses its own version
line, not the server's. Docker Desktop project name:
`aero-redpanda-ee`.

## What you get (4 containers)

| Container | Host address | Env | Role |
|---|---|---|---|
| `as-ee-1` | `127.0.0.1:3100` | `AEROSPIKE_EE_HOSTS` | Unsecured 2-node cluster, RF=2, cluster-name `aero-integ-redpanda-ee` |
| `as-ee-2` | `127.0.0.1:3110` | (same list) | Peer of node 1; namespaces `test` (AP) and `sc` (strong consistency) |
| `as-ee-sec` | `127.0.0.1:3200` plaintext, `127.0.0.1:clusterA:4333` TLS | `AEROSPIKE_SEC_HOST`, `AEROSPIKE_TLS_HOST` | Separate cluster `aero-integ-redpanda-ee-sec`. User `rpcn` / `rpcnpass` (also PKI; certificate `client.pem` CN `rpcn`). The TLS port requires that client certificate. |
| `as-ee-tools` | Compose network only | — | `aql`, `asadm`, and `asinfo` |

On Docker Desktop, configs use loopback `access-address` / `access-port`.
Do not point tests at `172.17` / `172.20` container IPs.
The tools container instead requests each server's `alternate-access-address`,
which is its Compose DNS name (`as-ee-1`, `as-ee-2`, or `as-ee-sec`).

The image still ships `/etc/aerospike/aerospike.conf` with `cluster-name docker`.
`asd` does **not** use that file. It starts with `--config-file /opt/rpcn/aerospike.conf`
(our `as-ee-*.conf`). Confirm the live name:

```bash
docker exec as-ee-1 asinfo -v cluster-name
# aero-integ-redpanda-ee
```

TLS certs under `certs/` are gitignored. `./up.sh` reuses them if present,
otherwise runs `./gen-certs.sh`. Force new certs with `FORCE=1 ./gen-certs.sh`.

## Start

From the **repository root**:

```bash
cd internal/impl/aerospike/testdata/ee

# Either copy the key here (gitignored):
cp /path/to/features.conf .

# Or point at it:
# export AEROSPIKE_FEATURE_KEY=/path/to/features.conf

chmod +x up.sh gen-certs.sh
./up.sh
```

`./up.sh` takes a couple of minutes (image pull on first run, then cluster
form + SC roster). It prints the `export` lines to copy.

## Run tests

From the **repository root** (not from `testdata/ee`). Use an **absolute**
`AEROSPIKE_TLS_CA` (`go test` cwd is the package directory).

```bash
export AEROSPIKE_EE_HOSTS=127.0.0.1:3100,127.0.0.1:3110
export AEROSPIKE_SEC_HOST=127.0.0.1:3200
export AEROSPIKE_TLS_HOST=127.0.0.1:clusterA:4333
export AEROSPIKE_TLS_CA="$PWD/internal/impl/aerospike/testdata/ee/certs/ca.pem"

go test -count=1 -timeout 10m -v -run 'TestEE' ./internal/impl/aerospike/
```

`-count=1` disables Go's test cache. Cached `ok` does not prove the cluster
is up.

## Inspect records with AQL

The host does not need Aerospike Tools installed. Use `as-ee-tools`.

Bare `aql` inside that container fails for two reasons:

- `aql` with no `-h` seeds `127.0.0.1:3000` **inside the tools container** (nothing listens there).
- `aql -h 172.18.0.2` reaches a node, then follows advertised peers `127.0.0.1:3100` / `:3110`. Those ports exist on the **host**, not in this container.

`astools.conf` turns on `services-alternate`. Recreate tools: `docker compose up -d as-ee-tools`.

From the host:

```bash
docker exec -it as-ee-tools aql-ee
docker exec as-ee-tools aql-ee -c "SELECT * FROM test.rpa_ee"
```

Already inside `as-ee-tools` (do **not** use `aql` or `aql -h 172.18.0.2` alone):

```bash
aql-ee
aql -h as-ee-1 --services-alternate
```

Secured cluster (no default in astools.conf):

```bash
docker exec -it as-ee-tools aql --host as-ee-sec --port 3000 \
  --services-alternate --no-config-file \
  --user rpcn --password rpcnpass
```

For `TestEETTLExpiresRemovesRecord`, the record is `test.rpa_ee` PK `ttl-gone` with TTL **20s**. The test then waits up to **45s** for NSUP (`nsup-period 10`) to delete it. While it runs:

```bash
docker exec as-ee-tools aql-ee -c "SELECT * FROM test.rpa_ee WHERE PK='ttl-gone'"
```

Present after the write; gone after TTL + NSUP. Faster TTL checks (`24H`, `1D`, invalid `24X`) stay on CE `TestIntegrationTTL*`.

Auth tests use `rpcn` / `rpcnpass`. That account lives in `smd/ee-sec`
(`security.smd`) so a recreate keeps it. The tests create it once per run
with the Go client (`CreateUser` as `admin` / `admin` on `AEROSPIKE_SEC_HOST`)
when it is missing. If that login fails, the image may use a different admin
password:

```bash
docker exec -it as-ee-sec asadm -U admin -P '<admin-password>' --enable \
  -e "manage acl create user rpcn password rpcnpass roles read-write sys-admin"
```

`as-ee-1` and `as-ee-2` pin `node-id` in their config, and `smd/` keeps the
`sc` roster across a container recreate. Namespace `sc` is memory-backed, so
a restart still marks all 4096 partitions dead until `revive:namespace=sc` is
sent to both nodes and `recluster:` is sent to the principal. `./up.sh` and
`TestEEStrongConsistency` do that, and they restage the roster when it names
node ids that are gone. Only the principal accepts `recluster`.

## Stop

```bash
cd internal/impl/aerospike/testdata/ee
docker compose down
```

`features.conf` and `certs/*` (except `.gitkeep`) stay gitignored.
