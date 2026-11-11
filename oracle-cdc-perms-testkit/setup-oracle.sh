#!/usr/bin/env bash
# Spins up the Oracle test instance used for the permission validation and applies the
# base setup (restricted CDC user + source schema). Control / doc-route users and the
# per-test grant mutations are separate (see sql/12, sql/13, sql/14).
set -euo pipefail

cd "$(dirname "$0")"

CONTAINER=oracle-cdc-perms
IMAGE=container-registry.oracle.com/database/free:latest-lite
ORACLE_PWD=YourPassword123

docker run -d --name "$CONTAINER" \
  -p 1521:1521 \
  -e ORACLE_PWD="$ORACLE_PWD" \
  "$IMAGE"

echo "waiting for the database to come up (this takes a few minutes on first run)..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' "$CONTAINER")" = "healthy" ]; do
  sleep 5
done
echo "database is healthy"

# Base setup: restricted CDC user (as SYS in CDB$ROOT) + source schema (as SYSTEM in the PDB).
docker exec -i "$CONTAINER" sqlplus -S / as sysdba < sql/10-cdb-restricted-user.sql
docker exec -i "$CONTAINER" sqlplus -S "system/${ORACLE_PWD}@//localhost:1521/FREEPDB1" < sql/11-pdb-source-schema.sql

echo "done — restricted user C##REDPANDA and source table SPACE_APP.T_CHANNEL_DEF are in place."
echo "optional extra profiles:"
echo "  docker exec -i $CONTAINER sqlplus -S / as sysdba < sql/12-cdb-control-user.sql"
echo "  docker exec -i $CONTAINER sqlplus -S / as sysdba < sql/13-cdb-logminer-doc-user.sql"
