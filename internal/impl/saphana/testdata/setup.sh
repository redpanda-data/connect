#!/usr/bin/env bash
# Run ONCE on the host before starting docker-compose.
# Must run as root (or with sudo).
set -euo pipefail

HANA_DATA="${HANA_DATA_PATH:-/tmp/hana-express-data}"

echo "==> Creating HANA data directory at $HANA_DATA"
mkdir -p "$HANA_DATA"
chown 12000:79 "$HANA_DATA"   # hxeadm:sapsys

echo "==> Copying password file"
cp "$(dirname "$0")/hxepasswd.json" "$HANA_DATA/hxepasswd.json"
chown 12000:79 "$HANA_DATA/hxepasswd.json"
chmod 600 "$HANA_DATA/hxepasswd.json"

echo "==> Setting required host kernel parameters"
sysctl -w kernel.shmmax=1073741824
sysctl -w kernel.shmmni=524288
sysctl -w kernel.shmall=8388608
sysctl -w fs.file-max=20000000
sysctl -w fs.aio-max-nr=262144
sysctl -w vm.memory_failure_early_kill=1
sysctl -w vm.max_map_count=135217728
sysctl -w net.ipv4.ip_local_port_range="40000 60999"

echo "==> Done. Now run:"
echo "    HANA_DATA_PATH=$HANA_DATA docker-compose up -d"
echo ""
echo "    First startup takes 5-10 minutes."
echo "    After HANA is healthy, activate log_mode=normal:"
echo "    BACKUP DATA USING FILE ('/hana/mounts/backup/FULL');"
