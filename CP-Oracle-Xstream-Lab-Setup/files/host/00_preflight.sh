#!/usr/bin/env bash
# Preflight checks on the target host. Fails fast before anything is installed.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

fail() { echo "PREFLIGHT FAILED: $*" >&2; exit 1; }
warn() { echo "PREFLIGHT WARNING: $*" >&2; }

echo "== Preflight on $(hostname)"

. /etc/os-release
echo "OS: $PRETTY_NAME"
[ "${ID:-}" = "amzn" ] || warn "scripts are tested on Amazon Linux 2023, found $ID"

ARCH=$(uname -m)
echo "Arch: $ARCH"
[ "$ARCH" = "x86_64" ] || fail "Oracle XE 21c image is amd64 only (found $ARCH). Use an x86_64 instance type."

# If the EBS volume was enlarged in the console, grow the partition + filesystem to match.
ROOT_SRC=$(findmnt -n -o SOURCE /)
ROOT_FSTYPE=$(findmnt -n -o FSTYPE /)
ROOT_DISK=/dev/$(lsblk -n -o PKNAME "$ROOT_SRC")
ROOT_PART=$(cat "/sys/class/block/$(basename "$ROOT_SRC")/partition" 2>/dev/null || true)
if [ -n "$ROOT_PART" ] && command -v growpart >/dev/null; then
  growpart "$ROOT_DISK" "$ROOT_PART" >/dev/null 2>&1 && echo "Root partition grown to fill the volume" || true
  case "$ROOT_FSTYPE" in
    xfs)  xfs_growfs -d / >/dev/null ;;
    ext4) resize2fs "$ROOT_SRC" >/dev/null ;;
  esac
fi

FREE_GB=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
echo "Free disk on /: ${FREE_GB} GB (need >= ${MIN_FREE_DISK_GB} GB)"
if [ "$FREE_GB" -lt "$MIN_FREE_DISK_GB" ] && ! docker image inspect "$ORACLE_IMAGE" >/dev/null 2>&1; then
  fail "not enough disk. Increase the root EBS volume (e.g. to 100 GB) in the AWS console, then re-run terraform apply - the partition is grown automatically."
fi

MEM_GB=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
echo "Memory: ${MEM_GB} GB"
[ "$MEM_GB" -ge 7 ] || fail "at least 8 GB RAM is required (16 GB recommended)."
[ "$MEM_GB" -ge 15 ] || warn "8 GB RAM works with the reduced heaps + swap, 16 GB (t3.xlarge) is recommended."

for url in https://container-registry.oracle.com https://registry-1.docker.io https://hub-downloads.confluent.io https://download.oracle.com https://raw.githubusercontent.com; do
  curl -s -o /dev/null --max-time 15 "$url" || fail "no outbound HTTPS access to $url"
done
echo "Outbound internet: OK"
echo "== Preflight passed"
