#!/usr/bin/env bash

set -euo pipefail

event_file="${1:-.github/act-events/release-tag.json}"
mode="${2:---dry-run}"

if ! command -v act >/dev/null 2>&1; then
  echo "act is not installed. Install with: brew install act"
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not running. Start Docker Desktop and retry."
  exit 1
fi

if [[ ! -f "$event_file" ]]; then
  echo "event file not found: $event_file"
  exit 1
fi

declare -a cmd=(
  act
  push
  -W .github/workflows/release.yml
  -e "$event_file"
  -j release
  --container-architecture linux/amd64
  --pull=false
  --rebuild=false
  -P ubuntu-latest=catthehacker/ubuntu:act-latest
)

if [[ "$mode" == "--dry-run" ]]; then
  cmd+=("-n")
elif [[ "$mode" != "--run" ]]; then
  echo "invalid mode: $mode (use --dry-run or --run)"
  exit 1
fi

XDG_CACHE_HOME="${XDG_CACHE_HOME:-/tmp}" \
ACT_CACHE_DIR="${ACT_CACHE_DIR:-/tmp/act}" \
ACT_CONFIG_HOME="${ACT_CONFIG_HOME:-/tmp/actcfg}" \
"${cmd[@]}"
