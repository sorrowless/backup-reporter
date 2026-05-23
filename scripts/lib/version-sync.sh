#!/usr/bin/env bash
# Version helpers for release-bump.sh (source, do not execute directly).

set -euo pipefail

PACKAGE_NAME="${PACKAGE_NAME:-backup-reporter}"
PYPROJECT="${PYPROJECT:-pyproject.toml}"
GIT_REMOTE="${GIT_REMOTE:-origin}"
VENV="${VENV:-.venv}"

_python3() {
  if [ -x "${VENV}/bin/python3" ]; then
    "${VENV}/bin/python3"
  else
    python3
  fi
}

normalize_version() {
  local v="${1#v}"
  echo "$v"
}

file_version() {
  sed -n 's/^version = "\(.*\)"/\1/p' "$PYPROJECT" | head -1
}

git_latest_tag_version() {
  local tag
  tag="$(git for-each-ref --sort=-v:refname --format='%(refname:short)' refs/tags 2>/dev/null | head -1 || true)"
  if [ -z "$tag" ]; then
    echo "0.0.0"
    return
  fi
  normalize_version "$tag"
}

pypi_latest_version() {
  local url="https://pypi.org/pypi/${PACKAGE_NAME}/json"
  local version py
  py="$(_python3)"
  if ! version="$(curl -fsSL --max-time 30 "$url" 2>/dev/null | "$py" -c "
import json, sys
try:
    data = json.load(sys.stdin)
    print(data.get('info', {}).get('version', '0.0.0'))
except Exception:
    print('0.0.0')
" 2>/dev/null)"; then
    echo "0.0.0"
    return
  fi
  echo "$version"
}

semver_max() {
  printf '%s\n' "$@" | sort -V | tail -1
}

semver_patch_bump() {
  "$(_python3)" -c "
from packaging.version import Version
v = Version('$1')
parts = v.release
if len(parts) >= 3:
    print(f'{parts[0]}.{parts[1]}.{parts[2] + 1}')
elif len(parts) == 2:
    print(f'{parts[0]}.{parts[1]}.1')
else:
    print(f'{parts[0]}.0.1')
" 2>/dev/null || {
    # Fallback without packaging module
    local ver="$1"
    local major minor patch
    ver="$(normalize_version "$ver")"
    major="${ver%%.*}"
    rest="${ver#*.}"
    minor="${rest%%.*}"
    patch="${rest#*.}"
    if [ "$patch" = "$rest" ] || [ -z "$patch" ]; then
      patch=0
    fi
    echo "${major}.${minor}.$((patch + 1))"
  }
}

tag_for_version() {
  echo "v$(normalize_version "$1")"
}

tag_points_to_head() {
  local ver="$1"
  local tag
  tag="$(tag_for_version "$ver")"
  if ! git rev-parse "$tag" >/dev/null 2>&1; then
    return 1
  fi
  [ "$(git rev-parse "$tag")" = "$(git rev-parse HEAD)" ]
}

remote_tag_exists() {
  local tag="$1"
  git ls-remote --exit-code "$GIT_REMOTE" "refs/tags/${tag}" >/dev/null 2>&1
}

remote_tag_commit() {
  local tag="$1"
  git ls-remote "$GIT_REMOTE" "refs/tags/${tag}" | awk '{print $1}' | head -1
}

fetch_remote_tags() {
  git fetch "$GIT_REMOTE" --tags --force 2>/dev/null || git fetch "$GIT_REMOTE" --tags 2>/dev/null || true
}
