#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export VENV="${VENV:-.venv}"
export PACKAGE_NAME="${PACKAGE_NAME:-backup-reporter}"
export GIT_REMOTE="${GIT_REMOTE:-origin}"

# shellcheck source=scripts/lib/version-sync.sh
source "$ROOT/scripts/lib/version-sync.sh"

die() {
  echo "release-bump: $*" >&2
  exit 1
}

if ! git diff --quiet || ! git diff --cached --quiet; then
  die "working tree is not clean; commit or stash changes before release"
fi

fetch_remote_tags

GIT_VER="$(git_latest_tag_version)"
PYPI_VER="$(pypi_latest_version)"
FILE_VER="$(file_version)"

BASE="$(semver_max "$GIT_VER" "$PYPI_VER" "$FILE_VER")"

# Strict mode: PyPI ahead of git tags without matching remote tag
if [ "${RELEASE_STRICT:-0}" = "1" ] && [ "$(printf '%s\n' "$PYPI_VER" "$GIT_VER" | sort -V | tail -1)" = "$PYPI_VER" ] && [ "$PYPI_VER" != "$GIT_VER" ]; then
  pypi_tag="$(tag_for_version "$PYPI_VER")"
  if ! remote_tag_exists "$pypi_tag"; then
    die "PyPI version ${PYPI_VER} is ahead of latest git tag ${GIT_VER}; push tag from release machine: git push ${GIT_REMOTE} ${pypi_tag}"
  fi
fi

LAST_TAG_REF=""
if [ "$GIT_VER" != "0.0.0" ]; then
  LAST_TAG_REF="$(tag_for_version "$GIT_VER")"
  if ! git rev-parse "$LAST_TAG_REF" >/dev/null 2>&1; then
    LAST_TAG_REF=""
  fi
fi

if [ -n "$LAST_TAG_REF" ]; then
  if [ -z "$(git rev-list "${LAST_TAG_REF}..HEAD" 2>/dev/null || true)" ]; then
    die "no commits since ${LAST_TAG_REF}; add changes before releasing"
  fi
fi

if tag_points_to_head "$FILE_VER"; then
  die "version ${FILE_VER} is already released at HEAD; add new commits before releasing"
fi

NEW="$(semver_patch_bump "$BASE")"
NEW_TAG="$(tag_for_version "$NEW")"

if remote_tag_exists "$NEW_TAG"; then
  remote_commit="$(remote_tag_commit "$NEW_TAG")"
  head_commit="$(git rev-parse HEAD)"
  if [ -n "$remote_commit" ] && [ "$remote_commit" != "$head_commit" ]; then
    die "tag ${NEW_TAG} already exists on ${GIT_REMOTE} at a different commit; resolve manually"
  fi
fi

if ! command -v poetry >/dev/null 2>&1; then
  die "poetry not found in PATH; run make prepare first"
fi

poetry version "$NEW" >/dev/null

SUBJECT="Bump version to ${NEW}"
BODY=""
if [ -n "$LAST_TAG_REF" ]; then
  BODY="$(git log --format='- %s' "${LAST_TAG_REF}..HEAD" 2>/dev/null || true)"
fi
if [ "$PYPI_VER" != "$GIT_VER" ] && [ "$(printf '%s\n' "$PYPI_VER" "$GIT_VER" | sort -V | tail -1)" = "$PYPI_VER" ]; then
  if [ -n "$BODY" ]; then
    BODY="Recovery release: PyPI ahead of git tags

${BODY}"
  else
    BODY="Recovery release: PyPI ahead of git tags"
  fi
fi

git add "$PYPROJECT"
if [ -n "$BODY" ]; then
  git commit -m "$SUBJECT" -m "$BODY"
else
  git commit -m "$SUBJECT"
fi

git tag -a "$NEW_TAG" -m "Release ${NEW}"

echo "release-bump: version ${FILE_VER} -> ${NEW}, tag ${NEW_TAG}"
poetry build
