#!/usr/bin/env bash
# Select an immutable comparison commit for the staged publication check.
set -euo pipefail

refuse() {
  echo "REFUSING: $*" >&2
  exit 2
}

fetch_commit() {
  git fetch --depth=1 origin "$1" || refuse "could not fetch comparison commit $1."
  selected="$(git rev-parse --verify 'FETCH_HEAD^{commit}')" || refuse "could not read the fetched comparison commit."
}

zero=0000000000000000000000000000000000000000
selected=
if [ -n "${BASE_SHA:-}" ]; then
  fetch_commit "$BASE_SHA"
elif [ -n "${PUSH_BEFORE:-}" ] && [ "$PUSH_BEFORE" != "$zero" ]; then
  fetch_commit "$PUSH_BEFORE"
else
  [ -n "${EVENT_SHA:-}" ] || refuse "the new-ref event has no commit SHA."
  event="$(git rev-parse --verify "$EVENT_SHA^{commit}")" || refuse "could not read the event commit."
  # A shallow boundary hides ^ from revision walking, but the commit object
  # still declares its parents. Consume the complete object and check its read.
  object="$(git cat-file commit "$event")" || refuse "could not read the event commit object."
  parent="$(printf '%s\n' "$object" | awk '
    BEGIN { header=1 }
    /^$/ { header=0 }
    header && $1 == "parent" && parent == "" { parent=$2 }
    END { print parent }
  ')" || refuse "could not read the event parent."
  if [ -n "$parent" ]; then
    if ! git cat-file -e "$parent^{commit}" 2>/dev/null; then
      shallow="$(git rev-parse --is-shallow-repository)" || refuse "could not read shallow-repository state."
      [ "$shallow" = true ] || refuse "the event parent is missing from a complete repository."
      # Fetch the event's fixed SHA, never the current tip of its moving ref.
      git fetch --deepen=1 origin "$event" || refuse "could not fetch the event parent."
      git cat-file -e "$parent^{commit}" 2>/dev/null || refuse "the fetched event parent is unreadable."
    fi
    selected="$parent"
  else
    # Only a commit object declaring no parent is a root. A failed read or
    # fetch must not fall through to the unrelated default branch.
    [ -n "${DEFAULT_BRANCH:-}" ] || refuse "the root event has no default branch."
    fetch_commit "$DEFAULT_BRANCH"
  fi
fi
[ -n "${GITHUB_ENV:-}" ] || refuse "GITHUB_ENV is not set."
printf 'SP_LANE_B_BASE=%s\n' "$selected" >> "$GITHUB_ENV"
