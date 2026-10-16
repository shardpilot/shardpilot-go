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
  [ -n "${EVENT_REF:-}" ] || refuse "the new-ref event has no ref name."
  git check-ref-format "$EVENT_REF" || refuse "the event ref name is invalid."
  event="$(git rev-parse --verify "$EVENT_SHA^{commit}")" || refuse "could not read the event commit."
  git cat-file commit "$event" >/dev/null || refuse "could not read the event commit object."
  # Snapshot other published heads/tags. The just-created ref includes every
  # commit in this push, so neither it nor the tip's parent establishes a base.
  inventory="$(git ls-remote --refs --heads --tags origin)" || refuse "could not read published refs."
  published=()
  while IFS=$'\t' read -r sha ref; do
    [ -n "$sha" ] || continue
    [ "$ref" != "$EVENT_REF" ] || continue
    [[ "$sha" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] || refuse "a published ref has an invalid object id."
    published+=("$sha")
  done <<< "$inventory"
  [ "${#published[@]}" -gt 0 ] || refuse "there are no other published refs."
  shallow="$(git rev-parse --is-shallow-repository)" || refuse "could not read shallow-repository state."
  fetch_options=(--no-tags)
  case "$shallow" in
    true) fetch_options+=(--unshallow) ;;
    false) ;;
    *) refuse "could not interpret shallow-repository state." ;;
  esac
  # Fetch fixed object ids from the snapshot, never moving branch names.
  git fetch "${fetch_options[@]}" origin "$event" "${published[@]}" || refuse "could not fetch published comparison history."
  shallow="$(git rev-parse --is-shallow-repository)" || refuse "could not read fetched history state."
  [ "$shallow" = false ] || refuse "published comparison history is still shallow."
  candidates=()
  for sha in "${published[@]}"; do
    tip="$(git rev-parse --verify "$sha^{commit}")" || refuse "a published ref is not a readable commit."
    if common="$(git merge-base --all "$event" "$tip")"; then
      while IFS= read -r candidate; do candidates+=("$candidate"); done <<< "$common"
    else
      merge_status=$?
      [ "$merge_status" -eq 1 ] || refuse "could not read published ancestry."
    fi
  done
  [ "${#candidates[@]}" -gt 0 ] || refuse "the event has no published ancestor."
  # Remove older ancestors, and refuse incomparable newest candidates rather
  # than choosing a base by timestamps or unrelated default-branch history.
  selected="$(git merge-base --independent "${candidates[@]}")" || refuse "could not select a published ancestor."
  [[ -n "$selected" && "$selected" != *$'\n'* ]] || refuse "the event has multiple incomparable published ancestors."
fi
[ -n "${GITHUB_ENV:-}" ] || refuse "GITHUB_ENV is not set."
printf 'SP_LANE_B_BASE=%s\n' "$selected" >> "$GITHUB_ENV"
