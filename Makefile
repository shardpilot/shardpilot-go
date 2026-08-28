SHELL := /usr/bin/env bash

.PHONY: check

# RUN THIS BEFORE YOU PUSH.
#
# This is the only PUBLIC repository in the platform, and a repository publishes
# its HISTORY along with its tree. A commit carrying internal material is
# published the moment it is pushed -- not when it merges -- so a gate that runs
# only in CI catches it one step too late, and the remedy is a branch rewrite
# rather than a fix. That is exactly what #73 needed.
#
# ⚠ WHAT THIS CHECKS, EXACTLY, so it is not read as more:
#
#   * `scripts/check_public_surface.sh` reads the tree `git write-tree` would
#     write -- your INDEX. So this target refuses to run when the index or the
#     working tree differs from HEAD: otherwise "what I checked" is not "what
#     you will push", and a staged deletion could pass while the dirty commit
#     goes out.
#   * NEITHER THIS NOR CI LOOKS AT HISTORY. The script says so itself: "Deleting
#     a line does not unpublish the commit that carried it." Material added in
#     one commit and removed in a later one is absent from both trees and is
#     caught by neither. That is a real hole and it is shared with CI -- named
#     here rather than left for someone to discover after a push.
#
# ⚠ THIS TARGET DOES NOT PREDICT CI'S VERDICT, AND NO LONGER CLAIMS A CASE IS
#   "COVERED".
#
# Three successive versions of this header tried to say which pushes were
# covered. Each one was wrong, and each correction was the same shape: review
# named another input to CI's decision that a Makefile cannot see. The list was
# `HEAD:release`, then `other:release`, then `--tags`, then `push.default` and
# `remote.<name>.push`, then a fork whose default branch is not `main`. That is
# not a list converging on completeness -- it is the wrong kind of claim, and
# every round adds one more entry to it.
#
# CI's base depends on the refspec you push and on push configuration that lives
# outside this repository -- `push.default=matching` can publish branches this
# target never scanned, and `remote.<name>.push` can supply any default refspec.
# So the honest contract is the narrow one:
#
#   THIS RUNS THE SAME SCANNER CI RUNS, AGAINST THE BASES NAMED BELOW,
#   ON THE TREE YOU HAVE CHECKED OUT.
#
# A refusal here is a real finding: do not push. A pass here means those bases
# are clean -- it is not a prediction about the ref you are about to publish. If
# you are pushing anything other than this branch's own tip, name the
# destination yourself:
#
#     PUBLIC_SURFACE_BASE_REF=origin/release make check
#
# and remember it still scans THIS working tree, not the source you are pushing.
#
# CI's rule, for reference (`.github/workflows/ci.yml`):
#     pull request                -> the event's base.sha
#     push to an EXISTING ref     -> github.event.before, the pre-push tip
#     push CREATING a ref         -> the repository's default branch
#
# The default bases mirror the two right-hand cases for THIS branch: its remote
# tip if it has one, and origin's actual default branch -- resolved, not assumed
# to be `main`, because a fork or a rename puts a stale `main` at an older,
# higher baseline. Both are FETCHED first: a stale remote-tracking ref silently
# weakens the gate, and nothing in a local ref announces that it is behind.
#
# Override with the ENVIRONMENT form, never as a make variable:
#     PUBLIC_SURFACE_BASE_REF=upstream/main make check
# Make expands `$` in a command-line variable value -- `feature/foo$bar` becomes
# `feature/fooar`, and `$(value ...)` does not save it either -- so a
# make-variable override is refused rather than silently altered. An override
# naming a remote is fetched too; one that names no known remote cannot be
# refreshed here and says so.
#
# Cost: about a minute per base. Duplicate bases are scanned once.
check:
	@set -euo pipefail; \
	if [ "$(origin PUBLIC_SURFACE_BASE_REF)" = "command line" ]; then \
	  printf 'REFUSING: pass the base ref in the ENVIRONMENT, not as a make variable.\n' >&2; \
	  printf '  Make expands $$ in a command-line value, so a legal ref like\n' >&2; \
	  printf '  feature/foo$$bar would be compared as feature/fooar.\n' >&2; \
	  printf '  Use: PUBLIC_SURFACE_BASE_REF=<ref> make check\n' >&2; \
	  exit 2; \
	fi; \
	if ! git diff --quiet HEAD -- || ! git diff --cached --quiet; then \
	  printf 'REFUSING: the index or working tree differs from HEAD.\n' >&2; \
	  printf '  This gate reads the tree your INDEX would write, and a push\n' >&2; \
	  printf '  publishes your commits. Commit or stash first, so what is\n' >&2; \
	  printf '  checked is what is published.\n' >&2; \
	  exit 2; \
	fi; \
	fetch_remote() { \
	  if ! git fetch --quiet "$$1" 2>/dev/null; then \
	    printf 'REFUSING: could not fetch %s. A stale remote-tracking ref sits\n' "$$1" >&2; \
	    printf '  at an older baseline and would silently weaken this gate.\n' >&2; \
	    exit 2; \
	  fi; \
	}; \
	bases=(); \
	if [ -n "$${PUBLIC_SURFACE_BASE_REF:-}" ]; then \
	  ref="$$PUBLIC_SURFACE_BASE_REF"; \
	  remote="$${ref%%/*}"; \
	  if [ "$$remote" != "$$ref" ] && git remote | grep -qxF "$$remote"; then \
	    fetch_remote "$$remote"; \
	  else \
	    printf 'NOTE: %s names no known remote, so this gate cannot refresh it.\n' "$$ref" >&2; \
	    printf '  If it is a remote-tracking ref, fetch it yourself first; a stale\n' >&2; \
	    printf '  base sits at an older, higher baseline and weakens the check.\n' >&2; \
	  fi; \
	  bases+=("$$ref"); \
	else \
	  fetch_remote origin; \
	  branch=$$(git rev-parse --abbrev-ref HEAD); \
	  git rev-parse --verify --quiet "origin/$$branch^{commit}" >/dev/null 2>&1 \
	    && bases+=("origin/$$branch"); \
	  git remote set-head --auto origin >/dev/null 2>&1 || true; \
	  default=$$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null || true); \
	  if [ -z "$$default" ]; then \
	    printf 'NOTE: origin/HEAD does not resolve, so the default branch is not\n' >&2; \
	    printf '  known here; falling back to origin/main. On a fork or after a\n' >&2; \
	    printf '  rename that base can be the wrong one.\n' >&2; \
	    default=origin/main; \
	  fi; \
	  git rev-parse --verify --quiet "$$default^{commit}" >/dev/null 2>&1 \
	    && bases+=("$$default"); \
	fi; \
	if [ $${#bases[@]} -eq 0 ]; then \
	  printf 'REFUSING: no comparison base resolves. Fetch origin first.\n' >&2; \
	  exit 2; \
	fi; \
	seen=(); \
	for b in "$${bases[@]}"; do \
	  sha=$$(git rev-parse --verify --quiet "$$b^{commit}" 2>/dev/null || printf '%s' "$$b"); \
	  dup=; \
	  for s in "$${seen[@]:-}"; do [ "$$s" = "$$sha" ] && dup=1; done; \
	  [ -n "$$dup" ] && { printf 'public surface: %s is the same commit as an earlier base -- skipping\n' "$$b"; continue; }; \
	  seen+=("$$sha"); \
	  printf 'public surface: comparing against %s\n' "$$b"; \
	  PUBLIC_SURFACE_BASE_REF="$$b" ./scripts/check_public_surface.sh; \
	done
