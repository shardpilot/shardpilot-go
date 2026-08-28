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
# ⚠ WHAT IT DOES NOT COVER, AND THIS CLAIM HAS BEEN WRONG TWICE.
#
# Earlier versions said this target was "at least as strong as CI whichever rule
# applies". It is not, and could not be: CI compares against the tip of the ref
# you actually push, and a Makefile cannot know your refspec.
#
#     git push                              -> covered
#     git push origin HEAD:release          -> NOT covered: CI compares against
#                                              release's tip, which is not in the
#                                              base set below
#     git push origin other-branch:release  -> NOT covered: a different SOURCE
#                                              tree is published, not this HEAD
#     git push --tags                       -> NOT covered: tags may point at
#                                              arbitrary commits
#
# For anything but a plain push of the checked-out branch, run the gate yourself
# against the destination's tip:
#
#     PUBLIC_SURFACE_BASE_REF=origin/release make check
#
# and remember it still scans THIS working tree, not the source you are pushing.
# Stating the gap is the point: a gate that overstates its coverage gives
# grounds not to check, which is the failure this file exists to remove.
#
# CI's rule, for reference:
#     pull request                -> the event's base.sha
#     push to an EXISTING ref     -> github.event.before, the pre-push tip
#     push CREATING a ref         -> the default branch
#
# The bases below cover the plain case: this branch's remote tip if it has one,
# and the default branch. Both are FETCHED first -- a stale origin/* sits at an
# older, higher baseline, so comparing against it silently weakens the gate, and
# nothing in a local ref announces that it is behind.
#
# Override with the ENVIRONMENT form, never as a make variable:
#     PUBLIC_SURFACE_BASE_REF=upstream/main make check
# Make expands `$` in a command-line variable value -- `feature/foo$bar` becomes
# `feature/fooar`, and `$(value ...)` does not save it either -- so a
# make-variable override is refused rather than silently altered.
#
# Cost: about a minute per base.
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
	if ! git fetch --quiet origin 2>/dev/null; then \
	  printf 'REFUSING: could not fetch origin. A stale remote-tracking ref sits\n' >&2; \
	  printf '  at an older baseline and would silently weaken this gate.\n' >&2; \
	  exit 2; \
	fi; \
	bases=(); \
	if [ -n "$${PUBLIC_SURFACE_BASE_REF:-}" ]; then \
	  bases+=("$$PUBLIC_SURFACE_BASE_REF"); \
	else \
	  branch=$$(git rev-parse --abbrev-ref HEAD); \
	  git rev-parse --verify --quiet "origin/$$branch^{commit}" >/dev/null 2>&1 \
	    && bases+=("origin/$$branch"); \
	  git rev-parse --verify --quiet "origin/main^{commit}" >/dev/null 2>&1 \
	    && bases+=("origin/main"); \
	fi; \
	if [ $${#bases[@]} -eq 0 ]; then \
	  printf 'REFUSING: no comparison base resolves. Fetch origin first.\n' >&2; \
	  exit 2; \
	fi; \
	for b in "$${bases[@]}"; do \
	  printf 'public surface: comparing against %s\n' "$$b"; \
	  PUBLIC_SURFACE_BASE_REF="$$b" ./scripts/check_public_surface.sh; \
	done
