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
# ⚠ AND A PLAIN `git fetch <remote>` DOES NOT GUARANTEE THE NAMED REF MOVED. The
# remote's configured `remote.<name>.fetch` refspec decides what a bare fetch
# updates: a remote configured to fetch only `release` returns success while
# `upstream/main` stays at its old commit, and the gate then compares against a
# stale, higher baseline. So the named ref is ALSO fetched by explicit refspec,
# and the target refuses if it does not resolve afterwards
# (shardpilot/shardpilot-go#77 review, reproduced by the reviewer).
#
# ⚠ AND DISCOVERY ASKS THE REMOTE, NOT THE LOCAL REPOSITORY. Six successive
# findings were all one mistake: reimplementing git's ref resolution here and
# keeping a list of its facts. `git remote set-head --auto` FAILS when the
# tracking ref does not exist yet (a narrow fetch mapping, a default branch
# called `trunk`), and the `|| true` hid it; a short name like `origin/main`
# resolves to `refs/tags/origin/main` FIRST if such a tag exists; a fully
# qualified `refs/remotes/origin/release` matched no remote prefix; a remote
# whose name starts with `-` parsed as a fetch option. So:
#
#   * the default branch comes from `git ls-remote --symref origin HEAD` -- the
#     remote's own answer -- and its absence is a REFUSAL, not a guess at `main`;
#   * a failed explicit fetch is separated from an absent branch by asking
#     `git ls-remote --heads`, so a transient error can no longer masquerade as
#     "this branch does not exist" and silently drop a base;
#   * every base is passed to the scanner as a canonical `refs/remotes/...`
#     path, so no tag can shadow it;
#   * `--` terminates options before every remote name;
#   * GIT_NO_REPLACE_OBJECTS=1 is exported, because `refs/replace/*` is honoured
#     by the diffs AND by `git write-tree` while a push publishes the ORIGINAL
#     object and does not transfer the replacement -- so the gate could scan a
#     clean replacement and publish the real one.
#
# Cost: one `ls-remote` round trip in addition to the fetches.
#
# ONE PATH, NOT TWO. The first fix protected only the override arm and left the
# two automatically selected bases on the bare-fetch path -- the same defect,
# fixed in one of the two places it lived. Every base now goes through
# `refresh_base`, which also matches the LONGEST configured remote prefix (git
# accepts a remote named `foo/bar`, and splitting at the first slash mistook it
# for `foo`) and treats a failed explicit fetch as a REFUSAL for a required base
# rather than falling back on whatever local tracking ref survived a deletion.
#
# Cost: about a minute per base. Duplicate bases are scanned once.
check:
	@set -euo pipefail; \
	export GIT_NO_REPLACE_OBJECTS=1; \
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
	  if ! git fetch --quiet -- "$$1" 2>/dev/null; then \
	    printf 'REFUSING: could not fetch %s. A stale remote-tracking ref sits\n' "$$1" >&2; \
	    printf '  at an older baseline and would silently weaken this gate.\n' >&2; \
	    exit 2; \
	  fi; \
	}; \
	remote_of() { \
	  cand="$${1#refs/remotes/}"; \
	  best=; \
	  for r in $$(git remote); do \
	    case "$$cand" in "$$r"/*) [ $${#r} -gt $${#best} ] && best="$$r";; esac; \
	  done; \
	  printf '%s' "$$best"; \
	}; \
	refresh_base() { \
	  ref="$$1"; required="$$2"; \
	  remote=$$(remote_of "$$ref"); \
	  if [ -z "$$remote" ]; then \
	    printf 'NOTE: %s names no known remote, so this gate cannot refresh it.\n' "$$ref" >&2; \
	    printf '  If it is a remote-tracking ref, fetch it yourself first; a stale\n' >&2; \
	    printf '  base sits at an older, higher baseline and weakens the check.\n' >&2; \
	    git rev-parse --verify --quiet "$$ref^{commit}" >/dev/null 2>&1 && bases+=("$$ref"); \
	    return 0; \
	  fi; \
	  branch="$${ref#refs/remotes/}"; branch="$${branch#$$remote/}"; \
	  canon="refs/remotes/$$remote/$$branch"; \
	  fetch_remote "$$remote"; \
	  before=$$(git rev-parse --verify --quiet "$$canon^{commit}" 2>/dev/null || true); \
	  if git fetch --quiet -- "$$remote" \
	       "+refs/heads/$$branch:$$canon" 2>/dev/null; then \
	    : ; \
	  else \
	    if git ls-remote --quiet --exit-code --heads -- "$$remote" \
	         "refs/heads/$$branch" >/dev/null 2>&1; then \
	      printf 'REFUSING: %s exists on %s but could not be fetched.\n' "$$branch" "$$remote" >&2; \
	      printf '  That is a failure, not an absence -- a transient network error or a\n' >&2; \
	      printf '  locked ref -- and treating it as absence would silently drop a base.\n' >&2; \
	      exit 2; \
	    fi; \
	    if [ "$$required" = required ]; then \
	      printf 'REFUSING: %s is not on %s.\n' "$$branch" "$$remote" >&2; \
	      printf '  A local tracking ref survives the branch being deleted, so proceeding\n' >&2; \
	      printf '  would compare against a ref the remote no longer has.\n' >&2; \
	      exit 2; \
	    fi; \
	    printf 'NOTE: %s has no counterpart on %s; dropping it as a base.\n' "$$branch" "$$remote" >&2; \
	    return 0; \
	  fi; \
	  after=$$(git rev-parse --verify --quiet "$$canon^{commit}" 2>/dev/null || true); \
	  if [ -z "$$after" ]; then \
	    printf 'REFUSING: %s does not resolve after fetching %s.\n' "$$canon" "$$remote" >&2; \
	    exit 2; \
	  fi; \
	  [ "$$before" != "$$after" ] && printf 'public surface: %s refreshed %s -> %s\n' \
	    "$$canon" "$${before:-absent}" "$$after"; \
	  bases+=("$$canon"); \
	}; \
	bases=(); \
	if [ -n "$${PUBLIC_SURFACE_BASE_REF:-}" ]; then \
	  refresh_base "$$PUBLIC_SURFACE_BASE_REF" required; \
	else \
	  fetch_remote origin; \
	  branch=$$(git rev-parse --abbrev-ref HEAD); \
	  refresh_base "origin/$$branch" optional; \
	  default=$$(git ls-remote --symref -- origin HEAD 2>/dev/null \
	    | awk '$$1=="ref:" && $$3=="HEAD" {sub("refs/heads/","",$$2); print $$2; exit}'); \
	  if [ -z "$$default" ]; then \
	    printf 'REFUSING: origin did not advertise a default branch (HEAD symref).\n' >&2; \
	    printf '  Guessing a name here is how a fork or a renamed default silently\n' >&2; \
	    printf '  becomes the wrong, higher baseline.\n' >&2; \
	    exit 2; \
	  fi; \
	  refresh_base "origin/$$default" required; \
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
