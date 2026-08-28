SHELL := /usr/bin/env bash

.PHONY: check

# RUN THIS BEFORE YOU PUSH.
#
# This is the only PUBLIC repository in the platform, and a repository publishes
# its HISTORY along with its tree. A commit carrying internal material is
# published the moment it is pushed -- not when it merges -- so a check that
# runs only in CI catches it one step too late, and the remedy is a branch
# rewrite rather than a fix.
#
# That is not hypothetical: a capture record naming another service's source
# paths, its environment-variable names, ADR numbers and an internal plan item
# was committed here and pushed. CI refused it, correctly, and the branch had to
# be rewritten (#73).
#
# The rule the check enforces: internal records do not belong in this tree. A
# runnable example belongs here; the internal reading of what it found does not.
#
# ⚠ AND IT MUST BE THE SAME VERDICT AS CI'S — WHICH IS NOT ALWAYS main.
# With PUBLIC_SURFACE_BASE_REF unset the script prints "(baseline-vs-target check
# skipped)" and takes the skip path, so a contributor who raises a lane-B count
# AND regenerates the baseline in the same commit passes here and is refused only
# after the push -- the one thing this target exists to prevent.
#
# CI's rule, in its own words (.github/workflows/ci.yml):
#     pull request                -> the event's base.sha
#     push to an EXISTING ref     -> github.event.before, the pre-push tip
#     push CREATING a ref         -> the default branch
#
# The middle case is the normal one, and defaulting to main gets it wrong in the
# direction that matters: a branch that LOWERED a lane-B count can restore it and
# regenerate the baseline up to main's higher value, pass here, and be refused by
# CI against its own lower tip -- after publication. So the default is this
# branch's remote-tracking tip when it has one, and main only when it does not,
# which is the local shape of the same three rules.
#
# Overridable for a fork or an unusual base:
#     make check PUBLIC_SURFACE_BASE_REF=upstream/main
# Passed through the ENVIRONMENT, never interpolated into the recipe: `&`, `;`
# and `|` are legal in a ref name (`git check-ref-format` accepts
# `refs/heads/feature/foo&bar`), and an unquoted expansion made the shell
# background the assignment and try to run `bar` -- the gate exiting 0 without
# checking anything.
#
# An unresolvable ref is refused by the script rather than skipped: "I could not
# compare" must not read like "nothing to compare".
#
# Cost: about a minute.
PUBLIC_SURFACE_BASE_REF ?=
export PUBLIC_SURFACE_BASE_REF

check:
	@set -eu; \
	if [ -z "$${PUBLIC_SURFACE_BASE_REF:-}" ]; then \
	  branch=$$(git rev-parse --abbrev-ref HEAD); \
	  if git rev-parse --verify --quiet "origin/$$branch^{commit}" >/dev/null 2>&1; then \
	    PUBLIC_SURFACE_BASE_REF="origin/$$branch"; \
	  else \
	    PUBLIC_SURFACE_BASE_REF="origin/main"; \
	  fi; \
	  export PUBLIC_SURFACE_BASE_REF; \
	fi; \
	printf 'public surface: comparing against %s\n' "$$PUBLIC_SURFACE_BASE_REF"; \
	./scripts/check_public_surface.sh
