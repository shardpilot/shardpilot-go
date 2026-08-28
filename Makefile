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
# ⚠ AND IT MUST BE THE SAME VERDICT AS CI'S. With PUBLIC_SURFACE_BASE_REF unset
# the script prints "(baseline-vs-target check skipped)" and takes the skip path,
# so a contributor who raises a lane-B count AND regenerates the baseline in the
# same commit passes here and is refused only after the push -- which is the one
# thing this target exists to prevent. CI derives the ref from the push event and
# falls back to the default branch; `origin/main` is that fallback locally.
#
# Overridable, because a fork or a long-lived branch has a different base:
#   make check PUBLIC_SURFACE_BASE_REF=upstream/main
# An unresolvable ref is refused by the script rather than skipped, which is the
# behaviour we want: "I could not compare" must not read like "nothing to
# compare".
PUBLIC_SURFACE_BASE_REF ?= origin/main

# Cost: about a minute.
check:
	PUBLIC_SURFACE_BASE_REF=$(PUBLIC_SURFACE_BASE_REF) ./scripts/check_public_surface.sh
