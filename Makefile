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
# Cost: about a minute.
check:
	./scripts/check_public_surface.sh
