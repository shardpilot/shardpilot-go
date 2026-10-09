# Consent-plan implementation notes

This helper remains restrictive: no plan is authenticated or used in this release.
Moving it under `internal/` changes the supported import boundary only.

Before a verifier can use authenticated plans, it must also reject JSON
escape-level unpaired surrogates and out-of-range RFC 3339 zone offsets. The
current `encoding/json` and `time.Parse` paths normalize those inputs rather than
refusing them. Neither changes a decision while every plan remains unused.
