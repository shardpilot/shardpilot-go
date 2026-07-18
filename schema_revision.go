package shardpilot

import (
	"errors"
	"net/http"
)

// schemaRevisionHeader is the request header through which a writer declares
// which ingest envelope schema set it was built against. It is defined for
// POST /v1/events:batch only — one header per batch request, never a body
// field (the batch body is strict-decoded server-side, so an unknown body
// field would reject the whole batch) — and must never be sent on the
// consent route or any other endpoint.
const schemaRevisionHeader = "X-ShardPilot-Schema-Revision"

// DefaultSchemaRevision identifies the ingest service's embedded envelope
// schema set this SDK release was coordinated against. It is the server's
// own digest of that set: sha256 over the lexicographically sorted
// schemas/apicurio/*.schema.json files embedded in the analytics-service
// binary (each file fed length-prefixed as "{len(name)}:{name}\n{len(content)}:"
// + content + "\n"), currently pinned to analytics-service main @ 7d118c5.
//
// This is a PUBLIC schema-set fingerprint — a content hash of served schema
// definitions, not a secret or a credential. It must be re-synced (bumped to
// the server's new digest) whenever the server's embedded schema set changes;
// going stale is by design — the server's schema-revision handshake exists to
// surface exactly that staleness when the handshake is armed. While the
// handshake runs in its default off mode the header is ignored entirely, so
// declaring a stale value costs nothing until ops deliberately arms
// enforcement.
const DefaultSchemaRevision = "sha256:e1ba01d4b76b9e73444e2edd5639281929fd89496cadc1dcc79eb68208c6a0a0"

// schemaRevisionMismatchCode is the error envelope code the ingest service
// uses when an armed (enforce-mode) handshake rejects a batch whose declared
// schema revision does not match the server's. The 409 status alone is NOT
// discriminating — other conflict codes share it — so classification must
// always check this code.
const schemaRevisionMismatchCode = "schema_revision_mismatch"

// effectiveSchemaRevision resolves the schema revision the transport declares
// on events:batch publishes: DisableSchemaRevision suppresses the header
// entirely (undeclared always passes the server handshake, in every mode),
// a non-empty SchemaRevision overrides the compiled-in default, and the zero
// config declares DefaultSchemaRevision.
func effectiveSchemaRevision(cfg Config) string {
	if cfg.DisableSchemaRevision {
		return ""
	}
	if cfg.SchemaRevision != "" {
		return cfg.SchemaRevision
	}
	return DefaultSchemaRevision
}

// isSchemaRevisionMismatch reports whether err is the ingest service's
// enforce-mode schema-revision-mismatch rejection: HTTP 409 carrying error
// code schema_revision_mismatch. Both parts are required — 409 is a shared
// status (workspace conflict codes use it too), so status alone never
// classifies.
func isSchemaRevisionMismatch(err error) bool {
	var statusErr *HTTPStatusError
	return errors.As(err, &statusErr) &&
		statusErr.StatusCode == http.StatusConflict &&
		statusErr.ErrorCode == schemaRevisionMismatchCode
}
