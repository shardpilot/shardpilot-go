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

// schemaRevisionMismatchCode is the error envelope code the ingest service
// uses when an armed (enforce-mode) handshake rejects a batch whose declared
// schema revision does not match the server's. The 409 status alone is NOT
// discriminating — other conflict codes share it — so classification must
// always check this code.
const schemaRevisionMismatchCode = "schema_revision_mismatch"

// effectiveSchemaRevision returns only the writer's explicit declaration.
func effectiveSchemaRevision(cfg Config) string {
	return cfg.SchemaRevision
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
