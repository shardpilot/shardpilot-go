# Consent notice wire fixture

Synthetic accepted request and response bytes retained from the receipt handler.
These pin the three optional identifiers and the existing receipt body. They
are a serializer fixture, not a live endpoint or deployment observation.

SHA-256:

- `request.json`: `fb88987ab60fd3836d86428e74e7ea3674c6905e56485c9b5c37cc177d47153b`
- `response.json`: `8add729673ccd76ee0baee7352dccff01efebc6387439a56c03c523ed34f2e0b`

The SDK scene uses its real setters and POST transport with this response. Only
its freshly minted `idempotency_key` and `decided_at` are substituted with the
fixture values before comparing the complete decoded request. Retry and reload
scenes separately require the actual key, timestamp and tuple to remain equal.
