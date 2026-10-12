# Consent notice wire fixture

Synthetic accepted request and response bytes retained from the receipt handler.
These pin the three optional identifiers and the existing receipt body. They
are a serializer fixture, not a live endpoint or deployment observation.

SHA-256:

- `request.json`: `daf0acb703982cdf1a772b310e37b08b01f9fd40bf6e0f81dba93d3149493066`
- `response.json`: `8add729673ccd76ee0baee7352dccff01efebc6387439a56c03c523ed34f2e0b`

The SDK scene uses its real setters and POST transport with this response. Only
its freshly minted `idempotency_key` and `decided_at` are substituted with the
fixture values before comparing the complete decoded request. Retry and reload
scenes separately require the actual key, timestamp and tuple to remain equal.
