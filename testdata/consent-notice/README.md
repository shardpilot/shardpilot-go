# Consent notice wire fixture

The platform's own consent golden, byte for byte: a synthetic request with the
actor `kind` (`anon`) and the response its consent handler returns for it in
the default kind mode. The request uses workspace_key, app_key and environment_key. These pin the
three optional notice identifiers, the kind and
the existing receipt body. They are a serializer fixture, not a live endpoint
or deployment observation. Change both sides together:
`TestConsentNoticeFixtureIsThePlatformsGolden` pins the hashes below.

SHA-256:

- `request.json`: `87f337aa10ad76ffcc60424e8f3a996b9b5548df57e62a4decbd13a1e886f233`
- `response.json`: `8add729673ccd76ee0baee7352dccff01efebc6387439a56c03c523ed34f2e0b`

The SDK scene uses its real setters and POST transport with this response. Only
its freshly minted `idempotency_key` and `decided_at` are substituted with the
fixture values before comparing the complete decoded request. Retry and reload
scenes separately require the actual key, timestamp and tuple to remain equal.
