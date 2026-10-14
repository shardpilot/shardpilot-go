# Crash app selector fixtures

These exact synthetic ingest-handler request and response bytes pin the accepted
app.key selector and response decoding. They are serializer/adoption fixtures,
not a live endpoint, authentication, database or deployment observation.

SHA-256:

- request.json: 64b8b73d69579eff096006fb1400f5d0f62f8b429fa9fd8252addf6676592969
- response.json: 3a81a7608ff0212496dad3f0735bff493fa40d9ce68042ccdc319735d5eac18d

TestCrashAppKeyFixturePins verifies the whole files byte for byte. The sender
scene compares only the app object with the accepted request and consumes the
exact response. It does not claim whole SDK request equality: the SDK requires
UUIDv7 crash IDs and adds its own runtime fields, while this synthetic handler
capture deliberately uses a noncanonical crash ID with a warning in its response.
No source byte is masked or regenerated. Intrinsic crash/module/debug IDs and
credential-derived workspace/environment authority are preservation controls.
