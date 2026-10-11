# Experiment fact v2 server bytes

`fact_scope_keys_client_attested.json` is copied byte-for-byte from the server
conformance fixture at revision `d4126feeb458bbea28c51e91b4983199c09b1c61`.
SHA-256: `d8056298713460b9ad9e8255fdfcb06eeb974d66f7938b9ede08bcfb1c49c021`.

The exposure fixture is the server-owned signed preimage. Tests derive an
outcome variant explicitly and treat the seal as an opaque synthetic value.
They check both producers' scope members, schema major and forwarding rather
than claiming the SDK verifies platform signatures.
