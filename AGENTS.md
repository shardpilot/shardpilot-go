# Contributor instructions

Keep the SDK dependency-free and compatible with the Go version in `go.mod`.
Run `go test ./...`, `go vet ./...`, and the current CI toolchain's `gofmt`
check when changing Go code.

For retry pacing changes, run `TestRestoredDeadlineAfterWithdrawal`, including
its queued-before-withdrawal cases and retained-work controls. Preserve the
distinction between a restored spool deadline and one armed by a publish in
the current launch. Worker pacing state belongs to the worker goroutine.

HTTP fixtures that block a dispatch leg must verify the request held there.
Persisting a fresh failed batch does not schedule it for startup resend;
restore the persisted record when a test needs that ordering. Keep the
consent-dispatch delivery and drop-count assertions intact.

Document behavior and verification changes in `README.md` and `CHANGELOG.md`.
Keep repository content and pull request discussions suitable for public SDK
consumers.
