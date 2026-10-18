# Contributor instructions

Keep the SDK dependency-free and compatible with the Go version in `go.mod`.
Run `go test ./...`, `go vet ./...`, and the current CI toolchain's `gofmt`
check when changing Go code.

For retry pacing changes, run `TestRestoredDeadlineAfterWithdrawal`, including
its queued-before-withdrawal cases and retained-work controls. Preserve the
distinction between a restored spool deadline and one armed by a publish in
the current launch. Worker pacing state belongs to the worker goroutine.

Document behavior changes in `README.md` and `CHANGELOG.md`. Keep repository
content and pull request discussions suitable for public SDK consumers.
