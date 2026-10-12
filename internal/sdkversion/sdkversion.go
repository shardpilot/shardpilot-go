// Package sdkversion holds this SDK's own release version and the identity
// header every request carries.
package sdkversion

// Version is the SDK's release version: the tag the release is cut from,
// without its leading "v" and without SemVer build metadata. The release PR
// bumps it together with the README, CHANGELOG and integration-skill claims;
// scripts/check_release_consistency.sh fails when they disagree.
const Version = "0.8.0-alpha"

// HeaderName is the SDK identity request header.
const HeaderName = "X-ShardPilot-SDK"

// HeaderValue is the identity header's value, "<sdk>/<version>".
const HeaderValue = "go/" + Version
