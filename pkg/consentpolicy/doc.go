// Package consentpolicy verifies a consent-regime plan for ONE scoped player
// operation, before that operation's events are admitted.
//
// ⚠ WHAT THIS PACKAGE IS NOT, because every one of these would be a defect:
//
//   - NOT a consent grant. A plan says which regime applies; it never says a
//     player agreed to anything. ADR-0337 §4.3 is explicit that a SOFT
//     non-objection needs a new, distinct admission-basis representation and
//     that today's SetConsent(true) must not be used to pretend the player
//     opted in. Nothing here touches the analytics consent state.
//   - NOT an ingest authorization. "A valid policy plan is still not
//     authorization to ingest" (ADR §3). The caller combines this verdict with
//     the scoped actor's own retained floors, age evidence, choices and
//     objections through its existing credential/actor authority path.
//   - NOT an HTTP client. The client-facing resolver is called by the CLIENT
//     SDKs; a Go server receives the plan through the normal post-choice
//     trusted game flow (ADR §5). Adding a second caller here would put the
//     game server's own connection where the player's belongs.
//   - NOT a geolocator. It never reads an IP, a forwarded header or a store
//     region, and it does not require a country to be present in a valid plan:
//     the resolver's initial release performs no geolocation at all and
//     reports its signals as unavailable with a reason.
//   - NOT process-wide. One plan is verified for one actor's operation. A
//     cached verdict served to a second actor would authorize a different
//     player than the events carry.
//
// The conservative rule, which is the whole point of the package: a plan that
// is missing, unparseable, out of scope, expired, unverifiable, or carrying
// anything outside its bounded vocabulary resolves to STRICT with optional
// processing closed — never to a partial permissive result.
//
// The wire schema is the control plane's, and there is one copy of it:
// openapi/control-plane.v1.yaml, path /api/cp/v1/consent/policy. The design is
// ADR-0337 (consent-regime resolution by player region and store region).
//
// SOFT_OPT_OUT is implemented here so that a future plan parses. It is NOT
// reachable today: every row of the jurisdiction matrix carries
// activation=COUNSEL_PENDING, so the resolver's initial release has no code
// path that can emit it (legal/consent-regimes-by-jurisdiction.md).
package consentpolicy
