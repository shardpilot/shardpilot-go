// Package consentpolicy verifies a consent-regime plan for ONE scoped player
// operation, before that operation's events are admitted.
//
// ⚠ WHAT THIS PACKAGE IS NOT, because every one of these would be a defect:
//
//   - NOT a consent grant. A plan says which regime applies; it never says a
//     player agreed to anything. A notice-and-objection outcome needs its own
//     distinct admission-basis representation; setting an analytics consent
//     boolean to stand in for one would record a grant nobody gave. Nothing
//     here touches the analytics consent state.
//   - NOT an ingest authorization. A valid plan is still not permission to
//     ingest anything. The caller combines this verdict with
//     the scoped actor's own retained floors, age evidence, choices and
//     objections through its existing credential/actor authority path.
//   - NOT an HTTP client. The client-facing resolver is called by the CLIENT
//     SDKs; a Go server receives the plan through the normal post-choice
//     trusted game flow. Adding a second caller here would put the
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
// ⚠ IN THIS RELEASE THAT RULE HAS NO EXCEPTION, AND THE CONSEQUENCE IS WORTH
// STATING PLAINLY RATHER THAN LEAVING A CALLER TO DISCOVER IT: Prepare returns
// a closed decision for EVERY input, including a perfectly well-formed plan.
// This build has no key to authenticate a plan with, an unauthenticated plan
// is not evidence, and so no plan is used. Decision.PlanUsed is false, every
// lane is closed, every purpose reports prohibited and every operation reports
// blocked.
//
// That is a posture, not a stub. Two weaker positions were tried here first.
// "A forged plan can only tighten" is true of a forged STRICT plan and says
// nothing about a forged permissive one. "Honour an unsigned plan only when
// its regime, crash profile and server-analytics basis are all conservative"
// then failed on an axis those three enums do not cover: prohibited_purposes
// and operation_blocks were carried through unauthenticated, so an attacker
// who could not make a plan permissive could STRIP its restrictions instead —
// and those govern transfer, age/capacity, localisation and safety, which no
// consent choice lifts. A restriction removed is permissive however strict the
// enums look.
//
// The parser, the bounds, the scope comparison and the expiry check all still
// run, and they run first, so a malformed or out-of-scope handoff is still
// named precisely for an operator. They are validation, not authentication,
// and the two are not substitutes. When a verification key is provisioned, the
// gate opens and the mapping from a verified plan to a verdict — which is
// present, tested and unreachable today — is what runs.
//
// The wire schema has exactly one copy, published with the resolver's own API
// contract; the route is POST /api/cp/v1/consent/policy.
//
// SOFT_OPT_OUT is implemented here so that a future plan parses. It is NOT
// reachable today: every row of the jurisdiction matrix is marked pending
// counsel confirmation, so the resolver's initial release has no code path
// that can emit it.
//
// ⚠ AND THE REASON THAT MATRIX IS PENDING IS A FACT ABOUT ITS PROVENANCE, not
// a scheduling delay. Owner statement of 2026-09-18 (rendering): the
// jurisdiction table — which countries are opt-in and which are opt-out — has
// NO counsel confirmation outside this repository; it was prepared as an AI
// draft (Fable 5.1). So the table is a drafting input, not a legal
// classification, and nothing in this package may treat a row as permission.
// That is why the conservative rule above is the whole design rather than a
// placeholder: STRICT is not a temporary default waiting for the table to be
// filled in, it is what an unconfirmed table can support.
package consentpolicy
