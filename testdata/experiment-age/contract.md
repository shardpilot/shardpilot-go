# Experiment age assignment contract

Captured on 2026-09-28 from the real in-memory HTTP assignment handler at
server revision `2b7012d1f761ef37e1034c0ee83a45ecfc0b3154`, using synthetic data.
No database, container or deployed service was involved. These files are
responses from that handler, not SDK-authored response examples.

Source anchors:

- `experiment_age_eligibility_routes_test.go:36`,
  `TestExperimentAgeRefusalRoutes`: omitted, `unknown`, and `under_threshold`.
- `experiment_fact_apply_routes_test.go:168`, `newExposureRouteFixture`, and
  `experiment_fact_apply_routes_test.go:219`, its adult assignment fetch.
- `experiments.go:284`, `experimentSubjectEligible`: the exact admission rule.
- `local_runtime_routes.go:1429`: the handler's JSON serialization.

A temporary capture test used that same route fixture, issued four GETs to
`/api/v1/runtime/experiments/assignment`, and saved each HTTP 200 body directly.
Each request used `app_key=exposure-app`, `environment_key=develop`,
`experiment_key=exposure-banner`, and the fixture's synthetic subject.
It varied only the presence/value of `age_band`.

## Public projection

The raw bodies are retained privately as capture receipts. The committed files
remove **only** the JSON member `/boundary/analytics_fact_ownership`, including
its preceding comma, because its value names an internal service. They are
explicit projections, not byte-identical copies of the complete responses.
All other bytes, including assignment, reason, version, subject-fact key and
serving state, are unchanged. No scanner exception or encoded private value is
used. The SHA-256 hashes below distinguish the raw receipts from these files.

| Fixture | Raw SHA-256 | Projected SHA-256 |
| --- | --- | --- |
| `undeclared.json` | `d187bf39f9344d2bf4a63bcc5ef1c9c069c276c2db78a193b8e04c12e0a3cadb` | `d870fcfacf2bee1cd17dc1a501cbcd0edf90a01e9f0823c067b5171a7aa5e2e8` |
| `unknown.json` | `d187bf39f9344d2bf4a63bcc5ef1c9c069c276c2db78a193b8e04c12e0a3cadb` | `d870fcfacf2bee1cd17dc1a501cbcd0edf90a01e9f0823c067b5171a7aa5e2e8` |
| `under_threshold.json` | `2d6912fd03b63eabdb0b5cce22e078b8a8a420028756a7466c97783c7bd0b8ab` | `b5dd88dc6b4bea663e5612b5ca95a874c29995439eb85ce6b8381a0b67192761` |
| `adult.json` | `a2ddd00c20c180b39dcab3bc9ed82abf4de2b55e46d2a4749264321665716587` | `84669986d3c1ead97db9fc23587c41324928cfa66d3670923960e7aa81c94052` |

## Behavior

- The game can declare exactly `unknown`, `under_threshold`, or `adult` through
  the typed SDK API. A declaration is not age verification or analytics consent.
- Client-id assignment requires at least one age declaration. Every present
  spelling (`age_band`, `custom_attribute_age_band`) must be exactly `adult`.
  Missing, blank, unknown, non-adult, differently cased, or padded declarations
  refuse admission. One adult spelling cannot override a refusal in the other.
- A valid adult declaration permits evaluation; it does not guarantee assignment
  or bypass any other gate. The captured full-traffic fixture assigns `control`.
- `age_ineligible` is an authoritative not-assigned reason: return it unchanged,
  drop memory and durable assignment caches, and do not retry it as a transient.
- Unknown future reasons remain malformed; the test's `future_age_reason` is an
  explicitly derived negative control, not a captured server verdict.
- Synthetic-subject experiments bypass this age gate; the SDK uses client ids.
