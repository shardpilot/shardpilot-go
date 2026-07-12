# Frontend checklist — console (Angular 22) and admin-console

Adapted from akovalion/paranoid-qa `references/frontend.md` (MIT), curated to
our tooling. Evidence must be obtainable with what we have:

- **console:** `node:test` + jsdom unit specs (`test:unit`) — run output is
  the artifact for logic/state checks. There is **no Playwright harness in
  the console repo**; browser-level claims need either the qa repo's separate
  Playwright smokes or a manual browser run with screenshots + DevTools
  captures. Never phrase a checklist result as "e2e green" from console
  alone.
- **admin-console:** Vitest run output (`ng test --watch=false`); same
  manual-browser rules.
- **Manual browser runs:** screenshot per claimed state (URL bar and viewport
  size visible), DevTools open (Console + Network) for error/payload claims,
  full-page screenshots for layout claims.

## Fields and forms

- Input types match purpose (email/number/password/date); number inputs:
  step, e/E/+/−, decimal separator.
- Length limits: maxlength at N−1/N/N+1; paste longer than the limit
  (truncate, not reject); emoji/surrogate pairs in counters; 10k+ chars
  without freezing; frontend limit vs backend limit agree (probe both).
- Special chars/unicode: `< > & " ' \` escaped everywhere the value is
  rendered; emoji (ZWJ); RTL and mixed; zero-width; long unbroken words.
- Paste/autofill: paste with whitespace/newlines (trim); autofill often does
  not fire the `input` event — the field looks empty to live validation;
  probe it.
- Whitespace/trim: spaces-only in a required field = empty; NBSP/tab.
- Validation and errors: timing per spec (live/blur/submit); untouched form
  not painted red; error clears once fixed; message names the field, not
  "Error 422"; server error maps back to its field; entered data preserved
  on error; double-submit protection (disable + spinner) — verify with a
  Network capture that only ONE request left.
- Dependent fields: cascading selects reset correctly; hiding a field does
  not lose its data; totals recalculated.
- disabled vs readonly: disabled not submitted and not clickable; Enter on a
  disabled form does not submit (Network tab is the proof).

## States and visuals

- Every interactive element: rest / hover / focus-visible / active /
  disabled / loading / error. Verify hover and focus empirically in the
  browser (computed styles or screenshot), not from the stylesheet.
- Loading: spinner inside the control, repeat clicks blocked, no layout
  jump; skeleton geometry matches content.
- Long/empty/large content: long text (ellipsis vs wrap per design), empty
  list vs "nothing found for filter" (different messages), 1000+ rows
  (virtualization, perf), large numbers.
- Overlays: modal/dropdown/toast above sticky header; Esc/backdrop close;
  focus returns to trigger; dropdown near viewport edge.
- Dark theme (if the surface has one): switch on the fly, no black-on-dark,
  states still distinguishable.
- Empty states and toasts: distinct from error and loading; toast auto-hide
  and manual close.
- Console (DevTools) open for the whole run: JS errors, hydration/render
  warnings, 404s for assets, CSP/CORS — capture the baseline noise first,
  then attribute by timestamp.

## Design comparison

- Texts character-for-character against the design/spec; casing,
  punctuation, ellipsis character.
- Spacing/sizes numerically (DevTools box model), not by eye; consistent
  design-system tokens (one accent color, uniform radii) — design-system
  repo is the source of truth.
- Discrepancy under an ambiguous requirement is a question to the owner,
  logged immediately (rule 3), not silently normalized.

## Responsive and browsers

- Desktop reference 1920×1080 plus a laptop width (1536×864); mobile widths
  360–430 if the surface claims mobile support; exact breakpoint boundaries.
- No horizontal scroll on body at any checked width (screenshot at each).
- 200% zoom: single column, nothing clipped.
- Browser matrix only as claimed by the ticket; note the browser+version in
  the run metadata. Anything not run is `Not tested`, listed.

## Driving the UI programmatically (jsdom/unit specs)

- Controlled inputs: set value via the native setter + dispatch
  `input`/`change`, or Angular form APIs — otherwise component state does
  not update and the spec passes vacuously.
- Assert on emitted requests/state, not only on rendered text; a spec that
  never exercises the submit path proves nothing about the payload (see
  cross-cutting: payload inspection).
- Re-query elements after re-render; stale handles pass stale assertions.
