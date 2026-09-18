// Command symbolication-rehearsal-sender sends the controlled crashes that the
// symbolication rehearsal's manifest describes, through the crash SDK's own
// native-address form, and prints per crash id the frame or status the owner
// reads back afterwards.
//
// It uploads no symbols and it resolves nothing. An accepted crash is not a
// symbolicated crash: every row here needs the manifest's upload command to
// have been run first, and the resolved frame is read back from the product,
// not from any reply this sender sees.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/shardpilot/shardpilot-go/internal/redact"
	"github.com/shardpilot/shardpilot-go/pkg/crash"
)

func main() {
	if len(os.Args) > 1 {
		fmt.Fprintln(os.Stderr, "positional arguments are not accepted; configuration comes from the environment")
		os.Exit(2)
	}
	os.Exit(run(os.Getenv, os.Stdout, http.DefaultTransport))
}

const (
	bodyLimit = 64 << 10
	// The module id the frame names, and the extent the rehearsal's own handler
	// test declares. The frame naming its module by id is load-bearing for two
	// of the twins, so it is not incidental.
	moduleID   = "rehearsal"
	moduleSize = "0x100000"
)

// ---------------------------------------------------------------------------
// The manifest, as cmd/symbolication-rehearsal writes it. Its producer lives in
// another repository (crash-symbolicator internal/symbols/symbolstest), so the
// field and twin names are PINNED here: an unrecognised twin fails the run
// naming it rather than being skipped, because a skipped row is a crash the
// owner waits for on run day and never receives.
// ---------------------------------------------------------------------------

type expectedFrame struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
}

type negativeTwin struct {
	Change       string `json:"change"`
	Status       string `json:"status"`
	HTTPStatus   int    `json:"http_status"`
	Symbolicated bool   `json:"symbolicated"`
	Why          string `json:"why"`
}

type manifestEntry struct {
	Platform           string         `json:"platform"`
	Format             string         `json:"format"`
	ModuleName         string         `json:"module_name"`
	DebugID            string         `json:"debug_id"`
	LoadAddress        string         `json:"load_address"`
	InstructionAddress string         `json:"instruction_address"`
	Symbolicated       bool           `json:"symbolication_exercised"`
	Expected           *expectedFrame `json:"expected_frame"`
	Note               string         `json:"note"`
	NegativeTwins      []negativeTwin `json:"negative_twins"`
}

// identity is the four fields a crash needs from a row: the module's name and
// debug id, its base, and the instruction address that must resolve inside it.
func (e manifestEntry) identity() []string {
	return []string{e.ModuleName, e.DebugID, e.LoadAddress, e.InstructionAddress}
}

// identified is a row this sender can send; blank is a row with NO artefact at
// all. Anything between the two is a partly described crash, and sending or
// skipping it would both be guesses — see the caller.
func (e manifestEntry) identified() bool {
	for _, value := range e.identity() {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}
	return true
}

func (e manifestEntry) blank() bool {
	for _, value := range e.identity() {
		if strings.TrimSpace(value) != "" {
			return false
		}
	}
	return true
}

// frameComplete reports whether an expected frame can actually be compared on
// run day: a function, a file and a line, not an empty object.
func (f *expectedFrame) frameComplete() bool {
	return f != nil && strings.TrimSpace(f.Function) != "" && strings.TrimSpace(f.File) != "" && f.Line > 0
}

type manifest struct {
	Producer string          `json:"producer"`
	Entries  []manifestEntry `json:"entries"`
}

// The twin names from the producer's own constants.
const (
	twinWrongDebugID  = "the crash body carries a debug id no uploaded symbol has"
	twinWrongBase     = "the crash body declares the wrong load_address"
	twinNoModuleRange = "the frame's address falls in no declared module's range (two modules declared, so there is no single-module default)"
	twinNoBaseAtAll   = "the module declares no load_address and no base_address"
)

// twin is one way the manifest can break a pair: the slug its case name uses,
// the mutation that spells it on the SDK event, and — for the shapes the SDK
// refuses before any request — the validation that refuses it. A refused twin
// is still ATTEMPTED here: the refusal is the measurement, and an SDK that
// stopped refusing would fail this run instead of quietly sending a body the
// manifest says is invalid.
type twin struct {
	slug    string
	mutate  func(*crash.Event)
	refusal string
}

var twinsByChange = map[string]twin{
	twinWrongDebugID: {slug: "wrong-debug-id", mutate: func(event *crash.Event) {
		event.Modules[0].DebugID = "0123456789ABCDEF0123456789ABCDEF0"
	}},
	twinWrongBase: {slug: "wrong-base", mutate: func(event *crash.Event) {
		event.Modules[0].LoadAddress = "0x800000"
	}},
	twinNoBaseAtAll: {slug: "no-base-at-all", mutate: func(event *crash.Event) {
		event.Modules[0].LoadAddress = ""
		event.Modules[0].BaseAddress = ""
	}, refusal: "pkg/crash/event.go:211-213 requires a load_address or a base_address on every module"},
	// Two declared ranges, neither containing the address, and a frame naming a
	// module id that is not among them. The SDK requires that selector to be
	// NONEMPTY, not to resolve (pkg/crash/event.go validateEvent), and an
	// unknown id falls through the server's module match to range containment,
	// which finds nothing and does not fall back to a lone module — so the
	// twin's own status stands.
	twinNoModuleRange: {slug: "no-module-range", mutate: func(event *crash.Event) {
		event.Modules[0].LoadAddress = "0x400000"
		event.Modules[0].EndAddress = "0x500000"
		event.Modules = append(event.Modules, crash.Module{
			ID: "other", Name: "libother.so", Platform: event.Platform,
			DebugID: "0123456789ABCDEF0123456789ABCDEF0", LoadAddress: "0x600000", EndAddress: "0x700000",
		})
		event.Threads[0].Frames[0].ModuleID = "missing"
		event.Threads[0].Frames[0].InstructionAddress = "0x900000"
	}},
}

// controlledCrash is the crash the rehearsal describes: the row's module identity and
// base, one crashed thread, one native frame carrying one instruction address.
// CrashID is left empty on purpose — the SDK mints a UUIDv7 (event.go:172
// refuses anything else), and this sender reads the id back off the wire.
func controlledCrash(entry manifestEntry, mutate func(*crash.Event)) crash.Event {
	event := crash.Event{
		OccurredAt: time.Now().UTC(),
		Platform:   entry.Platform,
		Exception:  crash.ExceptionInfo{Type: "SIGSEGV", Reason: "synthetic rehearsal crash; no process crashed", CrashedThreadID: "main"},
		Modules: []crash.Module{{
			ID: moduleID, Name: entry.ModuleName, Platform: entry.Platform,
			DebugID: entry.DebugID, LoadAddress: entry.LoadAddress, Size: moduleSize,
		}},
		Threads: []crash.Thread{{ID: "main", Crashed: true, Frames: []crash.Frame{{
			Index: 0, ModuleID: moduleID, InstructionAddress: entry.InstructionAddress,
		}}}},
	}
	if mutate != nil {
		mutate(&event)
	}
	return event
}

// ---------------------------------------------------------------------------
// Evidence
// ---------------------------------------------------------------------------

type evidenceWriter struct {
	io.Writer
	err error
}

func (w *evidenceWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.Writer.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

type exchange struct {
	Case         string  `json:"case"`
	CrashID      string  `json:"crash_id"`
	Method       string  `json:"method"`
	Route        string  `json:"route"`
	Status       int     `json:"status"`
	ResponseBody string  `json:"response_body"`
	RequestID    string  `json:"request_id"`
	LatencyMS    float64 `json:"latency_ms"`
	Error        string  `json:"error,omitempty"`
}

// Each case owns a witness with a ONE-request budget, so an SDK retry cannot
// turn a rehearsal probe into a second crash under a new id.
type witness struct {
	mu        sync.Mutex
	base      http.RoundTripper
	out       io.Writer
	redact    *redact.Redactor
	name      string
	attempted bool
	records   []exchange
}

func (w *witness) RoundTrip(req *http.Request) (*http.Response, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.attempted {
		return nil, fmt.Errorf("case request budget exhausted; no retry sent")
	}
	w.attempted = true
	body, err := io.ReadAll(io.LimitReader(req.Body, bodyLimit+1))
	if err != nil || len(body) > bodyLimit {
		return nil, fmt.Errorf("cannot record bounded request")
	}
	_ = req.Body.Close()
	req = req.Clone(req.Context())
	req.Body = io.NopCloser(bytes.NewReader(body))
	var sent struct {
		CrashID string `json:"crash_id"`
	}
	_ = json.Unmarshal(body, &sent)
	e := exchange{Case: w.name, CrashID: sent.CrashID, Method: req.Method, Route: req.URL.Path}
	start := time.Now()
	resp, err := w.base.RoundTrip(req)
	if resp != nil {
		e.Status, e.RequestID = resp.StatusCode, resp.Header.Get("X-Request-Id")
		if resp.Body == nil {
			err = fmt.Errorf("response has no body")
		} else {
			var data []byte
			data, err = io.ReadAll(io.LimitReader(resp.Body, bodyLimit+1))
			_ = resp.Body.Close()
			if len(data) > bodyLimit {
				data = data[:bodyLimit]
				err = fmt.Errorf("response exceeds evidence limit; body truncated")
			}
			e.ResponseBody = string(data)
			resp.Body = io.NopCloser(bytes.NewReader(data))
		}
	}
	e.LatencyMS = float64(time.Since(start).Microseconds()) / 1000
	if err != nil {
		e.Error = err.Error()
	}
	w.records = append(w.records, e)
	// The wire bytes stay intact for the verdict; only the printed copy is redacted.
	printed := e
	printed.ResponseBody = w.redact.Replace(printed.ResponseBody)
	printed.RequestID = w.redact.Replace(printed.RequestID)
	printed.Error = w.redact.Replace(printed.Error)
	if writeErr := json.NewEncoder(w.out).Encode(printed); writeErr != nil {
		return nil, fmt.Errorf("write evidence: %w", writeErr)
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// acknowledged applies the same reading the protocol witness applies: exactly
// one observed exchange, the expected status, the submitted id echoed back, a
// fingerprint with non-whitespace content, and no suppression. None of it says
// the frame resolved.
func (w *witness) acknowledged(expectStatus int) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.records) != 1 {
		return "", fmt.Errorf("expected one observed request, got %d", len(w.records))
	}
	e := w.records[0]
	if e.Error != "" {
		return e.CrashID, fmt.Errorf("incomplete HTTP exchange: %s", e.Error)
	}
	if e.Status != expectStatus {
		return e.CrashID, fmt.Errorf("expected HTTP %d, got %d", expectStatus, e.Status)
	}
	if expectStatus != http.StatusAccepted {
		return e.CrashID, nil
	}
	var got struct {
		ID          string `json:"crash_id"`
		Fingerprint string `json:"fingerprint"`
		Suppressed  bool   `json:"suppressed"`
	}
	if json.Unmarshal([]byte(e.ResponseBody), &got) != nil || e.CrashID == "" || got.ID != e.CrashID ||
		strings.TrimSpace(got.Fingerprint) == "" || got.Suppressed {
		return e.CrashID, fmt.Errorf("crash acknowledgement missing, mismatched or suppressed")
	}
	return e.CrashID, nil
}

func (w *witness) requested() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.records) > 0
}

type readback struct {
	Case               string         `json:"case"`
	SentCase           string         `json:"sent_case"`
	Platform           string         `json:"platform"`
	Format             string         `json:"format"`
	CrashID            string         `json:"crash_id"`
	ExpectSymbolicated bool           `json:"expect_symbolicated"`
	ExpectStatus       string         `json:"expect_status"`
	ExpectFrame        *expectedFrame `json:"expect_frame,omitempty"`
	Why                string         `json:"why,omitempty"`
}

// verdict is an absent row or a failed expectation: the two things that are
// neither an exchange nor a readback.
type verdict struct {
	Case     string `json:"case"`
	Subject  string `json:"subject"`
	Platform string `json:"platform"`
	Reason   string `json:"reason"`
	SDKSite  string `json:"sdk_site,omitempty"`
	SDKError string `json:"sdk_error,omitempty"`
}

type summary struct {
	Case         string `json:"case"`
	Sent         int    `json:"sent"`
	NotExercised int    `json:"not_exercised"`
	Rows         int    `json:"manifest_rows"`
	Contract     bool   `json:"contract_match"`
	ExitCode     int    `json:"exit_code"`
	Residual     string `json:"residual"`
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

// The residual every row carries, because HTTP acceptance and symbolication are
// different claims and only the first one is visible here.
const residual = "an accepted crash is not a resolved frame: each row needs the manifest's upload command to have run first, and the frame or status is read back from the product, never from these replies"

func run(getenv func(string) string, out io.Writer, transport http.RoundTripper) int {
	log := &evidenceWriter{Writer: out}
	out = log
	// The CRASH plane only. The analytics four the protocol witness needs are
	// deliberately absent: requiring them would be a false precondition for a
	// sender that never touches the analytics door.
	names := []string{"SHARDPILOT_CRASH_INGEST_URL", "SHARDPILOT_API_KEY", "SHARDPILOT_APP_ID", "SHARDPILOT_ANONYMOUS_ID", "SHARDPILOT_REHEARSAL_MANIFEST"}
	values := make(map[string]string)
	for _, name := range names {
		values[name] = strings.TrimSpace(getenv(name))
		if values[name] == "" {
			fmt.Fprintf(out, "configuration: %s is required; exit 2\n", name)
			return 2
		}
	}
	u, err := url.Parse(values["SHARDPILOT_CRASH_INGEST_URL"])
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") || (u.Scheme != "https" && u.Scheme != "http") {
		fmt.Fprintln(out, "configuration: SHARDPILOT_CRASH_INGEST_URL must be an HTTP(S) origin without credentials, path, query or fragment; exit 2")
		return 2
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme == "http" && !strings.EqualFold(u.Hostname(), "localhost") && (ip == nil || !ip.IsLoopback()) {
		fmt.Fprintln(out, "configuration: SHARDPILOT_CRASH_INGEST_URL requires HTTPS outside loopback; exit 2")
		return 2
	}
	actor, err := crash.SanitizeEvent(crash.Event{AnonymousID: values["SHARDPILOT_ANONYMOUS_ID"]})
	if err != nil || actor.AnonymousID != values["SHARDPILOT_ANONYMOUS_ID"] {
		fmt.Fprintln(out, "configuration: SHARDPILOT_ANONYMOUS_ID must survive the crash SDK actor sanitizer unchanged; exit 2")
		return 2
	}
	raw, err := os.ReadFile(values["SHARDPILOT_REHEARSAL_MANIFEST"])
	if err != nil {
		fmt.Fprintln(out, "configuration: SHARDPILOT_REHEARSAL_MANIFEST cannot be read; exit 2")
		return 2
	}
	// A field this sender does not know is TOLERATED — the producer may add one,
	// and refusing the manifest on run day over a new receipt field would cost
	// the owner the whole step. A twin it does not know is refused below,
	// because a twin is a body this sender must be able to spell.
	var produced manifest
	if err := json.Unmarshal(raw, &produced); err != nil || len(produced.Entries) == 0 {
		fmt.Fprintf(out, "configuration: SHARDPILOT_REHEARSAL_MANIFEST is not a manifest this sender can read (%v); exit 2\n", err)
		return 2
	}
	// The SAME redactor the protocol witness uses, not a second implementation:
	// it compares the raw credential against the JSON-unescaped and
	// percent-decoded views of whatever is printed, in either hex case.
	redactor := redact.New(values["SHARDPILOT_API_KEY"])
	fmt.Fprintf(out, "producer=%s rows=%d; synthetic crashes only; %s\n", produced.Producer, len(produced.Entries), residual)
	if log.err != nil {
		// The banner did not land, so no crash is sent: a mutation with no
		// receipt is the one outcome this sender must never produce.
		return 1
	}

	failed := false
	sent, absent := 0, 0
	note := func(record any) {
		if err := json.NewEncoder(out).Encode(record); err != nil {
			failed = true
		}
	}
	// An SDK or transport error can quote what it was handed, so it is redacted
	// exactly like a response body before it is printed.
	fail := func(subject, platform, reason, site, sdkErr string) {
		failed = true
		note(verdict{Case: "failed", Subject: subject, Platform: platform,
			Reason: redactor.Replace(reason), SDKSite: site, SDKError: redactor.Replace(sdkErr)})
	}
	// One case: its own client, its own one-request budget.
	emit := func(name string, event crash.Event) (*witness, error) {
		w := &witness{base: transport, out: out, redact: redactor, name: name}
		client, err := crash.NewClient(crash.ClientOptions{
			IngestURL:   values["SHARDPILOT_CRASH_INGEST_URL"],
			APIKey:      values["SHARDPILOT_API_KEY"],
			App:         crash.AppInfo{ID: values["SHARDPILOT_APP_ID"], Version: "rehearsal", BuildID: "symbolication-rehearsal"},
			Source:      "symbolication-rehearsal-sender",
			AnonymousID: values["SHARDPILOT_ANONYMOUS_ID"],
			HTTPClient:  &http.Client{Transport: w, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
			MaxAttempts: 1,
		})
		if err != nil {
			return w, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return w, client.EmitFatal(ctx, event)
	}
	// A crash this sender expects the door to take, with its readback row.
	send := func(name string, entry manifestEntry, event crash.Event, expectStatus int, expectSymbolicated bool, expectStatusWord, why string, frame *expectedFrame) {
		w, emitErr := emit(name, event)
		crashID, checkErr := w.acknowledged(expectStatus)
		if emitErr != nil || checkErr != nil {
			reason := errText(checkErr)
			if emitErr != nil {
				reason = errText(emitErr)
			}
			fail(name, entry.Platform, reason, "", errText(emitErr))
			return
		}
		sent++
		note(readback{Case: "readback", SentCase: name, Platform: entry.Platform, Format: entry.Format,
			CrashID: crashID, ExpectSymbolicated: expectSymbolicated, ExpectStatus: expectStatusWord,
			ExpectFrame: frame, Why: why})
	}

	for _, entry := range produced.Entries {
		if log.err != nil {
			// The evidence stream broke mid-run. Stop sending: the crashes
			// already sent have receipts, the rest would not.
			failed = true
			break
		}
		if !entry.identified() {
			if !entry.blank() {
				// PARTLY described: a name without a debug id, a base without
				// an address. This is not the artefact-less row — it is a row
				// whose crash cannot be built and whose absence cannot be
				// explained, so it is neither sent nor quietly skipped.
				fail(entry.Platform, entry.Platform,
					"the row carries some of the module identity and not all of it, so its crash can be neither sent nor honestly skipped", "", "")
				continue
			}
			// No artefact at all — the PDB leg. That is intentional only when
			// the row still says WHICH platform and format it stands for; a
			// row that names neither is a hole in the manifest, and printing
			// it as "not exercised" would present the hole as a decision.
			if strings.TrimSpace(entry.Platform) == "" || strings.TrimSpace(entry.Format) == "" {
				fail(firstText(entry.Platform, entry.Format, "(a row naming no platform)"), entry.Platform,
					"a row with no module identity is intentional only when it names its platform and its format", "", "")
				continue
			}
			// Printed, never skipped: the owner must read NOT EXERCISED rather
			// than read nothing.
			absent++
			note(verdict{Case: "not-exercised", Subject: entry.Platform, Platform: entry.Platform,
				Reason: firstText(entry.Note, "the producer wrote no artefact for this row, so there is no module identity to send")})
			if len(entry.NegativeTwins) > 0 {
				fail(entry.Platform, entry.Platform, "the row has no artefact yet promises negative twins", "", "")
			}
			continue
		}
		// A claimed frame must be comparable: an empty expected_frame is a
		// promise with nothing in it.
		if entry.Symbolicated && !entry.Expected.frameComplete() {
			fail(entry.Platform, entry.Platform,
				"the row claims a resolved frame but names no complete expected frame (function, file and line) to compare", "", "")
			continue
		}
		// The positive expectation is the ROW's: a row that does not exercise
		// symbolication never promises a resolved frame.
		expectWord, expectWhy := "resolved", ""
		if !entry.Symbolicated {
			expectWord = "not-exercised"
			expectWhy = firstText(entry.Note, "this row proves an accepted upload, not a resolved frame")
		}
		send(entry.Platform+"-positive", entry, controlledCrash(entry, nil), http.StatusAccepted,
			entry.Symbolicated, expectWord, expectWhy, entry.Expected)
		for _, promised := range entry.NegativeTwins {
			if log.err != nil {
				// The previous send's own receipt did not land. Every twin
				// after it would be a mutation with no evidence, so the row
				// stops here and the outer loop stops on its next turn.
				failed = true
				break
			}
			known, ok := twinsByChange[promised.Change]
			if !ok {
				// A break this sender cannot spell is a failure naming it, not
				// a row to leave out.
				fail(entry.Platform, entry.Platform, "unrecognised negative twin in the manifest: "+promised.Change, "", "")
				continue
			}
			name := entry.Platform + "-" + known.slug
			if strings.TrimSpace(promised.Status) == "" {
				// expect_status: "" is a readback row with nothing to compare.
				fail(name, entry.Platform,
					"the twin names no status to read back: "+promised.Change, "", "")
				continue
			}
			if known.refusal != "" {
				// Attempted, so the refusal is measured rather than asserted.
				w, emitErr := emit(name, controlledCrash(entry, known.mutate))
				if emitErr == nil || !errors.Is(emitErr, crash.ErrInvalidEvent) || w.requested() {
					fail(name, entry.Platform, "the SDK no longer refuses this shape before sending it",
						known.refusal, errText(emitErr))
					continue
				}
				absent++
				note(verdict{Case: "not-exercised", Subject: name, Platform: entry.Platform,
					Reason:  "the SDK refuses this shape before any request, so a client built on it cannot produce the crash this twin describes; the ingest contract for it is exercised by crash-symbolicator's own handler test",
					SDKSite: known.refusal, SDKError: emitErr.Error()})
				continue
			}
			expect := promised.HTTPStatus
			if expect == 0 {
				expect = http.StatusAccepted
			}
			send(name, entry, controlledCrash(entry, known.mutate), expect,
				promised.Symbolicated, promised.Status, promised.Why, nil)
		}
	}
	if sent == 0 {
		fail("run", "", "no crash was sent", "", "")
	}
	if log.err != nil {
		return 1
	}
	code := 0
	if failed {
		code = 1
	}
	note(summary{Case: "summary", Sent: sent, NotExercised: absent, Rows: len(produced.Entries),
		Contract: !failed, ExitCode: code, Residual: residual})
	if log.err != nil {
		return 1
	}
	return code
}

func firstText(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
