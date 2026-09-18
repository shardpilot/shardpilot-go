package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/shardpilot/shardpilot-go/pkg/crash"
)

// A manifest shaped exactly as cmd/symbolication-rehearsal writes one: a
// Breakpad row with all four twins, a dSYM row with three (no wrong-base), and
// the PDB row that carries no artefact. The extra receipt fields are present on
// purpose — this sender must read a manifest it does not fully model.
const manifestFixture = `{
  "producer": "cmd/symbolication-rehearsal",
  "entries": [
    {"platform": "linux", "format": "breakpad", "file": "linux-rehearsal.sym",
     "path": "rehearsal/linux-rehearsal.sym", "sha256": "0f", "bytes": 512,
     "module_name": "librehearsal.so", "debug_id": "5A1D0C7E4B2F41889E6D3A05C7B1E2F40",
     "load_address": "0x400000", "instruction_address": "0x401040",
     "symbolication_exercised": true,
     "expected_frame": {"function": "rehearsal_crash", "file": "rehearsal.c", "line": 42},
     "identity_source": "written by breakpad.go", "upload_command": "shardpilot symbols upload --platform linux 'x'",
     "negative_twins": [
       {"change": "the crash body carries a debug id no uploaded symbol has", "status": "symbol_missing", "symbolicated": false, "why": "the symbol lookup for that id finds nothing"},
       {"change": "the frame's address falls in no declared module's range (two modules declared, so there is no single-module default)", "status": "module_missing", "symbolicated": false, "why": "no declared range contains the address"},
       {"change": "the module declares no load_address and no base_address", "status": "rejected", "http_status": 400, "symbolicated": false, "why": "a module with no base is a rejected input"},
       {"change": "the crash body declares the wrong load_address", "status": "unresolved", "symbolicated": false, "why": "the frame moves outside every FUNC record"}
     ]},
    {"platform": "ios", "format": "dsym", "file": "Rehearsal.app.dSYM.zip",
     "module_name": "Rehearsal", "debug_id": "6B2E1D8F5C305299AF7E4B16D8C2F3A50",
     "load_address": "0x100000000", "instruction_address": "0x100001f40",
     "symbolication_exercised": true,
     "expected_frame": {"function": "main.crash", "file": "main.go", "line": 17},
     "residual": "crash-symbolicator#178", "upload_command": "shardpilot symbols upload --platform ios 'x'",
     "negative_twins": [
       {"change": "the crash body carries a debug id no uploaded symbol has", "status": "symbol_missing", "symbolicated": false, "why": "the symbol lookup for that id finds nothing"},
       {"change": "the frame's address falls in no declared module's range (two modules declared, so there is no single-module default)", "status": "module_missing", "symbolicated": false, "why": "no declared range contains the address"},
       {"change": "the module declares no load_address and no base_address", "status": "rejected", "http_status": 400, "symbolicated": false, "why": "a module with no base is a rejected input"}
     ]},
    {"platform": "windows-pdb", "format": "pdb", "symbolication_exercised": false,
     "note": "NOT EXERCISED: no PDB artefact is produced.",
     "upload_command": "shardpilot symbols upload --platform windows './MyGame.pdb'"}
  ]
}`

// jsonEscapedSlash is how a JSON body spells the credential's metacharacter.
var jsonEscapedSlash = string(rune(92)) + "u002f"

const (
	fixtureKey   = "crash/write-key-test"
	fixtureActor = "rehearsal-actor-0001"
)

// fake is the in-memory crash door: it opens no listener and records the bodies
// it was handed, so a scene can read what the SDK actually serialized.
type fake struct {
	mu       sync.Mutex
	mode     string
	bodies   []map[string]any
	statuses []int
}

func (f *fake) RoundTrip(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	f.bodies = append(f.bodies, parsed)
	id, _ := parsed["crash_id"].(string)
	reply := map[string]any{"crash_id": id, "fingerprint": "fp-" + id, "suppressed": false}
	status := http.StatusAccepted
	switch f.mode {
	case "blank_fingerprint":
		reply["fingerprint"] = "   "
	case "no_fingerprint":
		delete(reply, "fingerprint")
	case "wrong_id":
		reply["crash_id"] = "some-other-crash"
	case "suppressed":
		reply["suppressed"] = true
	case "echo_key":
		reply["echo"] = req.Header.Get("Authorization")
	case "error_with_key":
		// A transport error that quotes what it was handed. The sender must
		// redact it like any other echoed text.
		return nil, fmt.Errorf("dial tcp: refused while sending with %s", fixtureKey)
	case "error_with_encoded_key":
		return nil, fmt.Errorf("dial tcp: refused while sending with %s", url.QueryEscape(fixtureKey))
	case "echo_key_percent":
		reply["echo"] = url.QueryEscape(fixtureKey)
	case "echo_key_percent_lower":
		reply["echo"] = strings.ReplaceAll(fixtureKey, "/", "%2f")

	case "refused":
		status, reply = http.StatusBadRequest, map[string]any{"code": "invalid_request"}
	}
	f.statuses = append(f.statuses, status)
	encoded, err := json.Marshal(reply)
	if err != nil {
		return nil, err
	}
	if f.mode == "echo_key_json_unicode" {
		// Hand-built, because the escape has to be the JSON STRING's — a Go
		// string holding a backslash would be escaped again by Marshal and the
		// value on the wire would be literal text, not the credential.
		encoded = []byte(fmt.Sprintf(`{"crash_id":%q,"fingerprint":%q,"suppressed":false,"echo":"%s"}`,
			id, "fp-"+id, strings.ReplaceAll(fixtureKey, "/", jsonEscapedSlash)))
		if !json.Valid(encoded) {
			return nil, fmt.Errorf("the escaped echo fixture is not valid JSON: %s", encoded)
		}
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(bytes.NewReader(encoded)),
		Header: http.Header{"Content-Type": {"application/json"}, "X-Request-Id": {"fixture"}}}, nil
}

func sendWith(t *testing.T, mode, manifestBody string, overrides map[string]string) (int, string, *fake) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, []byte(manifestBody), 0o600); err != nil {
		t.Fatalf("write manifest fixture: %v", err)
	}
	env := map[string]string{
		"SHARDPILOT_CRASH_INGEST_URL":   "http://127.0.0.1:65535",
		"SHARDPILOT_API_KEY":            fixtureKey,
		"SHARDPILOT_APP_ID":             "app_rehearsal",
		"SHARDPILOT_ANONYMOUS_ID":       fixtureActor,
		"SHARDPILOT_REHEARSAL_MANIFEST": path,
	}
	for name, value := range overrides {
		if value == "" {
			delete(env, name)
			continue
		}
		env[name] = value
	}
	transport := &fake{mode: mode}
	out := &bytes.Buffer{}
	code := run(func(name string) string { return env[name] }, out, transport)
	// The credential in every spelling this sender claims to mask: a scene that
	// only checked the raw form would pass while an encoded echo leaked.
	for _, form := range []string{fixtureKey, url.QueryEscape(fixtureKey),
		strings.ReplaceAll(fixtureKey, "/", "%2f"),
		strings.ReplaceAll(fixtureKey, "/", jsonEscapedSlash)} {
		if strings.Contains(out.String(), form) {
			t.Fatalf("the configured credential reached the evidence stream as %q", form)
		}
	}
	return code, out.String(), transport
}

// lines returns the JSON records of a run, keyed by nothing: order matters.
func lines(t *testing.T, out string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("evidence line is not JSON: %s", line)
		}
		records = append(records, record)
	}
	return records
}

func casesOf(records []map[string]any, kind string) []string {
	var out []string
	for _, record := range records {
		if record["case"] == kind {
			continue
		}
		if kind == "exchange" && record["latency_ms"] != nil {
			out = append(out, record["case"].(string))
		}
	}
	return out
}

func TestEveryIdentifiedRowSendsItsPositiveAndExpressibleTwins(t *testing.T) {
	code, out, transport := sendWith(t, "", manifestFixture, nil)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d\n%s", code, out)
	}
	records := lines(t, out)
	if got, want := casesOf(records, "exchange"), []string{
		"linux-positive", "linux-wrong-debug-id", "linux-no-module-range", "linux-wrong-base",
		"ios-positive", "ios-wrong-debug-id", "ios-no-module-range",
	}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sent cases %v, want %v", got, want)
	}
	if len(transport.bodies) != 7 {
		t.Fatalf("expected seven crash bodies on the wire, got %d", len(transport.bodies))
	}
	// Each twin is spelled on the WIRE, not only in its case name.
	linuxPositive, linuxWrongID, linuxWrongBase := transport.bodies[0], transport.bodies[1], transport.bodies[3]
	if moduleField(t, linuxPositive, "debug_id") == moduleField(t, linuxWrongID, "debug_id") {
		t.Fatal("the wrong-debug-id twin carried the manifest's debug id")
	}
	if moduleField(t, linuxWrongBase, "load_address") != "0x800000" {
		t.Fatalf("the wrong-base twin carried %q", moduleField(t, linuxWrongBase, "load_address"))
	}
	if moduleField(t, linuxPositive, "load_address") != "0x400000" {
		t.Fatal("the positive body did not carry the manifest's load address")
	}
	noRange := transport.bodies[2]
	modules, _ := noRange["modules"].([]any)
	if len(modules) != 2 {
		t.Fatalf("the no-module-range twin declared %d module(s)", len(modules))
	}
	frame := crashedFrame(t, noRange)
	if frame["module_id"] == "" || frame["module_id"] == moduleID {
		t.Fatalf("the twin's frame names %v; it must name a module id none of the declared ones has", frame["module_id"])
	}
	if frame["instruction_addr"] != "0x900000" {
		t.Fatalf("the twin's frame address is %v", frame["instruction_addr"])
	}
	for _, declared := range modules {
		module := declared.(map[string]any)
		if module["id"] == frame["module_id"] {
			t.Fatalf("the twin's frame matches a declared module by id: %v", module["id"])
		}
		if module["load_address"] == "" || module["end_address"] == "" {
			t.Fatalf("a declared module has no range: %v", module)
		}
	}
	// The summary counts, and the residual travels with them.
	summary := records[len(records)-1]
	if summary["case"] != "summary" || summary["sent"].(float64) != 7 || summary["not_exercised"].(float64) != 3 ||
		summary["contract_match"] != true || summary["exit_code"].(float64) != 0 {
		t.Fatalf("unexpected summary: %v", summary)
	}
	if !strings.Contains(summary["residual"].(string), "not a resolved frame") {
		t.Fatalf("the summary lost its residual: %v", summary["residual"])
	}
}

func crashedFrame(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	threads, ok := body["threads"].([]any)
	if !ok || len(threads) != 1 {
		t.Fatalf("body carries no single thread: %v", body["threads"])
	}
	frames, ok := threads[0].(map[string]any)["frames"].([]any)
	if !ok || len(frames) != 1 {
		t.Fatalf("thread carries no single frame: %v", threads[0])
	}
	return frames[0].(map[string]any)
}

func moduleField(t *testing.T, body map[string]any, field string) string {
	t.Helper()
	modules, ok := body["modules"].([]any)
	if !ok || len(modules) == 0 {
		t.Fatalf("body carries no modules: %v", body)
	}
	value, _ := modules[0].(map[string]any)[field].(string)
	return value
}

func TestReadbackNamesTheExpectedFrameOrStatusPerCrashID(t *testing.T) {
	_, out, _ := sendWith(t, "", manifestFixture, nil)
	byCase := map[string]map[string]any{}
	for _, record := range lines(t, out) {
		if record["case"] == "readback" {
			byCase[record["sent_case"].(string)] = record
		}
	}
	if len(byCase) != 7 {
		t.Fatalf("expected seven readback rows, got %d", len(byCase))
	}
	if row := byCase["linux-no-module-range"]; row["expect_status"] != "module_missing" || row["expect_symbolicated"] != false {
		t.Fatalf("the no-module-range row does not expect the manifest's status: %v", row)
	}
	positive := byCase["linux-positive"]
	if positive["expect_symbolicated"] != true || positive["expect_status"] != "resolved" {
		t.Fatalf("the positive row does not expect a resolved frame: %v", positive)
	}
	frame := positive["expect_frame"].(map[string]any)
	if frame["function"] != "rehearsal_crash" || frame["file"] != "rehearsal.c" || frame["line"].(float64) != 42 {
		t.Fatalf("the positive row lost the manifest's expected frame: %v", frame)
	}
	if id, ok := positive["crash_id"].(string); !ok || strings.TrimSpace(id) == "" {
		t.Fatal("the readback row carries no crash id to read back")
	}
	twin := byCase["linux-wrong-base"]
	if twin["expect_symbolicated"] != false || twin["expect_status"] != "unresolved" || twin["expect_frame"] != nil {
		t.Fatalf("the twin row does not expect the manifest's status: %v", twin)
	}
}

func TestTheTwinsTheSDKRefusesAreAttemptedAndPrinted(t *testing.T) {
	code, out, transport := sendWith(t, "", manifestFixture, nil)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d\n%s", code, out)
	}
	refused := map[string]map[string]any{}
	for _, record := range lines(t, out) {
		if record["case"] == "not-exercised" {
			refused[record["subject"].(string)] = record
		}
	}
	for _, subject := range []string{"linux-no-base-at-all", "ios-no-base-at-all", "windows-pdb"} {
		record, ok := refused[subject]
		if !ok {
			t.Fatalf("%s was neither sent nor printed as not exercised", subject)
		}
		if subject == "windows-pdb" {
			continue
		}
		// The refusal is MEASURED: the SDK's own error, and no request.
		if site, _ := record["sdk_site"].(string); !strings.Contains(site, "pkg/crash/event.go") {
			t.Fatalf("%s does not cite the validation that refuses it: %v", subject, record)
		}
		if text, _ := record["sdk_error"].(string); !strings.Contains(text, "invalid shardpilot crash event") {
			t.Fatalf("%s does not carry the SDK's refusal: %v", subject, record)
		}
	}
	if len(transport.bodies) != 7 {
		t.Fatalf("a refused twin reached the door: %d bodies", len(transport.bodies))
	}
}

func TestFalseSuccessRepliesFailTheRun(t *testing.T) {
	for _, mode := range []string{"blank_fingerprint", "no_fingerprint", "wrong_id", "suppressed", "refused"} {
		t.Run(mode, func(t *testing.T) {
			code, out, _ := sendWith(t, mode, manifestFixture, nil)
			if code != 1 {
				t.Fatalf("expected exit 1 for %s, got %d\n%s", mode, code, out)
			}
			records := lines(t, out)
			summary := records[len(records)-1]
			if summary["contract_match"] != false {
				t.Fatalf("%s passed the contract: %v", mode, summary)
			}
		})
	}
}

func TestAnUnrecognisedTwinFailsTheRunNamingIt(t *testing.T) {
	broken := strings.Replace(manifestFixture,
		"the crash body declares the wrong load_address",
		"the crash body declares a load_address in the wrong nibble", 1)
	code, out, _ := sendWith(t, "", broken, nil)
	if code != 1 {
		t.Fatalf("expected exit 1, got %d\n%s", code, out)
	}
	if !strings.Contains(out, "unrecognised negative twin in the manifest: the crash body declares a load_address in the wrong nibble") {
		t.Fatalf("the run did not name the twin it could not spell:\n%s", out)
	}
	// And it is not quietly sent as something else.
	for _, record := range lines(t, out) {
		if record["case"] == "linux-wrong-base" {
			t.Fatalf("an unrecognised twin was sent anyway: %v", record)
		}
	}
}

func TestARowIsNeverSkippedSilently(t *testing.T) {
	// A row that claims a resolved frame but names none cannot be compared on
	// run day, so it fails instead of being sent.
	for _, broken := range []string{"", `"expected_frame": {},`,
		`"expected_frame": {"function": "rehearsal_crash", "file": "", "line": 42},`,
		`"expected_frame": {"function": "rehearsal_crash", "file": "rehearsal.c", "line": 0},`} {
		manifest := strings.Replace(manifestFixture,
			`"expected_frame": {"function": "rehearsal_crash", "file": "rehearsal.c", "line": 42},`, broken, 1)
		code, out, transport := sendWith(t, "", manifest, nil)
		if code != 1 || !strings.Contains(out, "no complete expected frame") {
			t.Fatalf("a row claiming a frame it cannot be compared against passed (%q): %d\n%s", broken, code, out)
		}
		for _, body := range transport.bodies {
			if body["platform"] == "linux" {
				t.Fatalf("the uncomparable row was sent anyway (%q)", broken)
			}
		}
	}
	// A row with no artefact that still promises twins is a broken manifest.
	promising := strings.Replace(manifestFixture,
		`"note": "NOT EXERCISED: no PDB artefact is produced.",`,
		`"note": "NOT EXERCISED", "negative_twins": [{"change": "x", "status": "y", "symbolicated": false, "why": "z"}],`, 1)
	if code, out, _ := sendWith(t, "", promising, nil); code != 1 ||
		!strings.Contains(out, "no artefact yet promises negative twins") {
		t.Fatalf("expected a failure over the artefact-less row, got %d\n%s", code, out)
	}
	// An empty manifest is configuration, not a clean run over nothing.
	if code, out, _ := sendWith(t, "", `{"producer": "x", "entries": []}`, nil); code != 2 {
		t.Fatalf("expected exit 2 over an empty manifest, got %d\n%s", code, out)
	}
	// A row that carries SOME of the identity is a failure naming it: its
	// crash cannot be built, and its absence cannot be explained either.
	for _, field := range []string{`"module_name": "librehearsal.so", `,
		`"debug_id": "5A1D0C7E4B2F41889E6D3A05C7B1E2F40",`,
		`"load_address": "0x400000", `, `"instruction_address": "0x401040",`} {
		manifest := strings.Replace(manifestFixture, field, "", 1)
		code, out, transport := sendWith(t, "", manifest, nil)
		if code != 1 || !strings.Contains(out, "some of the module identity and not all of it") {
			t.Fatalf("a partly identified row was not refused (%q): %d\n%s", field, code, out)
		}
		for _, body := range transport.bodies {
			if body["platform"] == "linux" {
				t.Fatalf("the partly identified row was sent anyway (%q)", field)
			}
		}
	}
}

func TestConfigurationIsRefusedBeforeAnyRequest(t *testing.T) {
	for _, name := range []string{"SHARDPILOT_CRASH_INGEST_URL", "SHARDPILOT_API_KEY",
		"SHARDPILOT_APP_ID", "SHARDPILOT_ANONYMOUS_ID", "SHARDPILOT_REHEARSAL_MANIFEST"} {
		code, out, transport := sendWith(t, "", manifestFixture, map[string]string{name: ""})
		if code != 2 || !strings.Contains(out, name) {
			t.Fatalf("missing %s did not exit 2 naming it: %d\n%s", name, code, out)
		}
		if len(transport.bodies) != 0 {
			t.Fatalf("missing %s still sent %d crash(es)", name, len(transport.bodies))
		}
	}
	for _, bad := range []string{"http://example.test", "https://example.test/path",
		"https://user@example.test", "https://example.test?x=1", "not-a-url"} {
		code, _, transport := sendWith(t, "", manifestFixture,
			map[string]string{"SHARDPILOT_CRASH_INGEST_URL": bad})
		if code != 2 || len(transport.bodies) != 0 {
			t.Fatalf("%q was accepted as a crash origin (exit %d, %d sends)", bad, code, len(transport.bodies))
		}
	}
	code, out, transport := sendWith(t, "", manifestFixture,
		map[string]string{"SHARDPILOT_REHEARSAL_MANIFEST": filepath.Join(t.TempDir(), "absent.json")})
	if code != 2 || !strings.Contains(out, "cannot be read") || len(transport.bodies) != 0 {
		t.Fatalf("an unreadable manifest did not exit 2: %d\n%s", code, out)
	}
}

// brokenSink fails after n successful writes, so a scene can break the evidence
// stream at the banner or in the middle of the run.
type brokenSink struct {
	after, written int
}

func (b *brokenSink) Write(p []byte) (int, error) {
	if b.written >= b.after {
		return 0, io.ErrClosedPipe
	}
	b.written++
	return len(p), nil
}

func runWithSink(t *testing.T, sink io.Writer, after int) (int, *fake) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, []byte(manifestFixture), 0o600); err != nil {
		t.Fatalf("write manifest fixture: %v", err)
	}
	env := map[string]string{
		"SHARDPILOT_CRASH_INGEST_URL":   "http://127.0.0.1:65535",
		"SHARDPILOT_API_KEY":            fixtureKey,
		"SHARDPILOT_APP_ID":             "app_rehearsal",
		"SHARDPILOT_ANONYMOUS_ID":       fixtureActor,
		"SHARDPILOT_REHEARSAL_MANIFEST": path,
	}
	transport := &fake{}
	return run(func(name string) string { return env[name] }, sink, transport), transport
}

func TestABrokenEvidenceSinkStopsTheRun(t *testing.T) {
	// The banner cannot be written: nothing is sent at all. A mutation with no
	// receipt is the one outcome this sender must never produce.
	code, transport := runWithSink(t, &brokenSink{after: 0}, 0)
	if code != 1 {
		t.Fatalf("a failed banner write did not exit 1, got %d", code)
	}
	if len(transport.bodies) != 0 {
		t.Fatalf("a failed banner write still sent %d crash(es)", len(transport.bodies))
	}
	// It breaks later instead: the crashes already sent have receipts, the run
	// stops rather than sending blind, and it never reports success.
	code, transport = runWithSink(t, &brokenSink{after: 4}, 4)
	if code != 1 {
		t.Fatalf("a mid-run evidence failure did not exit 1, got %d", code)
	}
	if len(transport.bodies) == 0 || len(transport.bodies) >= 7 {
		t.Fatalf("a mid-run evidence failure sent %d of 7 crashes; it must stop short", len(transport.bodies))
	}
}

func TestAPropagatedTransportErrorIsRedacted(t *testing.T) {
	code, out, _ := sendWith(t, "error_with_key", manifestFixture, nil)
	if code != 1 {
		t.Fatalf("expected exit 1, got %d\n%s", code, out)
	}
	// sendWith already fails the test if the raw key appears anywhere in the
	// stream; this asserts the redaction reached BOTH printed fields.
	var reasons, errs int
	for _, record := range lines(t, out) {
		if record["case"] != "failed" {
			continue
		}
		if strings.Contains(record["reason"].(string), "[REDACTED]") {
			reasons++
		}
		if text, ok := record["sdk_error"].(string); ok && strings.Contains(text, "[REDACTED]") {
			errs++
		}
	}
	if reasons == 0 || errs == 0 {
		t.Fatalf("the propagated error was not redacted in both fields (%d reasons, %d sdk_errors)\n%s", reasons, errs, out)
	}
}

func TestClearingTheTwinsModuleIDMakesTheSDKRefuseIt(t *testing.T) {
	// The no-module-range twin is expressible only because the SDK wants the
	// frame's module selector NONEMPTY, not resolvable. Clearing it — the
	// shape the rehearsal's handler test uses — is refused before any request,
	// which is why the twin names an id no declared module has instead.
	entry := manifestEntry{Platform: "linux", ModuleName: "librehearsal.so",
		DebugID: "5A1D0C7E4B2F41889E6D3A05C7B1E2F40", LoadAddress: "0x400000",
		InstructionAddress: "0x401040"}
	event := controlledCrash(entry, twinsByChange[twinNoModuleRange].mutate)
	event.Threads[0].Frames[0].ModuleID = ""
	transport := &fake{}
	client, err := crash.NewClient(crash.ClientOptions{
		IngestURL: "http://127.0.0.1:65535", APIKey: fixtureKey,
		App: crash.AppInfo{ID: "app_rehearsal"}, AnonymousID: fixtureActor,
		HTTPClient: &http.Client{Transport: transport}, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("construct the crash client: %v", err)
	}
	err = client.EmitFatal(context.Background(), event)
	if !errors.Is(err, crash.ErrInvalidEvent) {
		t.Fatalf("a frame naming no module was not refused: %v", err)
	}
	if len(transport.bodies) != 0 {
		t.Fatalf("the refused event still reached the door: %d bodies", len(transport.bodies))
	}
	// And with the twin's own id it is accepted for sending.
	if err := client.EmitFatal(context.Background(), controlledCrash(entry, twinsByChange[twinNoModuleRange].mutate)); err != nil {
		t.Fatalf("the twin as sent is not emittable: %v", err)
	}
}

func TestARowThatDoesNotExerciseSymbolicationNeverExpectsAResolvedFrame(t *testing.T) {
	// Full identity, no claim of a resolved frame: the readback must say so
	// instead of pairing "resolved" with expect_symbolicated false.
	manifest := strings.Replace(manifestFixture, `"symbolication_exercised": true,
     "expected_frame": {"function": "rehearsal_crash", "file": "rehearsal.c", "line": 42},`,
		`"symbolication_exercised": false, "note": "the upload is what this row proves",`, 1)
	code, out, _ := sendWith(t, "", manifest, nil)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d\n%s", code, out)
	}
	for _, record := range lines(t, out) {
		if record["case"] != "readback" || record["sent_case"] != "linux-positive" {
			continue
		}
		if record["expect_symbolicated"] != false {
			t.Fatalf("the row promised a resolved frame it does not exercise: %v", record)
		}
		if record["expect_status"] == "resolved" {
			t.Fatalf("expect_status contradicts expect_symbolicated: %v", record)
		}
		if record["expect_frame"] != nil {
			t.Fatalf("a row that exercises nothing still named a frame: %v", record)
		}
		if why, _ := record["why"].(string); !strings.Contains(why, "upload") {
			t.Fatalf("the row does not say what it does prove: %v", record)
		}
		return
	}
	t.Fatal("no readback row for linux-positive")
}

func TestAnEchoedCredentialIsRedactedInEverySpelling(t *testing.T) {
	// The helper already fails on the raw and the encoded forms anywhere in the
	// stream; this asserts the mask actually landed rather than the echo simply
	// not arriving.
	for _, mode := range []string{"echo_key", "echo_key_percent", "echo_key_percent_lower",
		"echo_key_json_unicode"} {
		t.Run(mode, func(t *testing.T) {
			code, out, transport := sendWith(t, mode, manifestFixture, nil)
			if code != 0 {
				t.Fatalf("expected exit 0, got %d\n%s", code, out)
			}
			if len(transport.bodies) != 7 {
				t.Fatalf("the echo mode changed the run: %d bodies", len(transport.bodies))
			}
			if strings.Count(out, "[REDACTED]") < 7 {
				t.Fatalf("the echoed credential was not masked in every reply:\n%s", out)
			}
		})
	}
	// And the same for an error that quotes what it was handed.
	for _, mode := range []string{"error_with_key", "error_with_encoded_key"} {
		t.Run(mode, func(t *testing.T) {
			code, out, _ := sendWith(t, mode, manifestFixture, nil)
			if code != 1 {
				t.Fatalf("expected exit 1, got %d\n%s", code, out)
			}
			if !strings.Contains(out, "[REDACTED]") {
				t.Fatalf("the propagated error was not redacted:\n%s", out)
			}
		})
	}
}

func TestAFailedReadbackWriteStopsTheRowsRemainingSends(t *testing.T) {
	// Writes: the banner, the first exchange, then its readback. Breaking the
	// third means the positive's own receipt is incomplete, so no twin for that
	// row may follow it.
	code, transport := runWithSink(t, &brokenSink{after: 2}, 2)
	if code != 1 {
		t.Fatalf("a failed readback write did not exit 1, got %d", code)
	}
	if len(transport.bodies) != 1 {
		t.Fatalf("sends continued past a failed readback: %d bodies", len(transport.bodies))
	}
}

func TestATwinWithNoStatusFailsTheRunNamingIt(t *testing.T) {
	for _, status := range []string{`""`, `"   "`} {
		manifest := strings.Replace(manifestFixture, `"status": "unresolved"`, `"status": `+status, 1)
		code, out, transport := sendWith(t, "", manifest, nil)
		if code != 1 {
			t.Fatalf("a twin with status %s did not fail the run: %d\n%s", status, code, out)
		}
		if !strings.Contains(out, "the twin names no status to read back: the crash body declares the wrong load_address") {
			t.Fatalf("the run did not name the twin missing its status:\n%s", out)
		}
		for _, record := range lines(t, out) {
			if record["case"] == "linux-wrong-base" {
				t.Fatalf("the twin was sent without a status to read back: %v", record)
			}
		}
		// The rest of the run still happens: one twin's defect is not a reason
		// to stop measuring the others.
		if len(transport.bodies) != 6 {
			t.Fatalf("expected the other six sends, got %d", len(transport.bodies))
		}
	}
}

func TestAnIdentityFreeRowMustSayWhatItStandsFor(t *testing.T) {
	manifest := strings.Replace(manifestFixture, `{"platform": "windows-pdb", "format": "pdb", "symbolication_exercised": false,
     "note": "NOT EXERCISED: no PDB artefact is produced.",
     "upload_command": "shardpilot symbols upload --platform windows './MyGame.pdb'"}`, `{}`, 1)
	code, out, _ := sendWith(t, "", manifest, nil)
	if code != 1 {
		t.Fatalf("an empty row did not fail the run: %d\n%s", code, out)
	}
	if !strings.Contains(out, "intentional only when it names its platform and its format") {
		t.Fatalf("the run did not name the hole in the manifest:\n%s", out)
	}
	// A row that names its platform but not its format is the same hole.
	manifest = strings.Replace(manifestFixture, `"platform": "windows-pdb", "format": "pdb",`,
		`"platform": "windows-pdb",`, 1)
	if code, out, _ := sendWith(t, "", manifest, nil); code != 1 ||
		!strings.Contains(out, "intentional only when it names its platform and its format") {
		t.Fatalf("a row with no format did not fail the run: %d\n%s", code, out)
	}
}
