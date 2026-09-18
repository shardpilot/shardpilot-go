package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
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

const (
	fixtureKey   = "crash-write-key-test"
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
	case "refused":
		status, reply = http.StatusBadRequest, map[string]any{"code": "invalid_request"}
	}
	f.statuses = append(f.statuses, status)
	encoded, err := json.Marshal(reply)
	if err != nil {
		return nil, err
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
	if strings.Contains(out.String(), fixtureKey) {
		t.Fatalf("the configured credential reached the evidence stream")
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
		"linux-positive", "linux-wrong-debug-id", "linux-wrong-base",
		"ios-positive", "ios-wrong-debug-id",
	}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sent cases %v, want %v", got, want)
	}
	if len(transport.bodies) != 5 {
		t.Fatalf("expected five crash bodies on the wire, got %d", len(transport.bodies))
	}
	// Each twin is spelled on the WIRE, not only in its case name.
	linuxPositive, linuxWrongID, linuxWrongBase := transport.bodies[0], transport.bodies[1], transport.bodies[2]
	if moduleField(t, linuxPositive, "debug_id") == moduleField(t, linuxWrongID, "debug_id") {
		t.Fatal("the wrong-debug-id twin carried the manifest's debug id")
	}
	if moduleField(t, linuxWrongBase, "load_address") != "0x800000" {
		t.Fatalf("the wrong-base twin carried %q", moduleField(t, linuxWrongBase, "load_address"))
	}
	if moduleField(t, linuxPositive, "load_address") != "0x400000" {
		t.Fatal("the positive body did not carry the manifest's load address")
	}
	// The summary counts, and the residual travels with them.
	summary := records[len(records)-1]
	if summary["case"] != "summary" || summary["sent"].(float64) != 5 || summary["not_exercised"].(float64) != 5 ||
		summary["contract_match"] != true || summary["exit_code"].(float64) != 0 {
		t.Fatalf("unexpected summary: %v", summary)
	}
	if !strings.Contains(summary["residual"].(string), "not a resolved frame") {
		t.Fatalf("the summary lost its residual: %v", summary["residual"])
	}
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
	if len(byCase) != 5 {
		t.Fatalf("expected five readback rows, got %d", len(byCase))
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
	for _, subject := range []string{"linux-no-base-at-all", "linux-no-module-range",
		"ios-no-base-at-all", "ios-no-module-range", "windows-pdb"} {
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
	if len(transport.bodies) != 5 {
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
	noFrame := strings.Replace(manifestFixture,
		`"expected_frame": {"function": "rehearsal_crash", "file": "rehearsal.c", "line": 42},`, "", 1)
	code, out, _ := sendWith(t, "", noFrame, nil)
	if code != 1 || !strings.Contains(out, "names no expected frame") {
		t.Fatalf("expected a failure naming the missing frame, got %d\n%s", code, out)
	}
	// A row with no artefact that still promises twins is a broken manifest.
	promising := strings.Replace(manifestFixture,
		`"note": "NOT EXERCISED: no PDB artefact is produced.",`,
		`"note": "NOT EXERCISED", "negative_twins": [{"change": "x", "status": "y", "symbolicated": false, "why": "z"}],`, 1)
	code, out, _ = sendWith(t, "", promising, nil)
	if code != 1 || !strings.Contains(out, "no artefact yet promises negative twins") {
		t.Fatalf("expected a failure over the artefact-less row, got %d\n%s", code, out)
	}
	// An empty manifest is configuration, not a clean run over nothing.
	code, out, _ = sendWith(t, "", `{"producer": "x", "entries": []}`, nil)
	if code != 2 {
		t.Fatalf("expected exit 2 over an empty manifest, got %d\n%s", code, out)
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

func TestAnEchoedCredentialIsRedacted(t *testing.T) {
	code, out, _ := sendWith(t, "echo_key", manifestFixture, nil)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d\n%s", code, out)
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("an echoed credential was not redacted:\n%s", out)
	}
}
