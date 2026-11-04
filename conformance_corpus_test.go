package shardpilot

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

const conformanceRoot = "testdata/conformance/"
const conformanceSHA = "bd63843c26a31f1c0ef7750352f983a3c8fe0321d1ce192e331fdb373fbe7340"

type conformanceSide struct {
	Method, Path, Credential, File, SHA256 string
	Bytes, Status                          int
	Headers                                map[string]string
}

type conformanceCase struct {
	ID, Surface, Operation string
	ByteExact              bool `json:"byte_exact"`
	Request, Response      conformanceSide
}

type conformanceManifest struct {
	CorpusSHA     string `json:"corpus_sha256"`
	Cases         []conformanceCase
	Applicability map[string]map[string]struct{ Value, Reason string }
}

func conformanceRequestDispositions() map[string]string {
	const batch = "SDK reserves app.* names and stamps ingest time; valid probe replays malformed/retired requests"
	const consent = "SDK stamps receipt time/key and derives actor kind; client JWT unsupported"
	const crash = "SDK requires UUIDv7 and module load_address; typed body omits retired/claimed-user members"
	return map[string]string{
		"batch-accepted":                     batch,
		"batch-size-rejected-mixed":          batch,
		"batch-duplicate":                    batch,
		"batch-suppressed-no-consent":        batch,
		"batch-retired-scope-member":         batch,
		"batch-malformed-json":               batch,
		"identity-batch-anonymous":           batch,
		"identity-batch-user-and-anonymous":  batch,
		"consent-grant-notice":               consent,
		"consent-denial-publishable":         consent,
		"consent-replayed":                   consent,
		"consent-grant-publishable-refused":  consent,
		"consent-retired-scope-member":       consent,
		"identity-consent-subject-match":     consent,
		"identity-consent-subject-mismatch":  consent,
		"identity-consent-kind-forgery":      consent,
		"json_ingest":                        crash,
		"json_key_id_shaped":                 crash,
		"json_retired":                       crash,
		"json_identity_claimed_user":         crash,
		"json_identity_user_only":            crash,
		"rc.two-key-200":                     "PASS",
		"rc.identity-header-200":             "PASS",
		"rc.etag-304-match":                  "PASS",
		"rc.etag-304-identity-header":        "PASS",
		"rc.etag-200-stale":                  "PASS",
		"rc.unknown-workspace-key-400":       "PASS",
		"rc.unknown-environment-key-400":     "PASS",
		"rc.not-published-404":               "PASS",
		"rc.rate-limited-429-positive-retry": "PASS",
		"rc.rate-limited-429-default-retry":  "PASS",
		"rc.scope-unavailable-503":           "PASS",
		"rc.unauthorized-401":                "credential-free fetch unsupported: SDK requires a publishable key",
		"resolver.resolved-strict-200":       "N/A",
		"resolver.advisory-admitted-200":     "N/A",
		"resolver.locale-mixed-case-200":     "N/A",
		"resolver.refusal-invalid-scope-400": "N/A",
		"resolver.refusal-retired-key-400":   "N/A",
		"resolver.refusal-unknown-scope-400": "N/A",
		"nonobjection-recorded":              "N/A",
		"nonobjection-flag-off":              "N/A",
		"nonobjection-service-refused":       "N/A",
		"nonobjection-publishable-no-region": "N/A",
		"nonobjection-mixed-categories":      "N/A",
		"nonobjection-retired-scope-member":  "N/A",
	}
}

func conformanceDispositionCoverage(ids map[string]bool, dispositions map[string]string) bool {
	if len(ids) != len(dispositions) {
		return false
	}
	for id := range ids {
		if dispositions[id] == "" {
			return false
		}
	}
	return true
}

// Check exact spellings and duplicates before encoding/json's permissive map decode.
func conformanceObject(raw []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	var walk func() error
	walk = func() error {
		tok, err := d.Token()
		if err != nil {
			return err
		}
		if delim, ok := tok.(json.Delim); ok {
			seen := map[string]bool{}
			for d.More() {
				if delim == '{' {
					key, err := d.Token()
					if err != nil {
						return err
					}
					name, ok := key.(string)
					if !ok || seen[name] {
						return fmt.Errorf("duplicate or invalid key %v", key)
					}
					seen[name] = true
				}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = d.Token()
		}
		return err
	}
	if err := walk(); err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON: %v", err)
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return nil, fmt.Errorf("expected object: %v", err)
	}
	return out, nil
}

func conformanceHash(raw []byte) string { return fmt.Sprintf("%x", sha256.Sum256(raw)) }

func conformanceCorpus(files map[string][]byte) (conformanceManifest, error) {
	var m conformanceManifest
	fail := func(s string) (conformanceManifest, error) { return m, fmt.Errorf("conformance integrity: %s", s) }
	source, err := conformanceObject(files["source.json"])
	if err != nil || len(source) != 5 {
		return fail("source schema")
	}
	for _, key := range []string{"carrier", "path", "commit", "corpus_sha256", "files_sha256"} {
		if _, ok := source[key]; !ok {
			return fail("source field " + key)
		}
	}
	var carrier, path, commit, corpus string
	for key, target := range map[string]*string{"carrier": &carrier, "path": &path, "commit": &commit, "corpus_sha256": &corpus} {
		if json.Unmarshal(source[key], target) != nil {
			return fail("source type " + key)
		}
	}
	if carrier != "shardpilot/developers" || path != "conformance/v1" || commit != "d5d4164efe19dcf30485a0723135f385b7d1e245" || corpus != conformanceSHA {
		return fail("source pin")
	}
	meta, err := conformanceObject(source["files_sha256"])
	if err != nil || len(meta) != 3 {
		return fail("metadata keys")
	}
	for key, pinned := range map[string]string{
		"manifest.json":  "cc260e6981e92098915977c54d6ce7afb2cb97ebd56ae5b71102f93a941205b3",
		"roster.json":    "73951798f540d2fc9ab2ae05c535ee43c9d3c0441f08c5c5651cc4f32fb28206",
		".gitattributes": "705fd4d6451a31d36b3df7de96f83f30ac976c9b4a6d1e51671d8e2f33e2d0da",
	} {
		var hash string
		raw, exists := files["v1/"+key]
		if !exists || json.Unmarshal(meta[key], &hash) != nil || hash != pinned || hash != conformanceHash(raw) {
			return fail("metadata hash " + key)
		}
	}
	if json.Unmarshal(files["v1/manifest.json"], &m) != nil || m.CorpusSHA != corpus || len(m.Cases) != 45 {
		return fail("manifest pin or case count")
	}
	expected, ids := map[string]bool{}, map[string]bool{}
	for _, c := range m.Cases {
		if c.ID == "" || ids[c.ID] || !c.ByteExact {
			return fail("case identity or byte_exact")
		}
		ids[c.ID] = true
		mode := m.Applicability[c.Surface]["go"]
		if mode.Reason == "" || (mode.Value != "required" && mode.Value != "parse_only" && mode.Value != "unsupported") {
			return fail("applicability " + c.ID)
		}
		for _, side := range []conformanceSide{c.Request, c.Response} {
			if side.File == "" {
				if side.Bytes != 0 || side.SHA256 != "" {
					return fail("absent body")
				}
				continue
			}
			if !regexp.MustCompile(`^bodies/[^/.][^/]*$`).MatchString(side.File) {
				return fail("body path")
			}
			raw, ok := files["v1/"+side.File]
			if !ok || len(raw) != side.Bytes || conformanceHash(raw) != side.SHA256 {
				return fail("body hash " + side.File)
			}
			expected[side.File] = true
		}
	}
	var roster struct{ Cases [][]json.RawMessage }
	if json.Unmarshal(files["v1/roster.json"], &roster) != nil {
		return fail("roster JSON")
	}
	rosterIDs := map[string]bool{}
	for _, row := range roster.Cases {
		var group []string
		if len(row) != 4 || json.Unmarshal(row[3], &group) != nil {
			return fail("roster row")
		}
		for _, id := range group {
			if !ids[id] || rosterIDs[id] {
				return fail("roster identity")
			}
			rosterIDs[id] = true
		}
	}
	if len(rosterIDs) != len(ids) {
		return fail("roster coverage")
	}
	var lines []string
	for name, raw := range files {
		if strings.HasPrefix(name, "v1/bodies/") {
			if !expected[strings.TrimPrefix(name, "v1/")] {
				return fail("extra body " + name)
			}
			lines = append(lines, conformanceHash(raw)+"  "+strings.TrimPrefix(name, "v1/")+"\n")
		} else if name != "source.json" && name != "v1/manifest.json" && name != "v1/roster.json" && name != "v1/.gitattributes" {
			return fail("extra file " + name)
		}
	}
	sort.Strings(lines)
	if len(expected) != 76 || len(files) != 80 || conformanceHash([]byte(strings.Join(lines, ""))) != corpus {
		return fail("complete corpus hash or file set")
	}
	if !conformanceDispositionCoverage(ids, conformanceRequestDispositions()) {
		return fail("case disposition coverage")
	}
	return m, nil
}

func conformanceFiles(t *testing.T) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(conformanceRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink %s", path)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(conformanceRoot, path)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = raw
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestConformanceIntegrity(t *testing.T) {
	files := conformanceFiles(t)
	if _, err := conformanceCorpus(files); err != nil {
		t.Fatal(err)
	}
	source, _ := conformanceObject(files["source.json"])
	files["source.json"], _ = json.Marshal(source)
	if _, err := conformanceCorpus(files); err != nil {
		t.Fatalf("equivalent descriptor formatting refused: %v", err)
	}
	for _, change := range []string{"unknown", "missing", "commit", "commit-substitution", "carrier", "path", "corpus", "metadata-extra", "metadata-missing", "duplicate", "case", "trailing", "metadata-bytes", "body-bytes", "body-missing", "body-extra", "rehash-manifest.json", "rehash-roster.json", "rehash-.gitattributes"} {
		t.Run(change, func(t *testing.T) {
			copy := map[string][]byte{}
			for k, v := range files {
				copy[k] = bytes.Clone(v)
			}
			source, _ := conformanceObject(copy["source.json"])
			switch change {
			case "unknown":
				source["extra"] = json.RawMessage(`true`)
			case "missing":
				delete(source, "commit")
			case "commit", "carrier", "path":
				source[change] = json.RawMessage(`"invalid"`)
			case "commit-substitution":
				source["commit"] = json.RawMessage(`"0000000000000000000000000000000000000000"`)
			case "rehash-manifest.json", "rehash-roster.json", "rehash-.gitattributes":
				key := strings.TrimPrefix(change, "rehash-")
				copy["v1/"+key] = append(copy["v1/"+key], ' ')
				meta, _ := conformanceObject(source["files_sha256"])
				meta[key], _ = json.Marshal(conformanceHash(copy["v1/"+key]))
				source["files_sha256"], _ = json.Marshal(meta)
			case "corpus":
				source["corpus_sha256"] = json.RawMessage(`"bad"`)
			case "metadata-extra":
				source["files_sha256"] = json.RawMessage(`{"extra":"bad"}`)
			case "metadata-missing":
				source["files_sha256"] = json.RawMessage(`{}`)
			case "case":
				source["Commit"] = source["commit"]
				delete(source, "commit")
			case "metadata-bytes":
				copy["v1/roster.json"] = append(copy["v1/roster.json"], ' ')
			case "body-bytes":
				copy["v1/bodies/batch-accepted.request"][0] = '['
			case "body-missing":
				delete(copy, "v1/bodies/batch-accepted.request")
			case "body-extra":
				copy["v1/bodies/extra.request"] = []byte("{}")
			}
			copy["source.json"], _ = json.Marshal(source)
			if change == "duplicate" {
				copy["source.json"] = bytes.Replace(copy["source.json"], []byte(`"commit":`), []byte(`"commit":"bad","commit":`), 1)
			}
			if change == "trailing" {
				copy["source.json"] = append(copy["source.json"], []byte(` {}`)...)
			}
			if _, err := conformanceCorpus(copy); err == nil {
				t.Fatal("altered corpus accepted")
			}
		})
	}
}

func TestConformanceDispositionCoverage(t *testing.T) {
	for _, change := range []string{"missing", "extra", "replaced", "empty"} {
		t.Run(change, func(t *testing.T) {
			dispositions := conformanceRequestDispositions()
			ids := map[string]bool{}
			for id := range dispositions {
				ids[id] = true
			}
			if change == "missing" || change == "replaced" {
				delete(dispositions, "rc.two-key-200")
			}
			if change == "extra" || change == "replaced" {
				dispositions["rc.unreviewed-200"] = "PASS"
			}
			if change == "empty" {
				dispositions["rc.two-key-200"] = ""
			}
			if conformanceDispositionCoverage(ids, dispositions) {
				t.Fatal("incomplete or unreviewed case dispositions accepted")
			}
		})
	}
}
