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
	if carrier != "shardpilot/developers" || path != "conformance/v1" || !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(commit) || corpus != conformanceSHA {
		return fail("source pin")
	}
	meta, err := conformanceObject(source["files_sha256"])
	if err != nil || len(meta) != 3 {
		return fail("metadata keys")
	}
	for _, key := range []string{"manifest.json", "roster.json", ".gitattributes"} {
		var hash string
		raw, exists := files["v1/"+key]
		if !exists || json.Unmarshal(meta[key], &hash) != nil || hash != conformanceHash(raw) {
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
	for _, change := range []string{"unknown", "missing", "commit", "carrier", "path", "corpus", "metadata-extra", "metadata-missing", "duplicate", "case", "trailing", "metadata-bytes", "body-bytes", "body-missing", "body-extra"} {
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
