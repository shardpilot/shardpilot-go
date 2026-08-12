package shardpilot

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"testing"
)

// ingestRequestBody returns a request's body as the ingest server sees it:
// gzip-decoded when the request declares Content-Encoding: gzip, verbatim
// otherwise.
//
// Every fake ingest server in this suite reads through here. The SDK
// compresses batch bodies over compressionMinimumBytes by default, so a test
// server that decodes r.Body directly starts failing the moment its fixture
// grows past the threshold — which is a property of the FIXTURE, not of the
// behaviour under test, and produces a failure that points nowhere near the
// cause. Decoding here mirrors what the real server does and keeps the
// assertions about the batch rather than about its framing.
func ingestRequestBody(t *testing.T, r *http.Request) io.Reader {
	t.Helper()

	return bytes.NewReader(ingestRequestBytes(t, r))
}

func ingestRequestBytes(t *testing.T, r *http.Request) []byte {
	t.Helper()

	wire, err := io.ReadAll(r.Body)
	if err != nil {
		t.Errorf("read request body: %v", err)
		return nil
	}
	if r.Header.Get("Content-Encoding") != "gzip" {
		return wire
	}

	reader, err := gzip.NewReader(bytes.NewReader(wire))
	if err != nil {
		t.Errorf("request declared gzip but is not a gzip stream: %v", err)
		return nil
	}
	defer reader.Close()

	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Errorf("decode gzip request body: %v", err)
		return nil
	}

	return decoded
}
