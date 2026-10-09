package shardpilot

import (
	"context"
	"strings"
	"testing"
)

func TestSchemaRevisionOptInWire(t *testing.T) {
	for _, tc := range []struct{ name, configured, want string }{
		{"default", "", ""},
		{"blank", " \t\n", ""},
		{"explicit", "  sha256:" + strings.Repeat("42", 32) + "  ", "sha256:" + strings.Repeat("42", 32)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := make(chan capturedRevisionHeader, 4)
			server := newSchemaRevisionTestServer(t, headers)
			defer server.Close()
			client := newSchemaRevisionTestClient(t, server.URL, func(cfg *Config) { cfg.SchemaRevision = tc.configured })
			defer client.Close(context.Background())
			if err := client.Track(context.Background(), Event{Name: "synthetic_event"}); err != nil {
				t.Fatal(err)
			}
			got := waitForRevisionHeader(t, headers)
			if got.route != "/v1/events:batch" {
				t.Fatalf("subject route = %q", got.route)
			}
			if got.present != (tc.want != "") || got.value != tc.want {
				t.Fatalf("schema header = %+v, want explicit value %q", got, tc.want)
			}
		})
	}
}
