package crash

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func TestCrashPlatformWire(t *testing.T) {
	cases := []struct{ input, module, want, wantModule string }{
		{"darwin", "Mac", "macos", "macos"},
		{"js", "browser", "web", "web"},
		{" WIN64 ", "ipad", "windows", "ios"},
		{"freebsd", "future-os", "other", "other"},
		{"dotted.platform", "dotted.module", "other", "other"},
		{"", "", "other", ""},
		{" \t ", " \t ", "other", ""},
		{"synthetic@example.invalid", "synthetic@example.invalid", "other", "other"},
	}
	for _, canonical := range strings.Fields("windows macos linux android ios tvos web ps4 ps5 xbox switch other") {
		cases = append(cases, struct{ input, module, want, wantModule string }{canonical, canonical, canonical, canonical})
	}
	for i, tc := range cases {
		t.Run(fmt.Sprintf("%02d-%s", i, tc.want), func(t *testing.T) {
			var received Event
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
					t.Errorf("decode actual request: %v", err)
				}
				w.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()
			client, err := NewClient(ClientOptions{IngestURL: server.URL, APIKey: "synthetic-key", Sampler: alwaysSampler{}})
			if err != nil {
				t.Fatal(err)
			}
			event := validEvent(t)
			event.Platform, event.Modules[0].Platform = tc.input, tc.module
			event.OS.Name = "synthetic-os"
			if i%2 == 0 {
				err = client.Emit(context.Background(), event)
			} else {
				err = client.EmitFatal(context.Background(), event)
			}
			if err != nil {
				t.Fatalf("real client failed before sending: %v", err)
			}
			if received.CrashID != event.CrashID || len(received.Modules) != 1 {
				t.Fatalf("subject did not send the complete crash: %+v", received)
			}
			if received.Platform != tc.want || received.Modules[0].Platform != tc.wantModule {
				t.Errorf("wire platforms = %q/%q, want %q/%q", received.Platform, received.Modules[0].Platform, tc.want, tc.wantModule)
			}
			if event.Platform != tc.input || event.Modules[0].Platform != tc.module || received.OS.Name != event.OS.Name {
				t.Error("normalization changed caller data or the separate OS name")
			}
		})
	}
}

type platformWarningLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *platformWarningLog) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func TestCrashPlatformWarningIsOnceAndPrivate(t *testing.T) {
	logger := &platformWarningLog{}
	client, err := NewClient(ClientOptions{IngestURL: "https://crash.example.invalid", APIKey: "synthetic-key", Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	base := validEvent(t)
	var workers sync.WaitGroup
	for i := range 64 {
		workers.Go(func() {
			event := cloneEvent(base)
			if i%2 == 0 {
				event.Platform = fmt.Sprintf("synthetic-host-%d", i)
			} else {
				event.Modules[0].Platform = fmt.Sprintf("synthetic-module-%d", i)
			}
			if _, err := client.prepareEvent(event, false); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	if len(logger.lines) != 1 || !strings.Contains(logger.lines[0], "other") {
		t.Fatalf("want one platform fallback warning, got %v", logger.lines)
	}
	if strings.Contains(logger.lines[0], "synthetic-") {
		t.Fatal("warning leaked raw host input")
	}
}

func TestCrashPlatformDefaultWarning(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	client, err := NewClient(ClientOptions{IngestURL: "https://crash.example.invalid", APIKey: "synthetic-key"})
	if err != nil {
		t.Fatal(err)
	}
	event := validEvent(t)
	event.Platform = "synthetic-unmapped"
	for range 2 {
		if _, err := client.prepareEvent(event, false); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Count(output.String(), "level=WARN") != 1 || strings.Contains(output.String(), event.Platform) {
		t.Fatalf("default warning missing, repeated or leaked input: %s", output.String())
	}
}

func TestCrashPlatformSanitizer(t *testing.T) {
	event := validEvent(t)
	event.Platform, event.Modules[0].Platform = " Mac ", "synthetic.module"
	sanitized, err := SanitizeEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	if sanitized.Platform != "macos" || sanitized.Modules[0].Platform != "other" {
		t.Fatalf("sanitizer platforms = %q/%q", sanitized.Platform, sanitized.Modules[0].Platform)
	}
}

func TestCrashPlatformAutoCapture(t *testing.T) {
	want := map[string]string{"android": "android", "darwin": "macos", "ios": "ios", "js": "web", "linux": "linux", "windows": "windows"}[runtime.GOOS]
	if want == "" {
		want = "other"
	}
	event := (&Client{}).panicEvent("synthetic panic")
	if event.Platform != want || event.OS.Name != runtime.GOOS {
		t.Fatalf("capture platform/OS = %q/%q, want %q/%q", event.Platform, event.OS.Name, want, runtime.GOOS)
	}
}
