package shardpilot_test

import (
	"context"
	"fmt"
	"go/version"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestConsentPlanPackageIsInternal(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("locate SDK module: %v", err)
	}
	for _, tc := range []struct{ name, program, refusal string }{
		{"supported-imports", `package main
import (
 "github.com/shardpilot/shardpilot-go"
 "github.com/shardpilot/shardpilot-go/pkg/crash"
)
var _ = shardpilot.Config{}
var _ = crash.ClientOptions{}
func main() {}
`, ""},
		{"retired-public-plan", `package main
import "github.com/shardpilot/shardpilot-go/pkg/consentpolicy"
var _ = consentpolicy.Prepare
func main() {}
`, "cannot find module providing package github.com/shardpilot/shardpilot-go/pkg/consentpolicy"},
		{"internal-plan", `package main
import "github.com/shardpilot/shardpilot-go/internal/consentpolicy"
var _ = consentpolicy.Prepare
func main() {}
`, "use of internal package github.com/shardpilot/shardpilot-go/internal/consentpolicy not allowed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			module := fmt.Sprintf("module example.invalid/consumer\n\ngo 1.25.0\n\nrequire github.com/shardpilot/shardpilot-go v0.0.0\n\nreplace github.com/shardpilot/shardpilot-go => %s\n", strconv.Quote(filepath.ToSlash(root)))
			for name, content := range map[string]string{"go.mod": module, "main.go": tc.program} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			// Trimmed builds may omit runtime's filesystem paths. Let the Go
			// launcher select an official version, or its local toolchain for
			// development builds, with downloads off.
			cmd := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-p=1", ".")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=", "GOTOOLCHAIN="+consumerToolchain(runtime.Version()), "GOMAXPROCS=2", "GOROOT=")
			out, err := cmd.CombinedOutput()
			if tc.refusal == "" {
				if err != nil {
					t.Fatalf("supported imports failed to compile: %v\n%s", err, out)
				}
			} else if err == nil || !strings.Contains(string(out), tc.refusal) {
				t.Fatalf("consumer boundary: want %q, got error %v\n%s", tc.refusal, err, out)
			}
		})
	}
}

func consumerToolchain(v string) string {
	// Keep official versions exact; custom/development builds need not have
	// an installable version name. IsValid permits custom suffixes, so exclude them.
	if version.IsValid(v) && !strings.Contains(v, "-") {
		return v
	}
	return "local"
}

func TestConsumerToolchain(t *testing.T) {
	for _, tc := range []struct{ name, version, want string }{
		{"baseline-release", "go1.25.0", "go1.25.0"},
		{"current-release", "go1.27.2", "go1.27.2"},
		{"release-candidate", "go1.28rc1", "go1.28rc1"},
		{"development", "devel go1.28-abc", "local"},
		{"commit-build", "abcdef 2026-10-09", "local"},
		{"custom-build", "go1.27.2-custom", "local"},
		{"empty", "", "local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := consumerToolchain(tc.version)
			if got != tc.want {
				t.Fatalf("toolchain for %q: got %q, want %q", tc.version, got, tc.want)
			}
			if tc.want == "local" {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, "go", "version")
				cmd.Env = append(os.Environ(), "GOTOOLCHAIN="+got, "GOPROXY=off", "GOSUMDB=off", "GOROOT=")
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("selected toolchain cannot run: %v\n%s", err, out)
				}
			}
		})
	}
}
