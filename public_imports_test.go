package shardpilot_test

import (
	"context"
	"fmt"
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
			// launcher locate the same installed toolchain, with downloads off.
			cmd := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-p=1", ".")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=", "GOTOOLCHAIN="+runtime.Version(), "GOMAXPROCS=2", "GOROOT=")
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
