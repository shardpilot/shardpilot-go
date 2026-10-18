package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const blockedFixtureFailureEnv = "SHARDPILOT_TEST_FAIL_BLOCKED_FIXTURE"
const blockedFixtureFailure = "forced blocked-request assertion failure"
const blockedFixtureCleanup = "blocked-request fixture cleanup completed"

func blockedFixtureEventName(t *testing.T, r *http.Request) string {
	t.Helper()
	var request batchRequest
	if err := json.NewDecoder(ingestRequestBody(t, r)).Decode(&request); err != nil {
		t.Errorf("decode blocked request: %v", err)
		return ""
	}
	if len(request.Events) != 1 {
		t.Errorf("expected one blocked event, got %d", len(request.Events))
		return ""
	}
	return request.Events[0].EventName
}

func requireBlockedFixtureRequest(t *testing.T, started <-chan string, want string) {
	t.Helper()
	if got := <-started; got != want {
		t.Fatalf("blocked request event = %q, want %q", got, want)
	}
}

func failBlockedFixtureAssertion(t *testing.T) {
	t.Helper()
	if os.Getenv(blockedFixtureFailureEnv) != t.Name() {
		return
	}
	// Test cleanups run after function defers, including client/server Close.
	t.Cleanup(func() { t.Log(blockedFixtureCleanup) })
	t.Fatal(blockedFixtureFailure)
}

func TestBlockedRequestFixtureCleanupAfterFailedAssertion(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []string{
		"TestEnqueueIsNotBlockedBySlowTrackPublish",
		"TestCloseWaitsForInFlightTrack",
		"TestCloseWithCanceledContextReturnsWhileTrackInFlight",
	} {
		t.Run(fixture, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^"+fixture+"$", "-test.timeout=15s", "-test.v")
			// A child race must not look like the expected assertion failure.
			cmd.Env = []string{blockedFixtureFailureEnv + "=" + fixture, "GOMAXPROCS=2", "GORACE=halt_on_error=1"}
			output, err := cmd.CombinedOutput()
			forced := strings.Contains(string(output), blockedFixtureFailure)
			cleaned := strings.Contains(string(output), blockedFixtureCleanup)
			if ctx.Err() != nil {
				t.Fatalf("fixture cleanup exceeded its bound: forced_assertion=%t cleanup_completed=%t\n%s", forced, cleaned, output)
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 || !forced || !cleaned {
				t.Fatalf("expected failed assertion followed by completed cleanup: err=%v forced_assertion=%t cleanup_completed=%t\n%s", err, forced, cleaned, output)
			}
			t.Logf("forced_assertion=%t cleanup_completed=%t child_exit=%d", forced, cleaned, exit.ExitCode())
		})
	}
}
