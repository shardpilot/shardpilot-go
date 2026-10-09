package crash

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestDefaultCrashSamplerKeepsEveryReport(t *testing.T) {
	for _, calls := range []int{1, 5, 10} {
		for clientIndex := 0; clientIndex < 2; clientIndex++ {
			t.Run(fmt.Sprintf("calls-%d/client-%d", calls, clientIndex), func(t *testing.T) {
				var bodies [][]byte
				callbacks := 0
				client, err := NewClient(ClientOptions{
					IngestURL: "https://crash.synthetic.invalid", APIKey: "synthetic-key",
					OnResult: func(Result) { callbacks++ },
					HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
						if r.Method != http.MethodPost || r.URL.Path != "/api/v1/crashes/ingest" {
							t.Fatalf("unexpected crash request: %s %s", r.Method, r.URL.Path)
						}
						body, err := io.ReadAll(r.Body)
						if err != nil {
							return nil, err
						}
						bodies = append(bodies, body)
						return &http.Response{StatusCode: http.StatusAccepted, Header: http.Header{},
							Body: io.NopCloser(strings.NewReader(`{}`))}, nil
					})},
				})
				if err != nil {
					t.Fatal(err)
				}
				for call := 0; call < calls; call++ {
					if err := client.Emit(context.Background(), validEvent(t)); err != nil {
						t.Fatalf("call %d: %v", call+1, err)
					}
					if len(bodies) != call+1 || callbacks != call+1 {
						t.Fatalf("default silently sampled call %d: posts=%d callbacks=%d", call+1, len(bodies), callbacks)
					}
					assertSamplingWire(t, bodies[call], false, 1)
				}
			})
		}
	}
}
