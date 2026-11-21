package shardpilot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
)

func TestPurposeConsentWire(t *testing.T) {
	witness := PurposeConsentWitness{AuthorizationID: "opaque-authorization-id", DecisionSeq: 9, GraphVersion: 2}
	grant := PurposeConsentReceipt{PurposeConsentWitness: PurposeConsentWitness{AuthorizationID: witness.AuthorizationID, DecisionSeq: 9}, Decision: ConsentDecisionGranted}
	withdrawal := PurposeConsentReceipt{PurposeConsentWitness: witness, Decision: ConsentDecisionDenied, PurgeStatus: "completed"}
	replayed := withdrawal
	replayed.Replayed = true
	pending := withdrawal
	pending.PurgeStatus = "pending"
	for _, test := range []struct {
		name, fixture, response, code string
		status, attempts              int
		want                          PurposeConsentReceipt
	}{
		{"grant", "grant", `{"authorization_id":"opaque-authorization-id","decision_seq":9,"graph_version":0,"decision":"granted","replayed":false}`, "", 200, 1, grant},
		{"withdraw", "withdraw", `{"authorization_id":"opaque-authorization-id","decision_seq":9,"graph_version":2,"decision":"denied","replayed":false,"purge_status":"completed"}`, "", 200, 1, withdrawal},
		{"lost acknowledgement", "withdraw", `{"authorization_id":"opaque-authorization-id","decision_seq":9,"graph_version":2,"decision":"denied","replayed":true,"purge_status":"completed"}`, "", 200, 2, replayed},
		{"conflict", "grant", `{"error":{"code":"purpose_decision_conflict"}}`, "purpose_decision_conflict", 409, 1, PurposeConsentReceipt{}},
		{"pending", "withdraw", `{"error":{"code":"withdrawal_purge_pending"},"receipt":{"authorization_id":"opaque-authorization-id","decision_seq":9,"graph_version":2,"decision":"denied","replayed":false,"purge_status":"pending"}}`, "withdrawal_purge_pending", 503, 3, pending},
	} {
		t.Run(test.name, func(t *testing.T) {
			expected, err := os.ReadFile("testdata/purpose-consent/" + test.fixture + ".json")
			if err != nil {
				t.Fatal(err)
			}
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil || !bytes.Equal(body, bytes.TrimSuffix(expected, []byte("\n"))) {
					t.Errorf("request bytes = %s, read error = %v", body, err)
				}
				if r.Method != http.MethodPost || r.URL.RequestURI() != "/v1/consent" || r.Header.Get("Content-Type") != "application/vnd.shardpilot.purpose-decision+json" ||
					r.Header.Get("Authorization") != "Bearer test-token" || r.Header.Get("Content-Encoding") != "" {
					t.Error("request did not use the trusted consent wire contract")
				}
				if requests.Add(1) == 1 && test.name == "lost acknowledgement" {
					conn, _, err := http.NewResponseController(w).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					conn.Close()
					return
				}
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(test.status)
				io.WriteString(w, test.response)
			}))
			defer server.Close()
			client := newTestClient(t, server.URL)
			defer client.Close(context.Background())
			request := PurposeConsentRequest{Purpose: "profile_features", ActorKind: "anon", ActorIdentifier: "example-actor", IdempotencyKey: test.fixture + "-request-1"}
			var receipt PurposeConsentReceipt
			if test.fixture == "grant" {
				receipt, err = client.GrantPurposeConsent(t.Context(), PurposeConsentGrant{PurposeConsentRequest: request, PolicyVersion: "policy-1", EvidenceRef: "example-evidence"})
			} else {
				receipt, err = client.WithdrawPurposeConsent(t.Context(), request)
			}
			var statusErr *HTTPStatusError
			if test.code == "" && err != nil || test.code != "" && (!errors.As(err, &statusErr) || statusErr.StatusCode != test.status || statusErr.ErrorCode != test.code) {
				t.Fatalf("error = %v, want code %q", err, test.code)
			}
			if receipt != test.want || int(requests.Load()) != test.attempts {
				t.Fatalf("receipt = %+v, requests = %d, want %+v and %d", receipt, requests.Load(), test.want, test.attempts)
			}
		})
	}
}
